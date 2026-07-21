import hashlib
import re
import secrets

import psycopg2

from django.conf import settings

from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.warehouse_provision")


def email_hash8(email: str) -> str:
    """Deterministic 8-hex-char slice of the email's sha256 digest.

    Shared disambiguator so trial resource names (db, role, org) all derive from the same
    per-email hash.
    """
    return hashlib.sha256(email.lower().encode()).hexdigest()[:8]


def _ft_key(email: str) -> str:
    """Deterministic, Postgres-identifier-safe key derived from a trial email.

    Sanitizing the local part alone can collide (e.g. 'a.b' and 'a_b' both sanitize to
    'a_b'), so we append an 8-hex-char slice of the email's sha256 digest to keep distinct
    emails distinct even when their sanitized forms collide.
    """
    local = re.sub(r"[^a-z0-9]+", "_", email.lower()).strip("_")
    return f"{local[:40]}_{email_hash8(email)}"


def ft_database_name(email: str) -> str:
    """Deterministic per-email trial database name (<=63 chars, Postgres-identifier-safe)."""
    return f"ft_{_ft_key(email)}_db"


def ft_role_name(email: str) -> str:
    """Deterministic per-email trial owner-role name (<=63 chars, Postgres-identifier-safe)."""
    return f"ft_{_ft_key(email)}_user"


def _admin_connect(dbname: str):
    """Connect to the trials-RDS instance as master, autocommit (DDL can't run in a txn)."""
    conn = psycopg2.connect(
        host=settings.TRIALS_RDS_HOST,
        port=settings.TRIALS_RDS_PORT,
        user=settings.TRIALS_RDS_ADMIN_USER,
        password=settings.TRIALS_RDS_ADMIN_PASSWORD,
        dbname=dbname,
    )
    conn.autocommit = True
    return conn


def _reassign_copied_objects(cursor, ft_role: str) -> None:
    """Hand ownership of every object copied from the template db over to the trial's ft role.

    `CREATE DATABASE ... TEMPLATE ...` clones the template's schemas/tables verbatim, keeping the
    TEMPLATE's owner role (e.g. the template warehouse role) on each object. The trial's ft role
    then has no rights on those schemas (`intermediate`, `staging`, …), so the app's chart queries
    fail with "permission denied for schema intermediate" and dbt can't rebuild the models.

    For each distinct non-system owner found in the copied db we make the admin a temporary member
    of that role (rds_superuser is allowed to grant any role to itself, but REASSIGN still requires
    actual membership in the source role), REASSIGN OWNED its objects to the ft role, then drop the
    membership again. REASSIGN OWNED is db-scoped, so this only touches the trial db. `public`
    (owned by `postgres`) is handled separately by the caller and skipped here.

    Owner names come from the copied template db's own catalog — controlled template content, never
    user input — so interpolating them as identifiers does not cross a trust boundary.
    """
    cursor.execute(
        """
        SELECT DISTINCT owner FROM (
            SELECT pg_get_userbyid(nspowner) AS owner FROM pg_namespace
            WHERE nspname NOT LIKE 'pg\\_%' AND nspname <> 'information_schema'
            UNION
            SELECT tableowner FROM pg_tables
            WHERE schemaname NOT LIKE 'pg\\_%' AND schemaname <> 'information_schema'
        ) owners
        """
    )
    owners = [row[0] for row in cursor.fetchall()]
    for owner in owners:
        if owner == ft_role or owner == "postgres" or owner.startswith("pg_"):
            continue
        cursor.execute(f'GRANT "{owner}" TO CURRENT_USER')
        cursor.execute(f'REASSIGN OWNED BY "{owner}" TO "{ft_role}"')
        cursor.execute(f'REVOKE "{owner}" FROM CURRENT_USER')
        logger.info(f"reassigned template objects owned by {owner} to {ft_role}")


def provision_trial_database(email: str, template_db: str | None = None) -> dict:
    """Create a dedicated Postgres database + owner role on the trials-RDS instance for a
    trial, keyed by the trial email (not the trialclone id) so repeated trials from the same
    email land on the same, deterministically-named db/role.

    When `template_db` is given (the template warehouse's db name on this SAME trials-RDS
    instance), the new db is created as a server-side, file-level copy of it via
    `CREATE DATABASE ... TEMPLATE ...` — a few seconds, no network transfer — instead of an
    empty database. Requires no active sessions on template_db (it must be a frozen template).

    Returns connection params for the new database using the FT-USER's own credentials
    (never the admin/master credentials).
    """
    ft_db = ft_database_name(email)
    ft_role = ft_role_name(email)
    password = secrets.token_urlsafe(24)

    conn = _admin_connect("postgres")
    try:
        with conn.cursor() as cursor:
            if template_db:
                # template_db is an ops/secrets-manager-controlled identifier (the template
                # warehouse's own db name, retrieved via retrieve_warehouse_credentials), never
                # raw user input — f-string interpolation here does not cross a trust boundary.
                cursor.execute(f'CREATE DATABASE "{ft_db}" TEMPLATE "{template_db}"')
            else:
                cursor.execute(f'CREATE DATABASE "{ft_db}"')
            cursor.execute(f"CREATE ROLE \"{ft_role}\" LOGIN PASSWORD '{password}'")
            cursor.execute(f'GRANT "{ft_role}" TO CURRENT_USER')
            cursor.execute(f'ALTER DATABASE "{ft_db}" OWNER TO "{ft_role}"')
    finally:
        conn.close()

    # reconnect to the freshly-created db to hand the `public` schema over to the ft role too —
    # PG15 makes `public` non-writable by non-owners by default, so this must be explicit and
    # cannot be done from the 'postgres' maintenance connection.
    ft_db_conn = _admin_connect(ft_db)
    try:
        with ft_db_conn.cursor() as cursor:
            cursor.execute(f'GRANT ALL ON SCHEMA public TO "{ft_role}"')
            cursor.execute(f'ALTER SCHEMA public OWNER TO "{ft_role}"')
            if template_db:
                _reassign_copied_objects(cursor, ft_role)
    finally:
        ft_db_conn.close()

    logger.info(
        f"provisioned trial database {ft_db} (role {ft_role}) on {settings.TRIALS_RDS_HOST}"
    )
    return {
        "host": settings.TRIALS_RDS_HOST,
        "port": settings.TRIALS_RDS_PORT,
        "database": ft_db,
        "username": ft_role,
        "password": password,
    }


def drop_trial_database(email: str) -> None:
    """Best-effort teardown of both the db and its dedicated owner role for a trial email.

    Mirrors provision_trial_database: DROP DATABASE/ROLE cannot run inside a transaction, so
    we connect with autocommit against the admin 'postgres' db. The database must be dropped
    before the role — a role that still owns a database cannot be dropped.
    """
    ft_db = ft_database_name(email)
    ft_role = ft_role_name(email)

    conn = _admin_connect("postgres")
    terminated = 0
    try:
        with conn.cursor() as cursor:
            # A bare DROP DATABASE fails with "database is being accessed by other users" if any
            # session is still connected (a recent clone, a pooled backend/Airbyte connection).
            # Block new connections and terminate the live ones first so teardown is idempotent
            # and never leaves the db+role stranded. Only touch the db while it still exists —
            # REVOKE has no IF-EXISTS form and would raise if the db is already gone, breaking
            # the "safe to run even if already gone" contract.
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", [ft_db])
            if cursor.fetchone():
                cursor.execute(f'REVOKE CONNECT ON DATABASE "{ft_db}" FROM PUBLIC')
                cursor.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = %s AND pid <> pg_backend_pid()",
                    [ft_db],
                )
                terminated = cursor.rowcount
            cursor.execute(f'DROP DATABASE IF EXISTS "{ft_db}"')
            cursor.execute(f'DROP ROLE IF EXISTS "{ft_role}"')
    finally:
        conn.close()

    logger.info(
        f"dropped trial database {ft_db} (role {ft_role}) on {settings.TRIALS_RDS_HOST} "
        f"(terminated {terminated} active session(s))"
    )
