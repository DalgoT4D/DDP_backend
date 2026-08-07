import hashlib
import re
import secrets
import time

import psycopg2

from django.conf import settings

from ddpui.core.trial.exceptions import TrialCloneError
from ddpui.schemas.trial_schema import TrialDbParams
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.warehouse_provision")

# Postgres refuses `CREATE DATABASE ... TEMPLATE src` while any OTHER session is connected to
# `src`. The template warehouse isn't truly frozen — a just-finished clone, a pooled/Airbyte
# backend, or someone viewing the template org's charts can hold an idle session on it. So we
# terminate those sessions right before the copy; a fresh session can still race in between the
# terminate and the CREATE, so we retry the terminate+create a few times before giving up.
_TEMPLATE_COPY_MAX_ATTEMPTS = 5
_TEMPLATE_COPY_RETRY_SLEEP_SECONDS = 1.0

# NOTE: verified empirically (2026-07-29, Postgres 15.16) that Postgres does NOT serialize two
# concurrent `CREATE DATABASE ... TEMPLATE src` calls issued by different sessions against the
# SAME src, as long as neither session's OWN current database is `src` — 4 concurrent copies
# from the same source ran fully in parallel (identical start/finish times), no ObjectInUse, no
# contention. So the only real race here is the one already handled below: a stray session that
# IS connected to `src` itself (idle pooled/Airbyte/chart-query connection). No app-level lock
# needed between concurrent clones — that would only add unnecessary serialization and hurt
# the goal of clones running fully in parallel.


def _create_database_from_template(cursor, ft_db: str, template_db: str) -> None:
    """CREATE DATABASE ft_db as a server-side copy of template_db, clearing blocking sessions.

    Kills every other session on template_db first (idle pooled/Airbyte/chart-query connections
    that would otherwise raise ObjectInUse), then runs the copy. If a new session slips in during
    that window the copy still raises ObjectInUse — we retry the whole terminate+create a bounded
    number of times. The cursor's connection is autocommit (see _admin_connect), so each statement
    commits immediately. Only idle/foreign sessions on the TEMPLATE are terminated; no trial data
    is at risk.
    """
    last_err: Exception | None = None
    for attempt in range(1, _TEMPLATE_COPY_MAX_ATTEMPTS + 1):
        cursor.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            [template_db],
        )
        terminated = cursor.rowcount
        if terminated:
            logger.info(
                f"terminated {terminated} session(s) on template db {template_db} "
                f"before copy (attempt {attempt})"
            )
        try:
            cursor.execute(f'CREATE DATABASE "{ft_db}" TEMPLATE "{template_db}"')
            return
        except psycopg2.errors.ObjectInUse as err:  # a session raced back in — retry
            last_err = err
            logger.warning(
                f"template db {template_db} still in use on attempt {attempt}/"
                f"{_TEMPLATE_COPY_MAX_ATTEMPTS}; retrying"
            )
            time.sleep(_TEMPLATE_COPY_RETRY_SLEEP_SECONDS)
    raise TrialCloneError(
        f"could not copy template db {template_db} after {_TEMPLATE_COPY_MAX_ATTEMPTS} attempts: "
        f"{last_err}"
    ) from last_err


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
    membership again. `public` (owned by `postgres`) is handled separately by the caller and
    skipped here.

    OPS REQUIREMENT: the template DATABASE itself must be owned by the trials ADMIN user, and
    the admin must not depend on a standing membership of the template warehouse role. Two
    Postgres gotchas otherwise bite (both happened on staging, 2026-07-23):
    - REASSIGN OWNED also transfers cluster-wide SHARED objects — if the template warehouse
      role owns the template db, this REASSIGN silently hands the TEMPLATE db to the trial's
      ft role (hijacking it and making the ft role's DROP ROLE fail at teardown);
    - the REVOKE below destroys any standing `GRANT <template-owner> TO <admin>` membership,
      so a copy that relied on that membership fails with InsufficientPrivilege on the NEXT
      clone.

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
        # NEVER reassign away from the connecting admin itself: REASSIGN OWNED transfers
        # cluster-wide shared objects too, and the admin owns the TEMPLATE database — a
        # `REASSIGN OWNED BY <admin>` would hand the template db to the trial role
        # (hijacking it and breaking the trial role's teardown). Admin-owned objects inside
        # the trial db copy stay admin-owned; the ft role can read them via the schema/table
        # grants issued by the caller.
        if owner == settings.TRIALS_RDS_ADMIN_USER:
            continue
        cursor.execute(f'GRANT "{owner}" TO CURRENT_USER')
        cursor.execute(f'REASSIGN OWNED BY "{owner}" TO "{ft_role}"')
        cursor.execute(f'REVOKE "{owner}" FROM CURRENT_USER')
        logger.info(f"reassigned template objects owned by {owner} to {ft_role}")


def provision_trial_database(email: str, template_db: str) -> TrialDbParams:
    """Create a dedicated Postgres database + owner role on the trials-RDS instance for a
    trial, keyed by the trial email (not the trialclone id) so repeated trials from the same
    email land on the same, deterministically-named db/role.

    The new db is created as a server-side, file-level copy of `template_db` (the template
    warehouse's db name on this SAME trials-RDS instance) via
    `CREATE DATABASE ... TEMPLATE ...` — a few seconds, no network transfer. Any sessions
    still connected to template_db are terminated first (see _create_database_from_template).

    Returns connection params for the new database using the FT-USER's own credentials
    (never the admin/master credentials).
    """
    if not template_db:
        raise ValueError("template_db is required — the trial db is always a template copy")
    ft_db = ft_database_name(email)
    ft_role = ft_role_name(email)
    password = secrets.token_urlsafe(24)

    # A partial provision must not leak: CREATE DATABASE has no IF NOT EXISTS, so a db+role left
    # behind by a failure between CREATE DATABASE and the return would both leak AND block every
    # retry for this email (the next CREATE DATABASE errors "already exists"). drop_trial_database
    # is idempotent, so on any failure below we roll back whatever got created before re-raising.
    try:
        conn = _admin_connect("postgres")
        try:
            with conn.cursor() as cursor:
                # template_db is an ops/secrets-manager-controlled identifier (the template
                # warehouse's own db name, retrieved via retrieve_warehouse_credentials),
                # never raw user input — f-string interpolation does not cross a trust boundary.
                # Copy clears any blocking sessions on the template first (see helper).
                _create_database_from_template(cursor, ft_db, template_db)
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
                _reassign_copied_objects(cursor, ft_role)
        finally:
            ft_db_conn.close()
    except Exception as err:  # skipcq PYL-W0703
        logger.error(f"provision failed for {email}; rolling back partial db/role: {err}")
        try:
            drop_trial_database(email)
        except Exception as cleanup_err:  # skipcq PYL-W0703
            logger.error(f"rollback after failed provision also failed for {email}: {cleanup_err}")
        raise

    logger.info(
        f"provisioned trial database {ft_db} (role {ft_role}) on {settings.TRIALS_RDS_HOST}"
    )
    return TrialDbParams(
        host=settings.TRIALS_RDS_HOST,
        port=settings.TRIALS_RDS_PORT,
        database=ft_db,
        username=ft_role,
        password=password,
    )


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
