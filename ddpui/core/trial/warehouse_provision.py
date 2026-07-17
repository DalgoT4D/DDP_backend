import hashlib
import re
import secrets

import psycopg2

from django.conf import settings

from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.warehouse_provision")


def _ft_key(email: str) -> str:
    """Deterministic, Postgres-identifier-safe key derived from a trial email.

    Sanitizing the local part alone can collide (e.g. 'a.b' and 'a_b' both sanitize to
    'a_b'), so we append an 8-hex-char slice of the email's sha256 digest to keep distinct
    emails distinct even when their sanitized forms collide.
    """
    local = re.sub(r"[^a-z0-9]+", "_", email.lower()).strip("_")
    digest = hashlib.sha256(email.lower().encode()).hexdigest()[:8]
    return f"{local[:40]}_{digest}"


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


def provision_trial_database(email: str) -> dict:
    """Create a dedicated Postgres database + owner role on the trials-RDS instance for a
    trial, keyed by the trial email (not the trialclone id) so repeated trials from the same
    email land on the same, deterministically-named db/role.

    Returns connection params for the new database using the FT-USER's own credentials
    (never the admin/master credentials).
    """
    ft_db = ft_database_name(email)
    ft_role = ft_role_name(email)
    password = secrets.token_urlsafe(24)

    conn = _admin_connect("postgres")
    try:
        with conn.cursor() as cursor:
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
    try:
        with conn.cursor() as cursor:
            cursor.execute(f'DROP DATABASE IF EXISTS "{ft_db}"')
            cursor.execute(f'DROP ROLE IF EXISTS "{ft_role}"')
    finally:
        conn.close()

    logger.info(f"dropped trial database {ft_db} (role {ft_role}) on {settings.TRIALS_RDS_HOST}")
