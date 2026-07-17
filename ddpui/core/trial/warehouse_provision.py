import psycopg2

from django.conf import settings

from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.warehouse_provision")


def provision_trial_database(trialclone_id: int) -> dict:
    """Create a fresh Postgres database on the trials-RDS instance for a trial clone.

    Returns Postgres connection params for the new database. CREATE DATABASE cannot run
    inside a transaction, so we connect with autocommit against the admin 'postgres' db.
    """
    db_name = f"trial_{trialclone_id}"
    admin_params = {
        "host": settings.TRIALS_RDS_HOST,
        "port": settings.TRIALS_RDS_PORT,
        "user": settings.TRIALS_RDS_ADMIN_USER,
        "password": settings.TRIALS_RDS_ADMIN_PASSWORD,
        "dbname": "postgres",
    }
    conn = psycopg2.connect(**admin_params)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(f'CREATE DATABASE "{db_name}"')
    finally:
        conn.close()

    logger.info(f"provisioned trial database {db_name} on {settings.TRIALS_RDS_HOST}")
    return {
        "host": settings.TRIALS_RDS_HOST,
        "port": settings.TRIALS_RDS_PORT,
        "database": db_name,
        "username": settings.TRIALS_RDS_ADMIN_USER,
        "password": settings.TRIALS_RDS_ADMIN_PASSWORD,
    }


def drop_trial_database(trialclone_id: int) -> None:
    """Drop the trials-RDS database for a trial clone (best-effort teardown on failure).

    Mirrors provision_trial_database: DROP DATABASE cannot run inside a transaction, so we
    connect with autocommit against the admin 'postgres' db.
    """
    db_name = f"trial_{trialclone_id}"
    admin_params = {
        "host": settings.TRIALS_RDS_HOST,
        "port": settings.TRIALS_RDS_PORT,
        "user": settings.TRIALS_RDS_ADMIN_USER,
        "password": settings.TRIALS_RDS_ADMIN_PASSWORD,
        "dbname": "postgres",
    }
    conn = psycopg2.connect(**admin_params)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
    finally:
        conn.close()

    logger.info(f"dropped trial database {db_name} on {settings.TRIALS_RDS_HOST}")
