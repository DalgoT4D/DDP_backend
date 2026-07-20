import os
import subprocess

from django.conf import settings

from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.warehouse_data")


def _pg_bin(name: str) -> str:
    """resolve the pg_dump/pg_restore binary — configurable so ops can point at a client
    version matching the trials-RDS server (a newer client emits SET params an older server
    rejects on restore). Defaults to the bare name (resolved from PATH)."""
    override = {
        "pg_dump": getattr(settings, "TRIALS_PG_DUMP_BIN", None),
        "pg_restore": getattr(settings, "TRIALS_PG_RESTORE_BIN", None),
    }.get(name)
    return override or name


def _pg_env(password: str) -> dict:
    env = os.environ.copy()
    env["PGPASSWORD"] = password
    # some pg client installs need their own libpq on the dynamic-loader path
    lib_dir = getattr(settings, "TRIALS_PG_LIB_DIR", None)
    if lib_dir:
        env["DYLD_LIBRARY_PATH"] = lib_dir
        env["LD_LIBRARY_PATH"] = lib_dir
    return env


def copy_warehouse_data(src: dict, dst: dict, dump_path: str) -> None:
    """pg_dump the source warehouse (custom format) then pg_restore into the destination.

    Schema + table names are preserved so all downstream string references still resolve.
    src/dst dicts require keys: host, port, database, username, password.
    """
    dump_cmd = [
        _pg_bin("pg_dump"),
        "-h",
        str(src["host"]),
        "-p",
        str(src["port"]),
        "-U",
        str(src["username"]),
        "-d",
        str(src["database"]),
        "-Fc",
        "--no-owner",
        "--no-acl",
        "-f",
        dump_path,
    ]
    result = subprocess.run(dump_cmd, env=_pg_env(src["password"]), capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"pg_dump failed: {result.stderr}")
    logger.info(f"pg_dump of {src['database']} → {dump_path} ok")

    restore_cmd = [
        _pg_bin("pg_restore"),
        "-h",
        str(dst["host"]),
        "-p",
        str(dst["port"]),
        "-U",
        str(dst["username"]),
        "-d",
        str(dst["database"]),
        "--no-owner",
        "--no-acl",
        dump_path,
    ]
    result = subprocess.run(
        restore_cmd, env=_pg_env(dst["password"]), capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"pg_restore failed: {result.stderr}")
    logger.info(f"pg_restore into {dst['database']} ok")
