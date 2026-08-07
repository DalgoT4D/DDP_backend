import os
from typing import Union

from ddpui.ddpdbt.schema import DbtProjectParams

# Fixed target label used inside profiles.yml. Matches proxy/prefect_flows_runner.py:DBT_TARGET
# so backend-generated profiles.yml and runner-generated ones use the same key.
DBT_TARGET = "default"


def map_airbyte_destination_spec_to_dbtcli_profile(conn_info: dict):
    """
    Dbt doesn't support tunnel methods
    So the translation to tunnel params is for our proxy service
    To do a hack & run dbt using ssh tunnel
    """
    if "tunnel_method" in conn_info:
        method = conn_info["tunnel_method"]

        if method["tunnel_method"] in ["SSH_KEY_AUTH", "SSH_PASSWORD_AUTH"]:
            conn_info["ssh_host"] = method["tunnel_host"]
            conn_info["ssh_port"] = method["tunnel_port"]
            conn_info["ssh_username"] = method["tunnel_user"]

        if method["tunnel_method"] == "SSH_KEY_AUTH":
            conn_info["ssh_pkey"] = method["ssh_key"]
            conn_info["ssh_private_key_password"] = method.get("tunnel_private_key_password")

        elif method["tunnel_method"] == "SSH_PASSWORD_AUTH":
            conn_info["ssh_password"] = method.get("tunnel_user_password")

    if "username" in conn_info:
        conn_info["user"] = conn_info["username"]

    # handle dbt ssl params
    if "ssl_mode" in conn_info:
        ssl_data = conn_info["ssl_mode"]
        mode = ssl_data["mode"] if "mode" in ssl_data else None
        ca_certificate = ssl_data["ca_certificate"] if "ca_certificate" in ssl_data else None
        if mode:
            conn_info["sslmode"] = mode

        if ca_certificate:
            conn_info["sslrootcert_content"] = ca_certificate
            # we are build this path in proxy
            conn_info["sslrootcert"] = None

    return conn_info


def preprocess_airbyte_creds_for_dbt(warehouse, airbyte_creds: dict) -> tuple[dict, dict]:
    """Convert raw airbyte destination-spec creds into the (dbt_creds, wh_extras)
    pair used everywhere downstream (profiles.yml, Secret block value).

    Mutates airbyte_creds: pops BigQuery-only keys (dataset_location,
    transformation_priority).

    Returns:
        dbt_creds: mapped to dbt-postgres / dbt-bigquery field names, with
            ssl_mode/ssl stripped.
        wh_extras: {"location": ..., "priority": ...} for BigQuery, {} for postgres.
    """
    bqlocation = None
    priority = None
    if warehouse.wtype == "bigquery":
        bqlocation = airbyte_creds.pop("dataset_location", None)
        priority = airbyte_creds.pop("transformation_priority", None)

    dbt_creds = map_airbyte_destination_spec_to_dbtcli_profile(airbyte_creds)
    dbt_creds.pop("ssl_mode", None)
    dbt_creds.pop("ssl", None)

    wh_extras = {}
    if warehouse.wtype == "bigquery":
        if bqlocation:
            wh_extras["location"] = bqlocation
        if priority:
            wh_extras["priority"] = priority

    return dbt_creds, wh_extras


def _build_output(wtype: str, schema: str, creds: dict, extras: dict, threads: int = 4) -> dict:
    """Inner `outputs.<target>` dict for postgres or bigquery. Mirrors
    proxy/prefect_flows_runner.py:_build_output so backend-built profiles.yml
    matches what the runner writes."""
    if wtype == "postgres":
        pg_creds = dict(creds)
        if "username" in pg_creds and "user" not in pg_creds:
            pg_creds["user"] = pg_creds.pop("username")
        pg_creds.pop("schema", None)
        # sslrootcert_content is not a dbt field; the caller writes it to disk
        # and rewrites `sslrootcert` to the path.
        pg_creds.pop("sslrootcert_content", None)
        output = {"type": "postgres", "schema": schema, "threads": threads}
        output.update(pg_creds)
        output.update(extras)
        return output

    if wtype == "bigquery":
        # `project` (dbt alias for `database`) is required at the top level;
        # if omitted, dbt-bigquery falls back to google.auth.default() (ADC).
        output = {
            "type": "bigquery",
            "schema": schema,
            "threads": threads,
            "method": "service-account-json",
            "project": creds["project_id"],
            "keyfile_json": creds,
        }
        if "location" in extras:
            output["location"] = extras["location"]
        if "priority" in extras:
            output["priority"] = extras["priority"]
        return output

    raise ValueError(f"Unsupported warehouse type: {wtype}")


def build_profile_dict(
    *,
    profile_name: str,
    wtype: str,
    schema: str,
    creds: dict,
    extras: dict,
    target: str = DBT_TARGET,
    threads: int = 4,
) -> dict:
    """Build a complete profiles.yml dict — single profile, single output.

    Backend counterpart to proxy/prefect_flows_runner.py:build_profile_dict.
    Kept in sync so runner-generated and backend-generated profiles.yml files
    are structurally identical.
    """
    return {
        profile_name: {
            "target": target,
            "outputs": {target: _build_output(wtype, schema, creds, extras, threads)},
        }
    }
