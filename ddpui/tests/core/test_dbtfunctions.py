import os
from types import SimpleNamespace

import pytest

from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.core.dbtfunctions import (
    DBT_TARGET,
    _build_output,
    build_profile_dict,
    map_airbyte_destination_spec_to_dbtcli_profile,
    preprocess_airbyte_creds_for_dbt,
)


def test_map_airbyte_destination_spec_to_dbtcli_profile_success_tunnel_params(tmpdir):
    """Tests all the success cases"""
    dbt_project_params = DbtProjectParams(
        org_project_dir=str(tmpdir),
        dbt_env_dir="/path/to/dbt_venv",
        dbt_repo_dir="/path/to/dbt_repo",
        project_dir="/path/to/project_dir",
        target="target",
        dbt_binary="dbt_binary",
        venv_binary="path/to/venv/bin",
        clients_base_dir="/path/to/clients_base",
        project_dir_relative="org/dbtrepo",
    )

    conn_info = {"some": "random value"}
    res = map_airbyte_destination_spec_to_dbtcli_profile(conn_info, dbt_project_params)
    assert res == conn_info

    # SSH_KEY_AUTH
    conn_info = {
        "tunnel_method": {
            "tunnel_method": "SSH_KEY_AUTH",
            "tunnel_host": "tunnel_host",
            "tunnel_port": 22,
            "tunnel_user": "tunnel_user",
            "ssh_key": "ssh_key",
            "tunnel_private_key_password": "tunnel_private_key_password",
        }
    }
    res = map_airbyte_destination_spec_to_dbtcli_profile(conn_info, dbt_project_params)
    assert res["ssh_host"] == conn_info["tunnel_method"]["tunnel_host"]
    assert res["ssh_port"] == conn_info["tunnel_method"]["tunnel_port"]
    assert res["ssh_username"] == conn_info["tunnel_method"]["tunnel_user"]
    assert res["ssh_pkey"] == conn_info["tunnel_method"]["ssh_key"]
    assert (
        res["ssh_private_key_password"] == conn_info["tunnel_method"]["tunnel_private_key_password"]
    )

    # SSH_PASSWORD_AUTH
    conn_info = {
        "tunnel_method": {
            "tunnel_method": "SSH_PASSWORD_AUTH",
            "tunnel_host": "tunnel_host",
            "tunnel_port": 22,
            "tunnel_user": "tunnel_user",
            "tunnel_user_password": "tunnel_user_password",
        }
    }
    res = map_airbyte_destination_spec_to_dbtcli_profile(conn_info, dbt_project_params)
    assert res["ssh_host"] == conn_info["tunnel_method"]["tunnel_host"]
    assert res["ssh_port"] == conn_info["tunnel_method"]["tunnel_port"]
    assert res["ssh_username"] == conn_info["tunnel_method"]["tunnel_user"]
    assert res["ssh_password"] == conn_info["tunnel_method"]["tunnel_user_password"]

    # make sure the username is mapped to user
    conn_info = {"username": "username"}
    res = map_airbyte_destination_spec_to_dbtcli_profile(conn_info, dbt_project_params)
    assert res["user"] == conn_info["username"]


def test_map_airbyte_destination_spec_to_dbtcli_profile_success_ssl_params(tmpdir):
    """Tests ssl params are stored in conn_info for runtime cert writing"""
    dbt_project_params = DbtProjectParams(
        org_project_dir=str(tmpdir),
        dbt_env_dir="/path/to/dbt_venv",
        dbt_repo_dir="/path/to/dbt_repo",
        project_dir="/path/to/project_dir",
        target="target",
        dbt_binary="dbt_binary",
        venv_binary="path/to/venv/bin",
        clients_base_dir="/path/to/clients_base",
        project_dir_relative="org/dbtrepo",
    )

    conn_info = {"ssl_mode": {"mode": "verify-ca", "ca_certificate": "ca_certificate"}}
    res = map_airbyte_destination_spec_to_dbtcli_profile(conn_info, dbt_project_params)
    assert res["sslmode"] == "verify-ca"
    assert res["sslrootcert"] == f"{tmpdir}/sslrootcert.pem"
    assert res["sslrootcert_content"] == "ca_certificate"
    # cert should NOT be written to disk at setup time
    assert not os.path.exists(f"{tmpdir}/sslrootcert.pem")


def test_map_airbyte_destination_spec_to_dbtcli_profile_ssl_mode_only(tmpdir):
    """Tests ssl_mode with mode but no ca_certificate"""
    dbt_project_params = DbtProjectParams(
        org_project_dir=str(tmpdir),
        dbt_env_dir="/path/to/dbt_venv",
        dbt_repo_dir="/path/to/dbt_repo",
        project_dir="/path/to/project_dir",
        target="target",
        dbt_binary="dbt_binary",
        venv_binary="path/to/venv/bin",
        clients_base_dir="/path/to/clients_base",
        project_dir_relative="org/dbtrepo",
    )

    conn_info = {"ssl_mode": {"mode": "require"}}
    res = map_airbyte_destination_spec_to_dbtcli_profile(conn_info, dbt_project_params)
    assert res["sslmode"] == "require"
    assert "sslrootcert" not in res
    assert "sslrootcert_content" not in res


def test_map_airbyte_destination_spec_to_dbtcli_profile_ssl_no_org_project_dir():
    """Tests ssl with ca_certificate but no dbt_project_params raises"""
    conn_info = {"ssl_mode": {"mode": "verify-ca", "ca_certificate": "ca_certificate"}}
    try:
        map_airbyte_destination_spec_to_dbtcli_profile(conn_info, None)
        assert False, "should have raised"
    except Exception as e:
        assert "org_project_dir is required" in str(e)


# ============================================================================
# preprocess_airbyte_creds_for_dbt
# ============================================================================


def test_preprocess_airbyte_creds_for_dbt_postgres_no_extras():
    """postgres: extras is {}, ssl_mode/ssl stripped."""
    warehouse = SimpleNamespace(wtype="postgres")
    airbyte_creds = {
        "username": "u",
        "password": "pw",
        "host": "h",
        "port": 5432,
        "ssl_mode": {"mode": "require"},  # no ca_certificate
        "ssl": True,
    }
    dbt_creds, wh_extras = preprocess_airbyte_creds_for_dbt(warehouse, airbyte_creds, None)
    assert wh_extras == {}
    assert "ssl_mode" not in dbt_creds
    assert "ssl" not in dbt_creds
    # username→user mapping happens in map_airbyte_destination_spec_to_dbtcli_profile
    assert dbt_creds["user"] == "u"


def test_preprocess_airbyte_creds_for_dbt_bigquery_extracts_location_and_priority():
    """bigquery: dataset_location and transformation_priority are moved out of
    creds into wh_extras (they're profile-shaping, not credentials)."""
    warehouse = SimpleNamespace(wtype="bigquery")
    airbyte_creds = {
        "type": "service_account",
        "project_id": "p",
        "dataset_location": "us-central1",
        "transformation_priority": "batch",
    }
    dbt_creds, wh_extras = preprocess_airbyte_creds_for_dbt(warehouse, airbyte_creds, None)
    assert wh_extras == {"location": "us-central1", "priority": "batch"}
    assert "dataset_location" not in dbt_creds
    assert "transformation_priority" not in dbt_creds


def test_preprocess_airbyte_creds_for_dbt_bigquery_empty_extras_when_absent():
    """bigquery: extras is {} when dataset_location and priority absent."""
    warehouse = SimpleNamespace(wtype="bigquery")
    airbyte_creds = {"type": "service_account", "project_id": "p"}
    _, wh_extras = preprocess_airbyte_creds_for_dbt(warehouse, airbyte_creds, None)
    assert wh_extras == {}


# ============================================================================
# _build_output — postgres
# ============================================================================


def test_build_output_postgres_maps_username_to_user_and_pops_schema():
    """dbt-postgres field is `user` (not `username`); airbyte `schema` in creds
    must be popped so the caller-supplied dbt schema wins."""
    creds = {"username": "u", "password": "pw", "schema": "airbyte_schema"}
    output = _build_output("postgres", "dbt_schema", creds, {}, threads=4)
    assert output["user"] == "u"
    assert "username" not in output
    assert output["schema"] == "dbt_schema"


def test_build_output_postgres_strips_sslrootcert_content():
    """sslrootcert_content is our internal PEM transport; not a dbt-postgres field."""
    creds = {
        "username": "u",
        "password": "pw",
        "sslrootcert": "/tmp/cert.pem",
        "sslrootcert_content": "PEM",
    }
    output = _build_output("postgres", "s", creds, {}, threads=4)
    assert "sslrootcert_content" not in output
    assert output["sslrootcert"] == "/tmp/cert.pem"


# ============================================================================
# _build_output — bigquery
# ============================================================================


def test_build_output_bigquery_puts_creds_as_keyfile_json_and_injects_extras():
    creds = {"type": "service_account", "project_id": "proj"}
    extras = {"location": "us", "priority": "batch"}
    output = _build_output("bigquery", "analytics", creds, extras, threads=4)
    assert output["type"] == "bigquery"
    assert output["method"] == "service-account-json"
    assert output["keyfile_json"] == creds
    assert output["schema"] == "analytics"
    assert output["location"] == "us"
    assert output["priority"] == "batch"


# ============================================================================
# _build_output — unsupported
# ============================================================================


def test_build_output_unsupported_wtype_raises():
    with pytest.raises(ValueError, match="Unsupported warehouse type: snowflake"):
        _build_output("snowflake", "s", {}, {}, threads=4)


# ============================================================================
# build_profile_dict
# ============================================================================


def test_build_profile_dict_shape():
    """profiles.yml top level = profile_name; nested target label matches the
    single output key."""
    profile = build_profile_dict(
        profile_name="dalgo",
        wtype="postgres",
        schema="analytics",
        creds={"username": "u", "password": "pw"},
        extras={},
    )
    assert list(profile.keys()) == ["dalgo"]
    assert profile["dalgo"]["target"] == DBT_TARGET
    assert list(profile["dalgo"]["outputs"].keys()) == [DBT_TARGET]
    assert profile["dalgo"]["outputs"][DBT_TARGET]["schema"] == "analytics"
