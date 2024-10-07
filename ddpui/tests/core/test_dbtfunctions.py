from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.core.dbtfunctions import map_airbyte_destination_spec_to_dbtcli_profile


def test_map_airbyte_destination_spec_to_dbtcli_profile_success_tunnel_params(tmpdir):
    """Tests all the success cases"""
    dbt_project_params = DbtProjectParams(
        org_project_dir=tmpdir,
        dbt_env_dir="/path/to/dbt_venv",
        dbt_repo_dir="/path/to/dbt_repo",
        project_dir="/path/to/project_dir",
        target="target",
        dbt_binary="dbt_binary",
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
    """Tests all the success cases"""
    dbt_project_params = DbtProjectParams(
        org_project_dir=tmpdir,
        dbt_env_dir="/path/to/dbt_venv",
        dbt_repo_dir="/path/to/dbt_repo",
        project_dir="/path/to/project_dir",
        target="target",
        dbt_binary="dbt_binary",
    )

    conn_info = {"ssl_mode": {"mode": "verify-ca", "ca_certificate": "ca_certificate"}}
    res = map_airbyte_destination_spec_to_dbtcli_profile(conn_info, dbt_project_params)
    assert res["sslmode"] == conn_info["ssl_mode"]["mode"]
    assert res["sslrootcert"] == f"{tmpdir}/sslrootcert.pem"
    with open(f"{tmpdir}/sslrootcert.pem") as file:
        assert file.read() == conn_info["ssl_mode"]["ca_certificate"]
