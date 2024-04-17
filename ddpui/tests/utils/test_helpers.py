from ddpui.utils.helpers import (
    map_airbyte_keys_to_postgres_keys,
    update_dict_but_not_stars,
)


def test_map_airbyte_keys_to_postgres_keys_oldconfig():
    """verifies the correct mapping of keys"""
    conn_info = {
        "host": "host",
        "port": 100,
        "username": "user",
        "password": "password",
        "database": "database",
    }
    conn_info = map_airbyte_keys_to_postgres_keys(conn_info)
    assert conn_info["host"] == "host"
    assert conn_info["port"] == 100
    assert conn_info["user"] == "user"
    assert conn_info["password"] == "password"
    assert conn_info["database"] == "database"


def test_map_airbyte_keys_to_postgres_keys_sshkey():
    """verifies the correct mapping of keys"""
    conn_info = {
        "host": "host",
        "port": 100,
        "username": "user",
        "password": "password",
        "database": "database",
        "tunnel_method": {
            "tunnel_method": "SSH_KEY_AUTH",
            "tunnel_host": "host",
            "tunnel_port": 22,
            "tunnel_user": "user",
            "ssh_key": "ssh-key",
        },
    }
    conn_info = map_airbyte_keys_to_postgres_keys(conn_info)
    assert conn_info["ssh_host"] == "host"
    assert conn_info["ssh_port"] == 22
    assert conn_info["ssh_username"] == "user"
    assert conn_info["ssh_pkey"] == "ssh-key"


def test_map_airbyte_keys_to_postgres_keys_password():
    """verifies the correct mapping of keys"""
    conn_info = {
        "host": "host",
        "port": 100,
        "username": "user",
        "password": "password",
        "database": "database",
        "tunnel_method": {
            "tunnel_method": "SSH_PASSWORD_AUTH",
            "tunnel_host": "host",
            "tunnel_port": 22,
            "tunnel_user": "user",
            "tunnel_user_password": "ssh-password",
        },
    }
    conn_info = map_airbyte_keys_to_postgres_keys(conn_info)
    assert conn_info["ssh_host"] == "host"
    assert conn_info["ssh_port"] == 22
    assert conn_info["ssh_username"] == "user"
    assert conn_info["ssh_password"] == "ssh-password"


def test_map_airbyte_keys_to_postgres_keys_notunnel():
    """verifies the correct mapping of keys"""
    conn_info = {
        "host": "host",
        "port": 100,
        "username": "user",
        "password": "password",
        "database": "database",
        "tunnel_method": {
            "tunnel_method": "NO_TUNNEL",
        },
    }
    conn_info = map_airbyte_keys_to_postgres_keys(conn_info)
    assert conn_info["host"] == "host"
    assert conn_info["port"] == 100
    assert conn_info["user"] == "user"
    assert conn_info["password"] == "password"


def test_update_dict_but_not_stars():
    """tests update_dict_but_not_stars"""
    payload = {
        "k1": "v1",
        "k2": 100,
        "k3": "*******",
        "k4": {
            "k5": "v5",
            "k6": 101,
            "k7": "*****",
            "k8": [
                {"k9": "*****", "k10": "v10", "k11": 11},
                {"k12": "*****", "k13": "v13", "k14": 14, "k15": "*****"},
                200,
                "v8",
            ],
        },
    }
    result = update_dict_but_not_stars(payload)
    assert result == {
        "k1": "v1",
        "k2": 100,
        "k4": {
            "k5": "v5",
            "k6": 101,
            "k8": [{"k10": "v10", "k11": 11}, {"k13": "v13", "k14": 14}, 200, "v8"],
        },
    }
