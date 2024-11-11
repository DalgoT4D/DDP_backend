from datetime import datetime, time
import pytz

from ddpui.utils.helpers import (
    remove_nested_attribute,
    isvalid_email,
    generate_hash_id,
    cleaned_name_for_prefectblock,
    map_airbyte_keys_to_postgres_keys,
    update_dict_but_not_stars,
    nice_bytes,
    get_schedule_time_for_large_jobs,
)


def test_remove_nested_attribute():
    """tests remove_nested_attribute"""
    payload = {
        "k1": "v1",
        "k2": 100,
        "k3": "v3",
        "k4": {
            "k5": "v5",
            "k6": 101,
            "k7": "v7",
            "k8": [
                {"k9": "v9", "k10": "v10", "k11": 11},
                {"k12": "v12", "k13": "v13", "k14": 14, "k15": "v15"},
                200,
                "v8",
            ],
        },
    }
    result = remove_nested_attribute(payload, "k7")
    assert result == {
        "k1": "v1",
        "k2": 100,
        "k3": "v3",
        "k4": {
            "k5": "v5",
            "k6": 101,
            "k8": [
                {"k9": "v9", "k10": "v10", "k11": 11},
                {"k12": "v12", "k13": "v13", "k14": 14, "k15": "v15"},
                200,
                "v8",
            ],
        },
    }


def test_isvalid_email_0():
    """valid email address"""
    assert isvalid_email("abc@abc.com")


def test_isvalid_email_1():
    """invalid email address"""
    assert not isvalid_email("abc@abc.com@foo")


def test_generate_hash_id():
    """tests length of generate_hash_id"""
    assert len(generate_hash_id(10)) == 10
    assert len(generate_hash_id(100)) == 100


def test_cleaned_name_for_prefectblock():
    """tests cleaned_name_for_prefectblock"""
    assert cleaned_name_for_prefectblock("blockname") == "blockname"
    assert cleaned_name_for_prefectblock("BLOCKNAME") == "blockname"
    assert cleaned_name_for_prefectblock("blockName0") == "blockname0"
    assert cleaned_name_for_prefectblock("blockName0@") == "blockname0"


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


def test_nice_bytes():
    """tests nice_bytes"""
    assert nice_bytes(1024) == "1.0 KB"
    assert nice_bytes(1024 * 1024) == "1.0 MB"
    assert nice_bytes(3 * 1024 * 1024) == "3.0 MB"


def test_get_schedule_time_for_large_jobs_1():
    """schedule for 10am UTC on the following sunday"""
    r1 = get_schedule_time_for_large_jobs(datetime(2024, 1, 1))
    assert r1.year == 2024
    assert r1.month == 1
    assert r1.day == 7
    assert r1.hour == 10
    assert r1.minute == 0
    assert r1.tzinfo == pytz.utc


def test_get_schedule_time_for_large_jobs_1_1():
    """schedule for 10am UTC on the following sunday"""
    r1 = get_schedule_time_for_large_jobs(datetime(2024, 1, 3))
    assert r1.year == 2024
    assert r1.month == 1
    assert r1.day == 7
    assert r1.hour == 10
    assert r1.minute == 0
    assert r1.tzinfo == pytz.utc


def test_get_schedule_time_for_large_jobs_2():
    """schedule for 10am UTC on the following sunday"""
    r1 = get_schedule_time_for_large_jobs(datetime(2024, 1, 4, 13, 45))
    assert r1.year == 2024
    assert r1.month == 1
    assert r1.day == 7
    assert r1.hour == 10
    assert r1.minute == 0
    assert r1.tzinfo == pytz.utc


def test_get_schedule_time_for_large_jobs_3():
    """schedule for specified time UTC on the following sunday"""
    r1 = get_schedule_time_for_large_jobs(datetime(2024, 1, 5, 13, 45), time(12, 30))
    assert r1.year == 2024
    assert r1.month == 1
    assert r1.day == 7
    assert r1.hour == 12
    assert r1.minute == 30

    assert r1.tzinfo == pytz.utc


def test_get_schedule_time_for_large_jobs_4():
    """if called on a sunday, schedule for the same sunday but an hour ahead"""
    now = datetime(2024, 1, 7, 13, 45)
    time_of_day = time(12, 30)
    r1 = get_schedule_time_for_large_jobs(curr=now, time_of_day=time_of_day)
    assert r1.year == now.year
    assert r1.month == now.month
    assert r1.day == now.day
    assert r1.hour == now.hour + 1
    assert r1.minute == now.minute
    assert r1.tzinfo == pytz.utc


def test_get_schedule_time_for_large_jobs_5():
    """make sure the scheduled time is in the future"""
    now = datetime(2024, 1, 7, 13, 45).astimezone(pytz.utc)
    r1 = get_schedule_time_for_large_jobs(now, time(12, 30))
    assert r1 >= now
