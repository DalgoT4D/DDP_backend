from unittest.mock import patch, MagicMock, call

import pytest
from ddpui.core.trial import warehouse_provision
from ddpui.core.trial.warehouse_provision import (
    _ft_key,
    ft_database_name,
    ft_role_name,
    provision_trial_database,
    drop_trial_database,
)

pytestmark = pytest.mark.django_db


# --------------------------------------------------------------------------
# naming helpers
# --------------------------------------------------------------------------


def test_ft_database_name_is_deterministic_and_safe():
    email = "support+cc1@dalgo.org"
    name = ft_database_name(email)

    assert name == ft_database_name(email)  # deterministic
    assert name.startswith("ft_support_cc1_dalgo_org_")
    assert name.endswith("_db")
    assert len(name) <= 63
    # postgres-safe identifier: only lowercase letters, digits, underscore; can't start with digit
    import re

    assert re.fullmatch(r"[a-z_][a-z0-9_]*", name)


def test_ft_role_name_is_deterministic_and_safe():
    email = "support+cc1@dalgo.org"
    name = ft_role_name(email)

    assert name == ft_role_name(email)
    assert name.startswith("ft_support_cc1_dalgo_org_")
    assert name.endswith("_user")
    assert len(name) <= 63
    import re

    assert re.fullmatch(r"[a-z_][a-z0-9_]*", name)


def test_ft_names_differ_when_sanitized_local_parts_collide():
    """'a.b@x.com' and 'a_b@x.com' both sanitize to local part 'a_b', but the emails differ
    so the hash suffix must differ, keeping the derived names distinct (collision-safety)."""
    email1 = "a.b@x.com"
    email2 = "a_b@x.com"

    assert _ft_key(email1) != _ft_key(email2)
    assert ft_database_name(email1) != ft_database_name(email2)
    assert ft_role_name(email1) != ft_role_name(email2)


# --------------------------------------------------------------------------
# provision_trial_database
# --------------------------------------------------------------------------


@patch("ddpui.core.trial.warehouse_provision.psycopg2")
@patch("ddpui.core.trial.warehouse_provision.settings")
def test_provision_creates_database_and_dedicated_role(mock_settings, mock_psycopg2):
    mock_settings.TRIALS_RDS_HOST = "rds-host"
    mock_settings.TRIALS_RDS_PORT = 5432
    mock_settings.TRIALS_RDS_ADMIN_USER = "admin"
    mock_settings.TRIALS_RDS_ADMIN_PASSWORD = "adminpass"

    admin_conn = MagicMock()
    admin_cursor = MagicMock()
    admin_conn.cursor.return_value.__enter__.return_value = admin_cursor

    ft_db_conn = MagicMock()
    ft_db_cursor = MagicMock()
    ft_db_conn.cursor.return_value.__enter__.return_value = ft_db_cursor

    mock_psycopg2.connect.side_effect = [admin_conn, ft_db_conn]

    email = "support+cc1@dalgo.org"
    expected_db = ft_database_name(email)
    expected_role = ft_role_name(email)

    params = provision_trial_database(email)

    # two connections: one to maintenance 'postgres' db, one to the freshly created ft db
    assert mock_psycopg2.connect.call_count == 2
    first_call_kwargs = mock_psycopg2.connect.call_args_list[0].kwargs
    second_call_kwargs = mock_psycopg2.connect.call_args_list[1].kwargs
    assert first_call_kwargs["dbname"] == "postgres"
    assert second_call_kwargs["dbname"] == expected_db

    # both connections used autocommit and were closed
    assert admin_conn.autocommit is True
    assert ft_db_conn.autocommit is True
    admin_conn.close.assert_called_once()
    ft_db_conn.close.assert_called_once()

    admin_statements = " ".join(str(c.args[0]) for c in admin_cursor.execute.call_args_list)
    assert f'CREATE DATABASE "{expected_db}"' in admin_statements
    assert f'CREATE ROLE "{expected_role}" LOGIN PASSWORD' in admin_statements
    assert f'GRANT "{expected_role}" TO CURRENT_USER' in admin_statements
    assert f'ALTER DATABASE "{expected_db}" OWNER TO "{expected_role}"' in admin_statements

    ft_db_statements = " ".join(str(c.args[0]) for c in ft_db_cursor.execute.call_args_list)
    assert f'GRANT ALL ON SCHEMA public TO "{expected_role}"' in ft_db_statements
    assert f'ALTER SCHEMA public OWNER TO "{expected_role}"' in ft_db_statements

    assert params["host"] == "rds-host"
    assert params["port"] == 5432
    assert params["database"] == expected_db
    assert params["username"] == expected_role
    assert params["password"]  # non-empty
    assert params["password"] != "adminpass"  # NOT admin creds


@patch("ddpui.core.trial.warehouse_provision.psycopg2")
@patch("ddpui.core.trial.warehouse_provision.settings")
def test_provision_never_logs_password(mock_settings, mock_psycopg2, caplog):
    mock_settings.TRIALS_RDS_HOST = "rds-host"
    mock_settings.TRIALS_RDS_PORT = 5432
    mock_settings.TRIALS_RDS_ADMIN_USER = "admin"
    mock_settings.TRIALS_RDS_ADMIN_PASSWORD = "adminpass"

    admin_conn = MagicMock()
    admin_cursor = MagicMock()
    admin_conn.cursor.return_value.__enter__.return_value = admin_cursor
    ft_db_conn = MagicMock()
    ft_db_cursor = MagicMock()
    ft_db_conn.cursor.return_value.__enter__.return_value = ft_db_cursor
    mock_psycopg2.connect.side_effect = [admin_conn, ft_db_conn]

    with caplog.at_level("DEBUG"):
        params = provision_trial_database("someone@example.com")

    password = params["password"]
    assert password
    for record in caplog.records:
        assert password not in record.getMessage()


@patch("ddpui.core.trial.warehouse_provision.psycopg2")
@patch("ddpui.core.trial.warehouse_provision.settings")
def test_provision_server_side_copy_from_template(mock_settings, mock_psycopg2):
    mock_settings.TRIALS_RDS_HOST = "rds"
    mock_settings.TRIALS_RDS_PORT = 5432
    mock_settings.TRIALS_RDS_ADMIN_USER = "admin"
    mock_settings.TRIALS_RDS_ADMIN_PASSWORD = "pw"
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    mock_psycopg2.connect.return_value = conn
    from ddpui.core.trial import warehouse_provision

    params = warehouse_provision.provision_trial_database("a@b.org", template_db="himanshu_wh")
    executed = " ".join(str(c.args[0]) for c in cursor.execute.call_args_list)
    assert "TEMPLATE" in executed and "himanshu_wh" in executed  # server-side copy issued
    assert params["database"].startswith("ft_")


# --------------------------------------------------------------------------
# drop_trial_database
# --------------------------------------------------------------------------


@patch("ddpui.core.trial.warehouse_provision.psycopg2")
@patch("ddpui.core.trial.warehouse_provision.settings")
def test_drop_trial_database(mock_settings, mock_psycopg2):
    mock_settings.TRIALS_RDS_HOST = "rds-host"
    mock_settings.TRIALS_RDS_PORT = 5432
    mock_settings.TRIALS_RDS_ADMIN_USER = "admin"
    mock_settings.TRIALS_RDS_ADMIN_PASSWORD = "adminpass"

    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    mock_psycopg2.connect.return_value = conn

    email = "someone@example.com"
    expected_db = ft_database_name(email)
    expected_role = ft_role_name(email)

    drop_trial_database(email)

    executed = [str(c.args[0]) for c in cursor.execute.call_args_list]
    joined = " ".join(executed)
    assert f'DROP DATABASE IF EXISTS "{expected_db}"' in joined
    assert f'DROP ROLE IF EXISTS "{expected_role}"' in joined
    # db must be dropped before the role (role owns the db)
    db_idx = next(i for i, s in enumerate(executed) if "DROP DATABASE" in s)
    role_idx = next(i for i, s in enumerate(executed) if "DROP ROLE" in s)
    assert db_idx < role_idx

    assert conn.autocommit is True
    conn.close.assert_called_once()
