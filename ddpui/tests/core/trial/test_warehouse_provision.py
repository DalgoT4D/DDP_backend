from unittest.mock import patch, MagicMock, call

import pytest
from ddpui.core.trial import warehouse_provision
from ddpui.core.trial.warehouse_provision import (
    _ft_key,
    _reassign_copied_objects,
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

    params = provision_trial_database(email, template_db="tmpl_db")

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

    assert params.host == "rds-host"
    assert params.port == 5432
    assert params.database == expected_db
    assert params.username == expected_role
    assert params.password  # non-empty
    assert params.password != "adminpass"  # NOT admin creds


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
    assert params.database.startswith("ft_")


def test_provision_requires_template_db():
    """The trial db is always a server-side copy — an empty/None template_db must fail fast."""
    with pytest.raises(ValueError, match="template_db"):
        provision_trial_database("a@b.org", template_db=None)


@patch("ddpui.core.trial.warehouse_provision.time.sleep")
@patch("ddpui.core.trial.warehouse_provision.psycopg2")
@patch("ddpui.core.trial.warehouse_provision.settings")
def test_template_copy_terminates_blocking_sessions_before_create(
    mock_settings, mock_psycopg2, _mock_sleep
):
    """The copy must terminate other sessions on the template db first, else Postgres raises
    ObjectInUse on `CREATE DATABASE ... TEMPLATE` (the real-world step-2 failure this fixes)."""
    mock_settings.TRIALS_RDS_HOST = "rds"
    mock_settings.TRIALS_RDS_PORT = 5432
    mock_settings.TRIALS_RDS_ADMIN_USER = "admin"
    mock_settings.TRIALS_RDS_ADMIN_PASSWORD = "pw"
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    mock_psycopg2.connect.return_value = conn

    warehouse_provision.provision_trial_database("a@b.org", template_db="himanshu_wh")

    executed = [str(c.args[0]) for c in cursor.execute.call_args_list]
    terminate_idx = next(i for i, s in enumerate(executed) if "pg_terminate_backend" in s)
    create_idx = next(
        i for i, s in enumerate(executed) if "CREATE DATABASE" in s and "TEMPLATE" in s
    )
    assert terminate_idx < create_idx  # sessions cleared BEFORE the copy


@patch("ddpui.core.trial.warehouse_provision.time.sleep")
@patch("ddpui.core.trial.warehouse_provision.psycopg2")
@patch("ddpui.core.trial.warehouse_provision.settings")
def test_template_copy_retries_when_a_session_races_back_in(
    mock_settings, mock_psycopg2, mock_sleep
):
    """If a new session connects between terminate and CREATE, the copy raises ObjectInUse; the
    helper terminates + retries until it wins (here: fails once, then succeeds)."""
    mock_settings.TRIALS_RDS_HOST = "rds"
    mock_settings.TRIALS_RDS_PORT = 5432
    mock_settings.TRIALS_RDS_ADMIN_USER = "admin"
    mock_settings.TRIALS_RDS_ADMIN_PASSWORD = "pw"

    class FakeObjectInUse(Exception):
        pass

    mock_psycopg2.errors.ObjectInUse = FakeObjectInUse

    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    mock_psycopg2.connect.return_value = conn

    create_attempts = {"n": 0}

    def exec_side_effect(sql, *args, **kwargs):
        text = str(sql)
        if "CREATE DATABASE" in text and "TEMPLATE" in text:
            create_attempts["n"] += 1
            if create_attempts["n"] == 1:
                raise FakeObjectInUse("database is being accessed by other users")
        return MagicMock()

    cursor.execute.side_effect = exec_side_effect

    warehouse_provision.provision_trial_database("a@b.org", template_db="himanshu_wh")

    assert create_attempts["n"] == 2  # first raced, second won
    mock_sleep.assert_called()  # backed off between attempts


@patch("ddpui.core.trial.warehouse_provision.drop_trial_database")
@patch("ddpui.core.trial.warehouse_provision.psycopg2")
@patch("ddpui.core.trial.warehouse_provision.settings")
def test_provision_rolls_back_on_partial_failure(mock_settings, mock_psycopg2, mock_drop):
    """A failure AFTER CREATE DATABASE (here: the public-schema grant on the ft db) must drop
    the half-created db+role before re-raising, else it leaks and blocks every retry for the
    email (CREATE DATABASE has no IF NOT EXISTS)."""
    mock_settings.TRIALS_RDS_HOST = "rds-host"
    mock_settings.TRIALS_RDS_PORT = 5432
    mock_settings.TRIALS_RDS_ADMIN_USER = "admin"
    mock_settings.TRIALS_RDS_ADMIN_PASSWORD = "adminpass"

    admin_conn = MagicMock()
    admin_cursor = MagicMock()
    admin_conn.cursor.return_value.__enter__.return_value = admin_cursor
    # second connection (to the freshly-created ft db) blows up mid-provision
    ft_db_conn = MagicMock()
    ft_db_cursor = MagicMock()
    ft_db_cursor.execute.side_effect = Exception("grant failed")
    ft_db_conn.cursor.return_value.__enter__.return_value = ft_db_cursor
    mock_psycopg2.connect.side_effect = [admin_conn, ft_db_conn]

    with pytest.raises(Exception, match="grant failed"):
        provision_trial_database("someone@example.com", template_db="tmpl_db")

    mock_drop.assert_called_once_with("someone@example.com")
    admin_conn.close.assert_called_once()  # first connection still cleanly closed
    ft_db_conn.close.assert_called_once()  # second connection closed by its finally before rollback


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


@patch("ddpui.core.trial.warehouse_provision.psycopg2")
@patch("ddpui.core.trial.warehouse_provision.settings")
def test_drop_terminates_active_sessions_before_dropping(mock_settings, mock_psycopg2):
    """When the db still exists, block new connections + terminate live sessions BEFORE the
    DROP, so an in-use db (recent clone / pooled connection) can't strand the db+role."""
    mock_settings.TRIALS_RDS_HOST = "rds-host"
    mock_settings.TRIALS_RDS_PORT = 5432
    mock_settings.TRIALS_RDS_ADMIN_USER = "admin"
    mock_settings.TRIALS_RDS_ADMIN_PASSWORD = "adminpass"

    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    cursor.fetchone.return_value = (1,)  # db exists
    cursor.rowcount = 2
    mock_psycopg2.connect.return_value = conn

    email = "someone@example.com"
    expected_db = ft_database_name(email)

    drop_trial_database(email)

    executed = [str(c.args[0]) for c in cursor.execute.call_args_list]
    joined = " ".join(executed)
    assert f'REVOKE CONNECT ON DATABASE "{expected_db}" FROM PUBLIC' in joined
    assert "pg_terminate_backend" in joined
    # terminate must happen before the DROP DATABASE
    terminate_idx = next(i for i, s in enumerate(executed) if "pg_terminate_backend" in s)
    drop_idx = next(i for i, s in enumerate(executed) if "DROP DATABASE" in s)
    assert terminate_idx < drop_idx


@patch("ddpui.core.trial.warehouse_provision.psycopg2")
@patch("ddpui.core.trial.warehouse_provision.settings")
def test_drop_skips_terminate_when_db_absent(mock_settings, mock_psycopg2):
    """If the db is already gone, don't REVOKE/terminate (REVOKE has no IF-EXISTS and would
    raise) — just run the idempotent DROP ... IF EXISTS statements."""
    mock_settings.TRIALS_RDS_HOST = "rds-host"
    mock_settings.TRIALS_RDS_PORT = 5432
    mock_settings.TRIALS_RDS_ADMIN_USER = "admin"
    mock_settings.TRIALS_RDS_ADMIN_PASSWORD = "adminpass"

    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    cursor.fetchone.return_value = None  # db already gone
    mock_psycopg2.connect.return_value = conn

    drop_trial_database("gone@example.com")

    joined = " ".join(str(c.args[0]) for c in cursor.execute.call_args_list)
    assert "REVOKE CONNECT" not in joined
    assert "pg_terminate_backend" not in joined
    assert "DROP DATABASE IF EXISTS" in joined
    assert "DROP ROLE IF EXISTS" in joined


# --------------------------------------------------------------------------
# _reassign_copied_objects
# --------------------------------------------------------------------------


def test_reassign_copied_objects_transfers_only_non_system_owners():
    """Objects copied from the template keep the template's owner role; ownership must move to
    the ft role so the trial's chart queries + dbt rebuild work. ft_role/postgres/pg_* are skipped
    (ft owns its own objects; public is handled separately; pg_* are system roles)."""
    ft_role = "ft_x_user"
    cursor = MagicMock()
    # 1st fetchall: owner discovery; 2nd: databases owned by "template_wh" (none here)
    cursor.fetchall.side_effect = [
        [("template_wh",), (ft_role,), ("postgres",), ("pg_monitor",)],
        [],
    ]

    _reassign_copied_objects(cursor, ft_role)

    stmts = [str(c.args[0]) for c in cursor.execute.call_args_list]
    joined = " ".join(stmts)

    # the template owner is fully transferred (grant → reassign → revoke)
    assert 'GRANT "template_wh" TO CURRENT_USER' in joined
    assert f'REASSIGN OWNED BY "template_wh" TO "{ft_role}"' in joined
    assert 'REVOKE "template_wh" FROM CURRENT_USER' in joined

    # ft_role / postgres / pg_* are never reassigned
    assert 'REASSIGN OWNED BY "postgres"' not in joined
    assert 'REASSIGN OWNED BY "pg_monitor"' not in joined
    assert f'REASSIGN OWNED BY "{ft_role}"' not in joined
    # no databases owned by template_wh → no ownership restore needed
    assert "ALTER DATABASE" not in joined


def test_reassign_copied_objects_restores_database_ownership():
    """REASSIGN OWNED also transfers cluster-wide SHARED objects — a DATABASE owned by the
    template role (e.g. role health_demo_staging owning db health_demo_staging) would silently
    follow the REASSIGN onto the ft role, hijacking the TEMPLATE db and blocking the ft role's
    teardown. The db's ownership must be restored to its original owner immediately after the
    REASSIGN, inside the temporary-membership window (before the REVOKE)."""
    ft_role = "ft_x_user"
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [("health_demo_staging",)],  # owner discovery
        [("health_demo_staging",)],  # databases owned by that role
    ]

    _reassign_copied_objects(cursor, ft_role)

    stmts = [str(c.args[0]) for c in cursor.execute.call_args_list]
    reassign_idx = next(i for i, s in enumerate(stmts) if s.startswith("REASSIGN OWNED"))
    restore_idx = next(i for i, s in enumerate(stmts) if s.startswith("ALTER DATABASE"))
    revoke_idx = next(i for i, s in enumerate(stmts) if s.startswith("REVOKE"))

    assert (
        stmts[restore_idx] == 'ALTER DATABASE "health_demo_staging" OWNER TO "health_demo_staging"'
    )
    # restore happens AFTER the reassign and BEFORE the membership revoke
    assert reassign_idx < restore_idx < revoke_idx


def test_reassign_copied_objects_noop_when_no_foreign_owners():
    """Only ft_role/postgres present (e.g. nothing copied) → no GRANT/REASSIGN/REVOKE issued."""
    ft_role = "ft_x_user"
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[(ft_role,), ("postgres",)]]

    _reassign_copied_objects(cursor, ft_role)

    joined = " ".join(str(c.args[0]) for c in cursor.execute.call_args_list)
    assert "REASSIGN OWNED BY" not in joined
    assert "GRANT" not in joined
