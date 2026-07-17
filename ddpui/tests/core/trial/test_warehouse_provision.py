from unittest.mock import patch, MagicMock

import pytest
from ddpui.models.org import Org
from ddpui.models.trial_clone import TrialClone
from ddpui.core.trial import warehouse_provision

pytestmark = pytest.mark.django_db


@patch("ddpui.core.trial.warehouse_provision.psycopg2")
@patch("ddpui.core.trial.warehouse_provision.settings")
def test_provision_creates_database(mock_settings, mock_psycopg2):
    mock_settings.TRIALS_RDS_HOST = "rds-host"
    mock_settings.TRIALS_RDS_PORT = 5432
    mock_settings.TRIALS_RDS_ADMIN_USER = "admin"
    mock_settings.TRIALS_RDS_ADMIN_PASSWORD = "adminpass"

    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    mock_psycopg2.connect.return_value = conn

    org = Org.objects.create(name="t", slug="t")
    tc = TrialClone.objects.create(template_org=org, trial_email="a@b.org")

    params = warehouse_provision.provision_trial_database(tc.id)

    assert params["host"] == "rds-host"
    assert params["database"] == f"trial_{tc.id}"
    assert params["username"] == "admin"
    # CREATE DATABASE was issued
    executed = " ".join(str(c.args[0]) for c in cursor.execute.call_args_list)
    assert f"trial_{tc.id}" in executed
    # connection closed and autocommit set (CREATE DATABASE cannot run in a transaction)
    assert conn.autocommit is True
    conn.close.assert_called_once()
