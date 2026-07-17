from unittest.mock import patch, Mock

import pytest
from django.contrib.auth.models import User
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser, UserAttributes
from ddpui.models.trial_clone import TrialCloneStatus, TrialClone
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.trial import clone_service

pytestmark = pytest.mark.django_db


@patch("ddpui.core.trial.clone_service._step_warehouse_data")
@patch("ddpui.core.trial.clone_service._step_warehouse")
@patch("ddpui.core.trial.clone_service._step_org_and_user")
def test_clone_runs_all_steps_and_completes(mock_s1, mock_s2, mock_s3):
    template = Org.objects.create(name="tmpl", slug="tmpl")
    tc = clone_service.clone_template_org(template.id, "a@b.org")
    assert tc.status == TrialCloneStatus.COMPLETED.value
    mock_s1.assert_called_once()
    mock_s2.assert_called_once()
    mock_s3.assert_called_once()
    assert set(tc.timings.keys()) == {
        "step1_org_user",
        "step2_warehouse",
        "step3_warehouse_data",
    }


@patch("ddpui.core.trial.clone_service._step_org_and_user", side_effect=RuntimeError("kaboom"))
def test_clone_marks_failed_and_reraises(mock_s1):
    template = Org.objects.create(name="tmpl2", slug="tmpl2")
    with pytest.raises(RuntimeError):
        clone_service.clone_template_org(template.id, "a@b.org")

    tc = TrialClone.objects.filter(template_org=template).first()
    assert tc.status == TrialCloneStatus.FAILED.value
    assert "kaboom" in tc.error


@patch("ddpui.core.trial.clone_service.create_org_plan")
@patch("ddpui.core.trial.clone_service.create_organization")
def test_step_org_and_user_creates_org_and_admin(mock_create_org, mock_create_plan):
    Role.objects.get_or_create(slug=ACCOUNT_MANAGER_ROLE, defaults={"name": "admin", "level": 1})
    template = Org.objects.create(name="tmpl", slug="tmpl")
    trial_org = Org.objects.create(
        name="Trial 1 tmpl", slug="trial-1-tmpl", airbyte_workspace_id="ws-9"
    )
    mock_create_org.return_value = (trial_org, None)
    mock_create_plan.return_value = (Mock(), None)

    tc = TrialClone.objects.create(template_org=template, trial_email="admin@b.org")
    clone_service._step_org_and_user(template, tc)

    tc.refresh_from_db()
    assert tc.trial_org_id == trial_org.id
    orguser = OrgUser.objects.filter(org=trial_org).first()
    assert orguser is not None
    assert orguser.user.email == "admin@b.org"
    assert orguser.new_role.slug == ACCOUNT_MANAGER_ROLE
    assert not orguser.user.has_usable_password()
    user_attrs = UserAttributes.objects.filter(user=orguser.user).first()
    assert user_attrs is not None
    assert user_attrs.email_verified is False


@patch("ddpui.core.trial.clone_service.create_warehouse")
@patch("ddpui.core.trial.clone_service.airbyte_service")
@patch("ddpui.core.trial.clone_service.retrieve_warehouse_credentials")
@patch("ddpui.core.trial.clone_service.provision_trial_database")
def test_step_warehouse_registers_trial_warehouse(
    mock_provision, mock_retrieve, mock_ab, mock_create_wh
):
    template = Org.objects.create(name="tmpl", slug="tmpl", airbyte_workspace_id="ws-tmpl")
    OrgWarehouse.objects.create(
        org=template, wtype="postgres", airbyte_destination_id="dest-tmpl", credentials="x"
    )
    trial_org = Org.objects.create(
        name="Trial 1 tmpl", slug="trial-1", airbyte_workspace_id="ws-tr"
    )

    mock_provision.return_value = {
        "host": "h",
        "port": 5432,
        "database": "trial_1",
        "username": "u",
        "password": "p",
    }
    mock_ab.get_destination.return_value = {"destinationDefinitionId": "pg-def-1"}
    mock_create_wh.return_value = (None, None)

    tc = TrialClone.objects.create(
        template_org=template, trial_email="a@b.org", trial_org=trial_org
    )
    clone_service._step_warehouse(template, tc)

    mock_provision.assert_called_once_with(tc.id)
    # create_warehouse called with the trial org + a schema carrying the new db + def id
    args, _ = mock_create_wh.call_args
    assert args[0] == trial_org
    assert args[1].destinationDefId == "pg-def-1"
    assert args[1].airbyteConfig["database"] == "trial_1"
    tc.refresh_from_db()
    assert tc.manifest["trial_warehouse_db"] == "trial_1"


@patch("ddpui.core.trial.clone_service.copy_warehouse_data")
@patch("ddpui.core.trial.clone_service.retrieve_warehouse_credentials")
def test_step_warehouse_data_copies(mock_retrieve, mock_copy):
    template = Org.objects.create(name="tmpl", slug="tmpl")
    trial_org = Org.objects.create(name="Trial 1 tmpl", slug="trial-1")
    OrgWarehouse.objects.create(org=template, wtype="postgres", credentials="tmpl-sec")
    OrgWarehouse.objects.create(org=trial_org, wtype="postgres", credentials="trial-sec")

    mock_retrieve.side_effect = [
        {"host": "sh", "port": 5432, "database": "sdb", "username": "su", "password": "sp"},
        {"host": "dh", "port": 5432, "database": "trial_1", "username": "du", "password": "dp"},
    ]

    tc = TrialClone.objects.create(
        template_org=template, trial_email="a@b.org", trial_org=trial_org
    )
    clone_service._step_warehouse_data(template, tc)

    mock_copy.assert_called_once()
    src, dst, _path = mock_copy.call_args.args
    assert src["database"] == "sdb"
    assert dst["database"] == "trial_1"
