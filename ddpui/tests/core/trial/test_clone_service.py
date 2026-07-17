from unittest.mock import patch, Mock

import pytest
from django.contrib.auth.models import User
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.trial_clone import TrialCloneStatus
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
    from ddpui.models.trial_clone import TrialClone

    tc = TrialClone.objects.filter(template_org=template).first()
    assert tc.status == TrialCloneStatus.FAILED.value
    assert "kaboom" in tc.error


@patch("ddpui.core.trial.clone_service.create_org_plan")
@patch("ddpui.core.trial.clone_service.create_organization")
def test_step_org_and_user_creates_org_and_admin(mock_create_org, mock_create_plan):
    from ddpui.models.role_based_access import Role
    from ddpui.models.trial_clone import TrialClone
    from ddpui.core.trial import clone_service
    from ddpui.auth import ACCOUNT_MANAGER_ROLE

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
