from unittest.mock import patch

import pytest
from ddpui.models.org import Org
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
