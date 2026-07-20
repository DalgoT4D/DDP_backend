from unittest.mock import patch, MagicMock

import pytest

pytestmark = pytest.mark.django_db


@patch("ddpui.core.trial.tasks.TaskProgress")
@patch("ddpui.core.trial.tasks.clone_template_org")
def test_clone_trial_org_task_success(mock_clone, mock_taskprogress_cls):
    from ddpui.core.trial.tasks import clone_trial_org_task

    mock_progress = MagicMock()
    mock_taskprogress_cls.return_value = mock_progress

    mock_run = MagicMock()
    mock_run.trial_org.slug = "trial-abc123"
    mock_clone.return_value = mock_run

    clone_trial_org_task("task-1", 5, "a@b.org", "Acme", "account-manager")

    mock_taskprogress_cls.assert_called_once_with("task-1", "trial-clone-task-1")

    mock_clone.assert_called_once()
    args, kwargs = mock_clone.call_args
    assert args == (5, "a@b.org")
    assert kwargs["org_name"] == "Acme"
    assert kwargs["role_slug"] == "account-manager"
    assert callable(kwargs["progress"])

    # exercise the progress callback passed to clone_template_org
    kwargs["progress"](2, "Setting up your warehouse")
    mock_progress.add.assert_any_call(
        {"step": 2, "message": "Setting up your warehouse", "status": "running"}
    )

    # queued progress written first
    mock_progress.add.assert_any_call({"message": "queued", "status": "queued"})
    # completed progress written on success
    mock_progress.add.assert_any_call(
        {"message": "done", "status": "completed", "org_slug": "trial-abc123"}
    )


@patch("ddpui.core.trial.tasks.TaskProgress")
@patch("ddpui.core.trial.tasks.clone_template_org")
def test_clone_trial_org_task_failure_records_progress(mock_clone, mock_taskprogress_cls):
    from ddpui.core.trial.tasks import clone_trial_org_task

    mock_progress = MagicMock()
    mock_taskprogress_cls.return_value = mock_progress
    mock_clone.side_effect = RuntimeError("boom")

    # should not raise
    clone_trial_org_task("task-2", 5, "a@b.org", "Acme", "account-manager")

    mock_progress.add.assert_any_call({"message": "queued", "status": "queued"})
    mock_progress.add.assert_any_call({"message": "boom", "status": "failed"})
