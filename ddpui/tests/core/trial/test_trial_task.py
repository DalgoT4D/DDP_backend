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
    # C1: the client-supplied "role" is job-title metadata only — it must never be forwarded
    # as the RBAC role_slug. role_slug=None lets clone_template_org apply its own default
    # (ACCOUNT_MANAGER_ROLE).
    assert kwargs["role_slug"] is None
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
def test_clone_trial_org_task_never_forwards_client_role_as_rbac_role(
    mock_clone, mock_taskprogress_cls
):
    """C1: even a self-escalation attempt (role='super-admin') must never reach role_slug."""
    from ddpui.core.trial.tasks import clone_trial_org_task

    mock_progress = MagicMock()
    mock_taskprogress_cls.return_value = mock_progress
    mock_run = MagicMock()
    mock_run.trial_org.slug = "trial-xyz"
    mock_clone.return_value = mock_run

    clone_trial_org_task("task-3", 5, "attacker@b.org", "Acme", "super-admin")

    _, kwargs = mock_clone.call_args
    assert kwargs["role_slug"] != "super-admin"
    assert kwargs["role_slug"] is None


@patch("ddpui.core.trial.tasks.TaskProgress")
@patch("ddpui.core.trial.tasks.clone_template_org")
def test_clone_trial_org_task_failure_records_progress(mock_clone, mock_taskprogress_cls):
    from ddpui.core.trial.tasks import clone_trial_org_task

    mock_progress = MagicMock()
    mock_taskprogress_cls.return_value = mock_progress
    mock_clone.side_effect = RuntimeError("boom: sensitive internal detail")

    # should not raise
    clone_trial_org_task("task-2", 5, "a@b.org", "Acme", "account-manager")

    mock_progress.add.assert_any_call({"message": "queued", "status": "queued"})
    # M2: the polled progress must not leak raw exception text — generic message only.
    failed_calls = [
        c.args[0] for c in mock_progress.add.call_args_list if c.args[0].get("status") == "failed"
    ]
    assert len(failed_calls) == 1
    assert failed_calls[0]["message"] == "clone failed"
    assert "boom" not in failed_calls[0]["message"]
