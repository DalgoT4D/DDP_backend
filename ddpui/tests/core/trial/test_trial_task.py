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

    mock_taskprogress_cls.assert_called_once_with("task-1", "trial-clone-task-1", 86400)

    mock_clone.assert_called_once()
    args, kwargs = mock_clone.call_args
    payload = args[0]
    assert payload.template_org_id == 5
    assert payload.trial_email == "a@b.org"
    assert payload.org_name == "Acme"
    # C1: the client-supplied "role" is job-title metadata only — it must never be forwarded
    # as the RBAC role_slug. role_slug=None lets clone_template_org apply its own default
    # (ACCOUNT_MANAGER_ROLE).
    assert payload.role_slug is None
    # it IS forwarded as work_domain, the plain job-title field on OrgUser
    assert payload.work_domain == "account-manager"
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

    args, _ = mock_clone.call_args
    payload = args[0]
    assert payload.role_slug != "super-admin"
    assert payload.role_slug is None
    # it lands in work_domain instead, which grants nothing
    assert payload.work_domain == "super-admin"


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


@patch("ddpui.core.trial.tasks.release_clone_lock")
@patch("ddpui.core.trial.tasks.TaskProgress")
@patch("ddpui.core.trial.tasks.clone_template_org")
def test_clone_task_releases_lock_on_success(mock_clone, mock_taskprogress_cls, mock_release):
    """The per-email running-clone lock must be freed when the clone finishes, so a later
    retry (or a fresh trial for that email) isn't blocked until the TTL backstop expires."""
    from ddpui.core.trial.tasks import clone_trial_org_task

    mock_taskprogress_cls.return_value = MagicMock()
    mock_run = MagicMock()
    mock_run.trial_org.slug = "trial-ok"
    mock_clone.return_value = mock_run

    clone_trial_org_task("task-ok", 5, "ok@b.org", "Acme", "account-manager")

    mock_release.assert_called_once_with("ok@b.org")


@patch("ddpui.core.trial.tasks.release_clone_lock")
@patch("ddpui.core.trial.tasks.TaskProgress")
@patch("ddpui.core.trial.tasks.clone_template_org")
def test_clone_task_releases_lock_on_failure(mock_clone, mock_taskprogress_cls, mock_release):
    """Lock freed on the failure path too (finally) — otherwise "Try again" would hit a held
    lock and 409 until the TTL expired."""
    from ddpui.core.trial.tasks import clone_trial_org_task

    mock_taskprogress_cls.return_value = MagicMock()
    mock_clone.side_effect = RuntimeError("boom")

    clone_trial_org_task("task-fail", 5, "fail@b.org", "Acme", "account-manager")

    mock_release.assert_called_once_with("fail@b.org")


@patch("ddpui.core.trial.tasks.release_clone_lock")
@patch("ddpui.core.trial.tasks.TaskProgress")
@patch("ddpui.core.trial.tasks.clone_template_org")
def test_clone_task_timeout_records_failed_and_releases_lock(
    mock_clone, mock_taskprogress_cls, mock_release
):
    """The soft_time_limit path: a clone that runs past CLONE_SOFT_TIME_LIMIT raises
    SoftTimeLimitExceeded inside the task. It must be handled exactly like any other failure —
    generic "clone failed" progress (no raw leak), no re-raise, and the lock released — so a
    wedged clone still lets the user "Try again"."""
    from celery.exceptions import SoftTimeLimitExceeded
    from ddpui.core.trial.tasks import clone_trial_org_task

    mock_progress = MagicMock()
    mock_taskprogress_cls.return_value = mock_progress
    mock_clone.side_effect = SoftTimeLimitExceeded()

    # must not propagate — the task swallows it into a "failed" progress entry
    clone_trial_org_task("task-timeout", 5, "slow@b.org", "Acme", "account-manager")

    failed_calls = [
        c.args[0] for c in mock_progress.add.call_args_list if c.args[0].get("status") == "failed"
    ]
    assert len(failed_calls) == 1
    assert failed_calls[0]["message"] == "clone failed"
    mock_release.assert_called_once_with("slow@b.org")


def _run_with_org(org_name="Trial a1b2c3d4 Acme", slug="trial-a1b2c3d4-acme"):
    """A CloneRun stand-in whose org/orguser render like real rows in the notification email."""
    import datetime
    from types import SimpleNamespace

    run = MagicMock()
    run.trial_org = SimpleNamespace(
        name=org_name,
        slug=slug,
        created_at=datetime.datetime(2026, 8, 1, 9, 12, tzinfo=datetime.timezone.utc),
    )
    run.trial_orguser = SimpleNamespace(
        user=SimpleNamespace(email="a@b.org", get_full_name=lambda: ""),
        work_domain="monitoring_evaluation",
        new_role=SimpleNamespace(name="Account Manager"),
    )
    return run


@patch("ddpui.core.trial.tasks.biz_dev_notifications")
@patch("ddpui.core.trial.tasks.OrgPlans")
@patch("ddpui.core.trial.tasks.TaskProgress")
@patch("ddpui.core.trial.tasks.clone_template_org")
def test_clone_task_notifies_biz_dev_on_success(
    mock_clone, mock_taskprogress_cls, mock_orgplans, mock_biz_dev
):
    """A finished clone means an org now exists — biz-dev is told, with the signup's details."""
    from types import SimpleNamespace
    from ddpui.core.trial.tasks import clone_trial_org_task

    mock_taskprogress_cls.return_value = MagicMock()
    mock_clone.return_value = _run_with_org()
    mock_orgplans.objects.filter.return_value.first.return_value = SimpleNamespace(
        base_plan="Free Trial"
    )

    clone_trial_org_task("task-notify", 5, "a@b.org", "Acme", "monitoring_evaluation")

    subject, body = mock_biz_dev.send_notification.call_args[0]
    assert subject == "New org created: Trial a1b2c3d4 Acme"
    assert "A new org has been created." in body
    assert "  Slug:         trial-a1b2c3d4-acme\n" in body
    assert "  Type:         Free Trial\n" in body
    assert "  Created:      2026-08-01 09:12 UTC\n" in body
    assert "  Email:        a@b.org\n" in body
    assert "  Function:     Monitoring and Evaluation\n" in body
    assert "  Dalgo role:   Account Manager\n" in body


@patch("ddpui.core.trial.tasks.biz_dev_notifications")
@patch("ddpui.core.trial.tasks.OrgPlans")
@patch("ddpui.core.trial.tasks.TaskProgress")
@patch("ddpui.core.trial.tasks.clone_template_org")
def test_clone_task_survives_a_failing_biz_dev_notification(
    mock_clone, mock_taskprogress_cls, mock_orgplans, mock_biz_dev
):
    """The clone is already done — a mail problem must not turn it into a failed task."""
    from ddpui.core.trial.tasks import clone_trial_org_task

    mock_progress = MagicMock()
    mock_taskprogress_cls.return_value = mock_progress
    mock_clone.return_value = _run_with_org()
    mock_orgplans.objects.filter.side_effect = RuntimeError("db gone")

    clone_trial_org_task("task-mailfail", 5, "a@b.org", "Acme", "monitoring_evaluation")

    # still reported as completed, and nothing raised
    assert any(c.args[0].get("status") == "completed" for c in mock_progress.add.call_args_list)
    mock_biz_dev.send_notification.assert_not_called()


@patch("ddpui.core.trial.tasks.biz_dev_notifications")
@patch("ddpui.core.trial.tasks.TaskProgress")
@patch("ddpui.core.trial.tasks.clone_template_org")
def test_clone_task_does_not_notify_biz_dev_on_failure(
    mock_clone, mock_taskprogress_cls, mock_biz_dev
):
    """A failed clone is torn down — there is no org to announce."""
    from ddpui.core.trial.tasks import clone_trial_org_task

    mock_taskprogress_cls.return_value = MagicMock()
    mock_clone.side_effect = RuntimeError("boom")

    clone_trial_org_task("task-nomail", 5, "a@b.org", "Acme", "monitoring_evaluation")

    mock_biz_dev.send_notification.assert_not_called()
