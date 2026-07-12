"""Tests for the Celery dispatcher + evaluator in ddpui.celeryworkers.alert_tasks.

Warehouse SQL execution is stubbed via monkey-patching
`ddpui.celeryworkers.alert_tasks.alert_query.compute` so these tests run
without a real warehouse connection. Delivery is also stubbed so we don't
hit SES or Slack.
"""

import os
import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.utils import timezone

from ddpui.api.alert_api import create_alert
from ddpui.celeryworkers import alert_tasks
from ddpui.models.alert import Alert, AlertLog
from ddpui.models.org import OrgWarehouse
from ddpui.models.user_group import UserGroupMember, UserGroupMemberStatus
from ddpui.schemas.alert_schema import (
    AlertCreate,
    RecipientIn,
    ThresholdCondition,
)
from ddpui.tests.api_tests.test_alert_api import (  # reuse fixtures
    analyst_orguser,
    authuser,
    org,
    orguser,
    sample_group,
    sample_metric,
    sample_kpi,
    seed_db,
)
from ddpui.tests.api_tests.test_user_org_api import mock_request


pytestmark = pytest.mark.django_db


# ── Helpers ─────────────────────────────────────────────────────────────────


def _base_alert(orguser, *, metric_id=None, cron="* * * * *"):
    return AlertCreate(
        name=f"alert {timezone.now().timestamp()}",
        alert_type="metric_threshold",
        metric_id=metric_id,
        condition=ThresholdCondition(operator="lt", value=100),
        schedule_cron=cron,
        delivery_channels=["email"],
        message_template="{{alert_name}} fired with {{current_value}}",
        recipients=[RecipientIn(type="external", email="ops@example.com")],
    )


def _patch_query(monkeypatch, value, rag_status=None, sql="SELECT 1"):
    monkeypatch.setattr(
        alert_tasks.alert_query,
        "compute",
        lambda alert, ow: (value, sql, rag_status),
    )


def _stub_warehouse(orguser):
    """Ensure there's an OrgWarehouse so the evaluator doesn't short-circuit."""
    OrgWarehouse.objects.get_or_create(org=orguser.org, defaults={"wtype": "postgres"})


def _stub_delivery(monkeypatch):
    monkeypatch.setattr(
        alert_tasks.delivery_helpers,
        "deliver_all",
        lambda alert, subject, body: [
            {
                "channel": "email",
                "target": r.get("email", f"orguser:{r.get('orguser_id')}"),
                "status": "sent",
                "error_reason": None,
                "http_status": None,
                "sent_at": "2026-06-11T00:00:00Z",
            }
            for r in (alert.recipients or [])
        ],
    )


# ── evaluate_alert ──────────────────────────────────────────────────────────


def test_evaluate_alert_writes_log_and_fires(monkeypatch, seed_db, orguser, sample_metric):
    """Threshold met → fired=True, deliveries populated, AlertLog written."""
    _stub_warehouse(orguser)
    _patch_query(monkeypatch, value=42.0)
    _stub_delivery(monkeypatch)

    created = create_alert(mock_request(orguser), _base_alert(orguser, metric_id=sample_metric.id))

    did_work = alert_tasks.evaluate_alert(created.id)
    assert did_work is True

    log = AlertLog.objects.filter(alert_id=created.id).first()
    assert log is not None
    assert log.fired is True
    assert log.value == 42.0
    assert log.sql_executed == "SELECT 1"
    assert "42" in log.message
    assert len(log.deliveries) == 1
    assert log.deliveries[0]["status"] == "sent"


def test_evaluate_alert_does_not_fire_when_condition_unmet(
    monkeypatch, seed_db, orguser, sample_metric
):
    """value above threshold → fired=False, no deliveries, AlertLog still written."""
    _stub_warehouse(orguser)
    _patch_query(monkeypatch, value=999.0)
    _stub_delivery(monkeypatch)

    created = create_alert(mock_request(orguser), _base_alert(orguser, metric_id=sample_metric.id))

    alert_tasks.evaluate_alert(created.id)

    log = AlertLog.objects.filter(alert_id=created.id).first()
    assert log.fired is False
    assert log.deliveries == []
    # Message is rendered even on non-fire so the log shows what would've gone
    assert "999" in log.message


def test_evaluate_alert_idempotent_atomic_claim(monkeypatch, seed_db, orguser, sample_metric):
    """A second call within the same cron tick must NOT double-write."""
    _stub_warehouse(orguser)
    _patch_query(monkeypatch, value=42.0)
    _stub_delivery(monkeypatch)

    created = create_alert(mock_request(orguser), _base_alert(orguser, metric_id=sample_metric.id))

    did_1 = alert_tasks.evaluate_alert(created.id)
    did_2 = alert_tasks.evaluate_alert(created.id)
    assert did_1 is True
    assert did_2 is False
    assert AlertLog.objects.filter(alert_id=created.id).count() == 1


def test_evaluate_alert_skips_disabled(monkeypatch, seed_db, orguser, sample_metric):
    _stub_warehouse(orguser)
    _patch_query(monkeypatch, value=42.0)
    _stub_delivery(monkeypatch)

    created = create_alert(mock_request(orguser), _base_alert(orguser, metric_id=sample_metric.id))
    Alert.objects.filter(id=created.id).update(is_active=False)

    did = alert_tasks.evaluate_alert(created.id)
    assert did is False
    assert AlertLog.objects.filter(alert_id=created.id).count() == 0


def test_evaluate_alert_records_warehouse_error_without_crashing(
    monkeypatch, seed_db, orguser, sample_metric
):
    """If the warehouse query raises, the log row records the error and fired=False."""
    _stub_warehouse(orguser)

    def boom(alert, ow):
        raise RuntimeError("warehouse down")

    monkeypatch.setattr(alert_tasks.alert_query, "compute", boom)

    created = create_alert(mock_request(orguser), _base_alert(orguser, metric_id=sample_metric.id))
    alert_tasks.evaluate_alert(created.id)

    log = AlertLog.objects.filter(alert_id=created.id).first()
    assert log.fired is False
    assert "warehouse down" in log.alert_snapshot.get("error", "")


# ── group recipient expansion at fire time ──────────────────────────────────
#
# These tests exercise the REAL delivery.deliver_all/_expand_recipients path
# (only the SES send call is stubbed) so the expansion + dedupe logic itself
# is pinned, not just the evaluator's plumbing around a stubbed delivery.


def _stub_ses(monkeypatch):
    """Stub only the outbound SES call — deliver_all's expansion/dedupe/loop
    logic still runs for real."""
    monkeypatch.setattr(
        alert_tasks.delivery_helpers.awsses,
        "send_text_message",
        lambda to, subject, body: {"MessageId": "stub"},
    )


def _alert_with_recipients(orguser, metric_id, recipients):
    return AlertCreate(
        name=f"alert {timezone.now().timestamp()}",
        alert_type="metric_threshold",
        metric_id=metric_id,
        condition=ThresholdCondition(operator="lt", value=100),
        schedule_cron="* * * * *",
        delivery_channels=["email"],
        message_template="{{alert_name}} fired with {{current_value}}",
        recipients=recipients,
    )


def test_fire_dedupes_direct_recipient_also_in_group(
    monkeypatch, seed_db, orguser, analyst_orguser, sample_metric, sample_group
):
    """Group has 2 active members (orguser, analyst_orguser); orguser is ALSO
    listed as a direct recipient. Expect exactly 2 unique deliveries, not 3."""
    _stub_warehouse(orguser)
    _patch_query(monkeypatch, value=42.0)
    _stub_ses(monkeypatch)

    UserGroupMember.objects.create(
        group=sample_group, orguser=orguser, status=UserGroupMemberStatus.ACTIVE
    )
    UserGroupMember.objects.create(
        group=sample_group, orguser=analyst_orguser, status=UserGroupMemberStatus.ACTIVE
    )

    payload = _alert_with_recipients(
        orguser,
        sample_metric.id,
        [
            RecipientIn(type="orguser", orguser_id=orguser.id),
            RecipientIn(type="group", group_id=sample_group.id),
        ],
    )
    created = create_alert(mock_request(orguser), payload)

    alert_tasks.evaluate_alert(created.id)

    log = AlertLog.objects.filter(alert_id=created.id).first()
    assert log.fired is True
    assert len(log.deliveries) == 2
    targets = {d["target"] for d in log.deliveries}
    assert targets == {orguser.user.email, analyst_orguser.user.email}
    assert all(d["status"] == "sent" for d in log.deliveries)


def test_fire_skips_pending_group_member(
    monkeypatch, seed_db, orguser, sample_metric, sample_group
):
    """A pending (email-only, no OrgUser) group member is never a delivery
    target — only the active OrgUser member resolves."""
    _stub_warehouse(orguser)
    _patch_query(monkeypatch, value=42.0)
    _stub_ses(monkeypatch)

    UserGroupMember.objects.create(
        group=sample_group, orguser=orguser, status=UserGroupMemberStatus.ACTIVE
    )
    UserGroupMember.objects.create(
        group=sample_group, pending_email="invited@example.com", status="pending"
    )

    payload = _alert_with_recipients(
        orguser, sample_metric.id, [RecipientIn(type="group", group_id=sample_group.id)]
    )
    created = create_alert(mock_request(orguser), payload)

    alert_tasks.evaluate_alert(created.id)

    log = AlertLog.objects.filter(alert_id=created.id).first()
    assert log.fired is True
    assert len(log.deliveries) == 1
    assert log.deliveries[0]["target"] == orguser.user.email


def test_fire_skips_all_pending_group_with_no_active_members(
    monkeypatch, seed_db, orguser, sample_metric, sample_group
):
    """A group whose only member(s) are pending (email-only, no OrgUser) has
    zero active members at fire time — treated the same as an empty group:
    skipped gracefully, contributing zero deliveries, while a direct
    recipient still gets theirs."""
    _stub_warehouse(orguser)
    _patch_query(monkeypatch, value=42.0)
    _stub_ses(monkeypatch)

    UserGroupMember.objects.create(
        group=sample_group, pending_email="invited@example.com", status="pending"
    )

    payload = _alert_with_recipients(
        orguser,
        sample_metric.id,
        [
            RecipientIn(type="external", email="ops@example.com"),
            RecipientIn(type="group", group_id=sample_group.id),
        ],
    )
    created = create_alert(mock_request(orguser), payload)

    did_work = alert_tasks.evaluate_alert(created.id)
    assert did_work is True

    log = AlertLog.objects.filter(alert_id=created.id).first()
    assert log.fired is True
    assert len(log.deliveries) == 1
    assert log.deliveries[0]["target"] == "ops@example.com"


def test_fire_gracefully_skips_deleted_group(
    monkeypatch, seed_db, orguser, sample_metric, sample_group
):
    """A group referenced by an alert can be deleted after the alert is
    created (recipients are only validated at create/update time). At fire
    time the now-dangling group_id must not crash the run — it's skipped,
    and delivery still proceeds to direct recipients."""
    _stub_warehouse(orguser)
    _patch_query(monkeypatch, value=42.0)
    _stub_ses(monkeypatch)

    payload = _alert_with_recipients(
        orguser,
        sample_metric.id,
        [
            RecipientIn(type="external", email="ops@example.com"),
            RecipientIn(type="group", group_id=sample_group.id),
        ],
    )
    created = create_alert(mock_request(orguser), payload)
    # Queryset delete (not the instance's .delete()) so `sample_group`'s pk
    # attribute survives for the fixture's own teardown to no-op cleanly.
    UserGroupMember.objects.filter(group=sample_group).delete()
    type(sample_group).objects.filter(id=sample_group.id).delete()

    did_work = alert_tasks.evaluate_alert(created.id)
    assert did_work is True

    log = AlertLog.objects.filter(alert_id=created.id).first()
    assert log.fired is True
    assert len(log.deliveries) == 1
    assert log.deliveries[0]["target"] == "ops@example.com"
    assert log.deliveries[0]["status"] == "sent"


# ── trigger context in the notification ──────────────────────────────────────


def test_fired_message_carries_alert_id_and_link(monkeypatch, seed_db, orguser, sample_metric):
    """The rendered message (what's actually delivered, and what lands in the
    audit log) always carries the alert id + a deep link back to it, on top
    of whatever metric/value tokens the author's template references."""
    _stub_warehouse(orguser)
    _patch_query(monkeypatch, value=42.0)
    _stub_ses(monkeypatch)

    created = create_alert(mock_request(orguser), _base_alert(orguser, metric_id=sample_metric.id))
    alert_tasks.evaluate_alert(created.id)

    log = AlertLog.objects.filter(alert_id=created.id).first()
    # Trigger context tokens (name via subject/snapshot, current value via template)
    assert "42" in log.message
    assert log.alert_snapshot["name"] == created.name
    # Alert identity + click-through link for the request-access flow
    assert f"#{created.id}" in log.message
    assert f"alertId={created.id}" in log.message


# ── dispatch_due_alerts ─────────────────────────────────────────────────────


def test_dispatch_due_alerts_enqueues_active_due_alerts(
    monkeypatch, seed_db, orguser, sample_metric
):
    enqueued: list[int] = []
    monkeypatch.setattr(
        alert_tasks.evaluate_alert,
        "delay",
        lambda alert_id: enqueued.append(alert_id),
    )

    a1 = create_alert(mock_request(orguser), _base_alert(orguser, metric_id=sample_metric.id))
    a2 = create_alert(
        mock_request(orguser),
        _base_alert(orguser, metric_id=sample_metric.id, cron="* * * * *"),
    )
    # is_due gates on created_at when last_evaluated_at is NULL — backdate so a
    # scheduled tick has occurred since creation and the dispatcher picks them up.
    past = timezone.now() - timezone.timedelta(minutes=5)
    Alert.objects.filter(id__in=[a1.id, a2.id]).update(created_at=past)

    n = alert_tasks.dispatch_due_alerts()
    assert n == 2
    assert len(enqueued) == 2


def test_dispatch_due_alerts_skips_inactive(monkeypatch, seed_db, orguser, sample_metric):
    enqueued: list[int] = []
    monkeypatch.setattr(
        alert_tasks.evaluate_alert,
        "delay",
        lambda alert_id: enqueued.append(alert_id),
    )

    created = create_alert(mock_request(orguser), _base_alert(orguser, metric_id=sample_metric.id))
    Alert.objects.filter(id=created.id).update(is_active=False)

    n = alert_tasks.dispatch_due_alerts()
    assert n == 0
    assert enqueued == []
