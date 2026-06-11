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
from ddpui.schemas.alert_schema import (
    AlertCreate,
    RecipientIn,
    ThresholdCondition,
)
from ddpui.tests.api_tests.test_alert_api import (  # reuse fixtures
    authuser,
    org,
    orguser,
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

    create_alert(mock_request(orguser), _base_alert(orguser, metric_id=sample_metric.id))
    create_alert(
        mock_request(orguser),
        _base_alert(orguser, metric_id=sample_metric.id, cron="* * * * *"),
    )

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
