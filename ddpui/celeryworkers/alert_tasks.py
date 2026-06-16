"""Celery tasks for the schedule-driven alert evaluation engine.

Beat fires `dispatch_due_alerts` every 60 seconds. The dispatcher iterates
active alerts, asks `scheduling.is_due()` whether the most-recent cron tick
has been claimed yet, and if not enqueues `evaluate_alert.delay(alert_id)`.

The evaluator does an atomic CAS via a conditional UPDATE on
`last_evaluated_at` — the first worker to land the UPDATE owns the tick.
After winning the claim it builds & executes the SQL, evaluates the
condition, renders the template, delivers via SES + Slack, and writes one
AlertLog row. Crashes between claim and delivery lose the fire for that tick
but next tick proceeds normally (acceptable at daily/weekly/monthly cadence).
"""

from __future__ import annotations

from datetime import datetime

from django.db.models import Q
from django.utils import timezone

from ddpui.celery import app
from ddpui.core.alerts import (
    alert_query,
    condition as condition_helpers,
    delivery as delivery_helpers,
    rendering,
    scheduling,
)
from ddpui.models.alert import Alert, AlertLog, AlertType
from ddpui.models.org import OrgWarehouse
from ddpui.utils.custom_logger import CustomLogger


logger = CustomLogger("ddpui.alerts.tasks")


@app.task(name="alerts.dispatch_due_alerts")
def dispatch_due_alerts() -> int:
    """Beat-fired entry point. Enqueue evaluators for every due alert.

    Returns the number of evaluators enqueued — useful in tests and logs.
    """
    enqueued = 0
    now = timezone.now()
    qs = Alert.objects.filter(is_active=True).only("id", "schedule_cron", "last_evaluated_at")
    for alert in qs:
        try:
            if scheduling.is_due(alert, now):
                evaluate_alert.delay(alert.id)
                enqueued += 1
        except Exception as e:  # pragma: no cover — defensive only
            logger.error(f"dispatcher skipped alert {alert.id}: {e}")
    logger.info(f"alert dispatcher enqueued {enqueued} of {qs.count()} active alerts")
    return enqueued


@app.task(name="alerts.evaluate_alert", bind=True, max_retries=0)
def evaluate_alert(self, alert_id: int) -> bool:
    """Atomic claim → query → evaluate → render → deliver → write AlertLog.

    Returns whether this worker actually performed work — False if the tick
    was already claimed by another worker (idempotent), True otherwise.
    """
    now = timezone.now()

    alert = Alert.objects.filter(id=alert_id, is_active=True).first()
    if not alert:
        logger.info(f"evaluate_alert: alert {alert_id} inactive or deleted, exit")
        return False

    try:
        scheduled_for = scheduling.previous_fire(alert.schedule_cron, now)
    except Exception as e:
        logger.error(f"evaluate_alert {alert_id}: cron parse failed: {e}")
        return False

    # Atomic claim — Postgres serializes; only one worker matches the WHERE
    rows_updated = (
        Alert.objects.filter(id=alert_id, is_active=True)
        .filter(Q(last_evaluated_at__isnull=True) | Q(last_evaluated_at__lt=scheduled_for))
        .update(last_evaluated_at=now)
    )
    if rows_updated == 0:
        logger.info(f"evaluate_alert {alert_id}: scheduled tick already claimed, exit")
        return False

    # Reload with related Metric / KPI / Org
    alert = Alert.objects.select_related("metric", "kpi", "org").filter(id=alert_id).first()
    if not alert:
        return False

    _run_evaluation(alert, scheduled_for=scheduled_for, evaluated_at=now)
    return True


def _run_evaluation(alert: Alert, *, scheduled_for: datetime, evaluated_at: datetime) -> None:
    """Core evaluation flow — extracted so it's reusable in tests."""
    org_warehouse = OrgWarehouse.objects.filter(org=alert.org).first()
    sql_str = ""
    value = None
    rag_status = None
    error: str | None = None

    if not org_warehouse:
        error = "No warehouse configured for org"
    else:
        try:
            value, sql_str, rag_status = alert_query.compute(alert, org_warehouse)
        except Exception as e:
            logger.error(f"evaluate_alert {alert.id}: warehouse query failed: {e}")
            error = str(e)

    # Evaluate condition
    cond = alert.condition or {}
    if error:
        fired = False
    else:
        eval_input = rag_status if alert.alert_type == AlertType.KPI_RAG else value
        fired = condition_helpers.evaluate(alert.alert_type, cond, eval_input)

    # Render message (rendered on every evaluation — also useful for non-fire audit)
    tokens = rendering.tokens_for_alert(alert, current_value=value, rag_status=rag_status)
    body = rendering.render(alert.message_template, tokens)
    subject = f"[Dalgo alert] {alert.name}"

    # Deliver if fired and not errored
    deliveries: list[dict] = []
    if fired:
        deliveries = delivery_helpers.deliver_all(alert, subject=subject, body=body)

    # Snapshot — preserves audit-relevant config at evaluation time
    snapshot = {
        "name": alert.name,
        "alert_type": alert.alert_type,
        "metric_id": alert.metric_id,
        "kpi_id": alert.kpi_id,
        "condition": cond,
        "recipients": list(alert.recipients or []),
        "message_template": alert.message_template,
        # rag_status is only meaningful for KPI alerts; None otherwise. Stored
        # here (not on the row) so the log modal can paint the colored dot
        # without re-querying KPI thresholds, which may drift over time.
        "rag_status": rag_status,
    }
    if error:
        snapshot["error"] = error

    AlertLog.objects.create(
        alert=alert,
        scheduled_for=scheduled_for,
        evaluated_at=evaluated_at,
        value=value,
        fired=fired,
        alert_snapshot=snapshot,
        sql_executed=sql_str or "",
        message=body or "",
        deliveries=deliveries,
    )
    logger.info(
        f"evaluate_alert {alert.id}: fired={fired} value={value} deliveries={len(deliveries)}"
    )


# Re-export for tests that need direct access to the inner flow
__all__ = ["dispatch_due_alerts", "evaluate_alert", "_run_evaluation"]
