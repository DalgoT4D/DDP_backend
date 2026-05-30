"""Celery tasks for alert evaluation and email delivery"""

from datetime import datetime, timezone

from croniter import croniter

from ddpui.celery import app
from ddpui.models.alert import Alert
from ddpui.core.alerts.alert_service import AlertService
from ddpui.utils.awsses import send_text_message
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.alert_tasks")


@app.task(bind=False)
def check_and_fire_alerts():
    """
    Runs every 5 minutes via celery beat.
    Checks each active alert's cron expression against current time.
    If cron matches (current time falls within the 5-min window),
    evaluate the alert and send email if fired.
    """
    now = datetime.now(timezone.utc)
    active_alerts = Alert.objects.filter(is_active=True)
    alert_count = active_alerts.count()

    logger.info(
        f"check_and_fire_alerts running at {now.isoformat()} — "
        f"{alert_count} active alert(s) to check"
    )

    matched = 0
    fired_count = 0

    for alert in active_alerts:
        try:
            if not _cron_matches_now(alert.cron, now):
                continue

            matched += 1
            logger.info(f"Alert {alert.id} ({alert.name}) cron matched, evaluating")
            fired, rows_count = AlertService.evaluate_alert(alert)

            if fired:
                fired_count += 1
                logger.info(f"Alert {alert.id} fired with {rows_count} rows, sending emails")
                _send_alert_emails(alert)
            else:
                logger.info(f"Alert {alert.id} did not fire (0 rows matched)")

        except Exception as e:
            logger.error(f"Failed to process alert {alert.id}: {e}")

    logger.info(
        f"check_and_fire_alerts completed — "
        f"checked: {alert_count}, matched cron: {matched}, fired: {fired_count}"
    )


def _cron_matches_now(cron_expr: str, now: datetime) -> bool:
    """
    Check if the cron expression matches the current 5-minute window.
    Uses croniter to get the previous scheduled time and checks
    if it falls within the last 5 minutes (300 seconds).
    """
    cron = croniter(cron_expr, now)
    prev_fire = cron.get_prev(datetime)
    diff = (now - prev_fire).total_seconds()
    return diff < 300


def _send_alert_emails(alert: Alert):
    """Send alert email to all recipients"""
    for recipient in alert.recipients:
        try:
            send_text_message(
                to_email=recipient,
                subject=f"Dalgo Alert: {alert.name}",
                message=alert.message,
            )
        except Exception as e:
            logger.error(f"Failed to send alert email to {recipient} for alert {alert.id}: {e}")
