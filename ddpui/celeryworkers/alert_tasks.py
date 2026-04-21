"""Celery helpers for alert delivery."""

from ddpui.celery import app
from ddpui.models.alert import Alert
from ddpui.core.alerts.delivery import send_alert_emails
from ddpui.utils.custom_logger import CustomLogger


logger = CustomLogger("ddpui.alert_tasks")


@app.task(bind=False)
def deliver_alert_emails(alert_id: int, rendered_message: str = ""):
    """Async wrapper for sending alert notifications when needed."""
    try:
        alert = Alert.objects.get(id=alert_id)
    except Alert.DoesNotExist:
        logger.error("Alert %s does not exist, skipping email delivery", alert_id)
        return

    send_alert_emails(alert, rendered_message)
