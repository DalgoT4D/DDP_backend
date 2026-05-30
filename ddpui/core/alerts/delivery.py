"""Alert notification delivery helpers."""

from ddpui.models.alert import Alert
from ddpui.utils.awsses import send_text_message
from ddpui.utils.custom_logger import CustomLogger


logger = CustomLogger("ddpui.alert_delivery")


def send_alert_emails(alert: Alert, rendered_message: str):
    """Send the rendered alert notification to all recipients."""
    for recipient in alert.recipients:
        try:
            send_text_message(
                to_email=recipient,
                subject=f"Dalgo Alert: {alert.name}",
                message=rendered_message or alert.message,
            )
        except Exception as err:
            logger.error(
                "Failed to send alert email to %s for alert %s: %s",
                recipient,
                alert.id,
                err,
            )
