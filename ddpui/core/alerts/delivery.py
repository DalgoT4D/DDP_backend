"""Alert notification delivery helpers."""

from typing import Optional

from ddpui.models.alert import Alert
from ddpui.utils.awsses import send_text_message
from ddpui.utils.custom_logger import CustomLogger


logger = CustomLogger("ddpui.alert_delivery")


def send_alert_emails(
    alert: Alert,
    rendered_message: str,
    recipients: Optional[list[str]] = None,
):
    """Send the rendered alert notification to each email recipient.

    `recipients` is a flat list of email strings. If omitted, falls back to
    the alert's stored recipients list (legacy call sites) — callers should
    prefer passing the pre-resolved list from `resolve_recipient_emails`.
    """
    if recipients is None:
        recipients = []
        for r in alert.recipients or []:
            if isinstance(r, str):
                recipients.append(r)
            elif isinstance(r, dict):
                # Typed dict: best-effort email extraction (user-type refs
                # require a DB lookup which is the service's responsibility).
                if r.get("type") == "email":
                    recipients.append(r.get("ref", ""))

    for email in recipients:
        if not email:
            continue
        try:
            send_text_message(
                to_email=email,
                subject=f"Dalgo Alert: {alert.name}",
                message=rendered_message or alert.message,
            )
        except Exception as err:
            logger.error(
                "Failed to send alert email to %s for alert %s: %s",
                email,
                alert.id,
                err,
            )
