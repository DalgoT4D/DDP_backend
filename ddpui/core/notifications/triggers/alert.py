"""Alert-fired notification trigger.

Owns the email fan-out for an alert fire — every recipient (orguser + external)
gets the specialized ``render_alert_email`` HTML via SES. Delivery dicts are
returned in stored recipient order — same shape used by ``AlertLog.deliveries``.

By design, alert firings do **not** create in-app bell rows: the user explicitly
picks the delivery channel (email or Slack) when creating the alert, and honoring
that choice is the contract. Slack delivery stays in ``core/alerts/delivery.py``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from ddpui.core.notifications.templates import render_alert_email
from ddpui.models.alert import Alert
from ddpui.models.org_user import OrgUser
from ddpui.utils import awsses
from ddpui.utils.custom_logger import CustomLogger


logger = CustomLogger("ddpui.notifications.triggers.alert")


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_recipient_email(recipient: dict, orguser_email_by_id: dict) -> Optional[str]:
    """Map a stored recipient entry to an email address.

    OrgUser recipients that no longer exist in the org are silently skipped
    (returns None) — the caller records this as a "failed" delivery.
    """
    if recipient.get("type") == "external":
        return recipient.get("email")
    if recipient.get("type") == "orguser":
        return orguser_email_by_id.get(recipient.get("orguser_id"))
    return None


def _describe_missing_recipient(r: dict) -> str:
    """Best-effort label so the log row tells the operator who couldn't be reached."""
    if r.get("type") == "external":
        return r.get("email") or "external:unknown"
    if r.get("type") == "orguser":
        return f"orguser:{r.get('orguser_id')}"
    return "unknown"


def _deliver_email(*, to_email: str, subject: str, plain_body: str, html_body: str) -> dict:
    """Send a multipart HTML+plain email via SES, returning a delivery dict."""
    try:
        awsses.send_html_message(to_email, subject, plain_body, html_body)
        return {
            "channel": "email",
            "target": to_email,
            "status": "sent",
            "error_reason": None,
            "http_status": None,
            "sent_at": _now_iso(),
        }
    except Exception as e:  # SES exceptions are too varied to enumerate
        logger.error(f"SES delivery to {to_email} failed: {e}")
        return {
            "channel": "email",
            "target": to_email,
            "status": "failed",
            "error_reason": str(e),
            "http_status": None,
            "sent_at": _now_iso(),
        }


def notify_alert_recipients(alert: Alert, *, subject: str, body: str) -> list:
    """Dispatch the email leg of an alert to its recipient list.

    Returns a list of email-delivery dicts, one per recipient, in stored
    order. Callers append this to any other channel dicts (e.g. Slack) they
    generate separately.
    """
    deliveries: list = []

    plain_body, html_body = render_alert_email(alert, body)

    recipients = alert.recipients or []
    orguser_ids = [r["orguser_id"] for r in recipients if r.get("type") == "orguser"]
    orguser_email_by_id: dict = {}
    if orguser_ids:
        for ou in OrgUser.objects.filter(id__in=orguser_ids, org_id=alert.org_id).select_related(
            "user"
        ):
            orguser_email_by_id[ou.id] = ou.user.email

    for r in recipients:
        email = _resolve_recipient_email(r, orguser_email_by_id)
        if not email:
            deliveries.append(
                {
                    "channel": "email",
                    "target": _describe_missing_recipient(r),
                    "status": "failed",
                    "error_reason": "recipient could not be resolved",
                    "http_status": None,
                    "sent_at": _now_iso(),
                }
            )
            continue
        deliveries.append(
            _deliver_email(
                to_email=email,
                subject=subject,
                plain_body=plain_body,
                html_body=html_body,
            )
        )

    return deliveries
