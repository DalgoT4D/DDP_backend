"""Alert delivery — email (SES) + Slack webhook POST.

Each function returns one or more "delivery dicts" — the same JSON shape
that lands in AlertLog.deliveries. Failures are captured (not raised) so the
evaluator can record per-recipient outcomes without aborting the loop.

Delivery dict shape (matches AlertLog.deliveries entries):
    {
      "channel": "email" | "slack",
      "target":  "<email>" | "slack:webhook",
      "status":  "sent" | "failed",
      "error_reason": "<smtp error or http body>" | None,
      "http_status": <int> | None,
      "sent_at": "<UTC ISO 8601>"
    }
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Optional

import requests

from ddpui.models.alert import Alert
from ddpui.models.org_user import OrgUser
from ddpui.utils import awsses
from ddpui.utils.custom_logger import CustomLogger


logger = CustomLogger("ddpui.alerts.delivery")


SLACK_TARGET = "slack:webhook"
DEFAULT_HTTP_TIMEOUT = 10


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_recipient_email(recipient: dict, orguser_email_by_id: dict[int, str]) -> Optional[str]:
    """Map a stored recipient entry to an email address.

    OrgUser recipients that no longer exist in the org are silently skipped
    (returns None) — the evaluator records this as a "failed" delivery.
    """
    if recipient.get("type") == "external":
        return recipient.get("email")
    if recipient.get("type") == "orguser":
        return orguser_email_by_id.get(recipient.get("orguser_id"))
    return None


def deliver_email(
    *,
    to_email: str,
    subject: str,
    body: str,
) -> dict:
    """Send a plain-text email via SES, returning a delivery dict."""
    try:
        awsses.send_text_message(to_email, subject, body)
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


def deliver_slack(*, webhook_url: str, body: str) -> dict:
    """POST a Slack-compatible payload to a webhook URL, returning a delivery dict."""
    payload = {"text": body}
    try:
        response = requests.post(webhook_url, json=payload, timeout=DEFAULT_HTTP_TIMEOUT)
        ok = 200 <= response.status_code < 300
        return {
            "channel": "slack",
            "target": SLACK_TARGET,
            "status": "sent" if ok else "failed",
            "error_reason": None if ok else (response.text or response.reason or "")[:500],
            "http_status": response.status_code,
            "sent_at": _now_iso(),
        }
    except requests.RequestException as e:
        logger.error(f"Slack webhook delivery failed: {e}")
        return {
            "channel": "slack",
            "target": SLACK_TARGET,
            "status": "failed",
            "error_reason": str(e)[:500],
            "http_status": 0,
            "sent_at": _now_iso(),
        }


def deliver_all(alert: Alert, *, subject: str, body: str) -> list[dict]:
    """Run the full delivery loop for a fired alert.

    Looks at `alert.delivery_channels` and `alert.recipients` to decide what
    to send. Returns the list of delivery dicts in the order they were
    attempted (email recipients in stored order, then Slack if enabled).
    """
    deliveries: list[dict] = []
    channels = alert.delivery_channels or []

    if "email" in channels:
        recipients = alert.recipients or []
        orguser_ids = [r["orguser_id"] for r in recipients if r.get("type") == "orguser"]
        orguser_email_by_id: dict[int, str] = {}
        if orguser_ids:
            for ou in OrgUser.objects.filter(
                id__in=orguser_ids, org_id=alert.org_id
            ).select_related("user"):
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
            deliveries.append(deliver_email(to_email=email, subject=subject, body=body))

    if "slack" in channels and alert.slack_webhook_url:
        deliveries.append(deliver_slack(webhook_url=alert.slack_webhook_url, body=body))

    return deliveries


def _describe_missing_recipient(r: dict) -> str:
    """Best-effort label so the log row tells the operator who couldn't be reached."""
    if r.get("type") == "external":
        return r.get("email") or "external:unknown"
    if r.get("type") == "orguser":
        return f"orguser:{r.get('orguser_id')}"
    return "unknown"


def summarize(deliveries: Iterable[dict]) -> str:
    """Return 'success' / 'partial' / 'failed' / 'not_attempted' from a delivery list.

    Not stored — derived for callers that want a quick label.
    """
    items = list(deliveries)
    if not items:
        return "not_attempted"
    states = {d.get("status") for d in items}
    if states == {"sent"}:
        return "success"
    if "sent" in states and "failed" in states:
        return "partial"
    return "failed"
