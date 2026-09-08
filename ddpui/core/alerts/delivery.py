"""Alert delivery orchestration — email (via notifications trigger) + Slack.

The email fan-out (and the new in-app + Discord write for orguser recipients)
lives in ``core.notifications.triggers.alert``. This module keeps only the
Slack webhook path and the top-level ``deliver_all`` orchestrator, since
Slack isn't part of the notifications pipeline (no in-app row, no user
preference gating).

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
from typing import Iterable

import requests

from ddpui.core.notifications.triggers.alert import notify_alert_recipients
from ddpui.models.alert import Alert
from ddpui.utils.custom_logger import CustomLogger


logger = CustomLogger("ddpui.alerts.delivery")


SLACK_TARGET = "slack:webhook"
DEFAULT_HTTP_TIMEOUT = 10


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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

    Looks at ``alert.delivery_channels`` and ``alert.recipients`` to decide what
    to send. Returns the list of delivery dicts in the order they were
    attempted (email recipients in stored order, then Slack if enabled).
    """
    deliveries: list[dict] = []
    channels = alert.delivery_channels or []

    if "email" in channels:
        deliveries.extend(notify_alert_recipients(alert, subject=subject, body=body))

    # Slack keeps the raw user body — no HTML shell on webhook posts.
    if "slack" in channels and alert.slack_webhook_url:
        deliveries.append(deliver_slack(webhook_url=alert.slack_webhook_url, body=body))

    return deliveries


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
