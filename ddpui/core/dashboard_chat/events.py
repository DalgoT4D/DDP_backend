"""Websocket event helpers for dashboard chat."""

import json

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.utils import timezone

from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


def dashboard_chat_group_name(session_id: str) -> str:
    """Return the channel-layer group name for a dashboard chat session."""
    return f"dashboard_chat_{session_id}"


def build_dashboard_chat_event(
    *,
    event_type: str,
    dashboard_id: int,
    data: dict,
    session_id: str | None = None,
    message_id: str | None = None,
) -> dict:
    """Build a dashboard chat websocket event envelope."""
    event = {
        "event_type": event_type,
        "dashboard_id": dashboard_id,
        "occurred_at": timezone.now().isoformat(),
        "data": data,
    }
    if session_id is not None:
        event["session_id"] = session_id
    if message_id is not None:
        event["message_id"] = message_id
    return event


def publish_dashboard_chat_event(session_id: str, event: dict) -> None:
    """Publish a dashboard chat event to the session channel-layer group."""
    channel_layer = get_channel_layer()
    if channel_layer is None:
        return
    try:
        async_to_sync(channel_layer.group_send)(
            dashboard_chat_group_name(session_id),
            {
                "type": "dashboard_chat_event",
                "event": json.dumps(event),
            },
        )
    except Exception:
        logger.exception(
            "failed to publish dashboard chat event for session=%s event_type=%s",
            session_id,
            event.get("event_type"),
        )
