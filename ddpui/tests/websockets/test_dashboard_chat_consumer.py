import json
from unittest.mock import Mock, patch

import pytest

from ddpui.websockets.dashboard_chat_consumer import DashboardChatConsumer


def test_dashboard_chat_consumer_send_message_requires_message():
    consumer = DashboardChatConsumer()
    consumer.send = Mock()
    consumer.dashboard = Mock(id=42)
    consumer.websocket_receive({"text": json.dumps({"action": "send_message"})})

    payload = json.loads(consumer.send.call_args.kwargs["text_data"])
    assert payload["event_type"] == "error"
    assert payload["data"]["message"] == "Message is required"


def test_dashboard_chat_consumer_send_message_requires_available_chat():
    consumer = DashboardChatConsumer()
    consumer.send = Mock()
    consumer.dashboard = Mock(id=42)
    consumer._chat_available = Mock(return_value=(False, "Chat unavailable"))
    consumer.websocket_receive(
        {
            "text": json.dumps(
                {
                    "action": "send_message",
                    "message": "Why did funding drop?",
                }
            )
        }
    )

    payload = json.loads(consumer.send.call_args.kwargs["text_data"])
    assert payload["event_type"] == "error"
    assert payload["data"]["message"] == "Chat unavailable"


@patch("ddpui.websockets.dashboard_chat_consumer.publish_dashboard_chat_event")
@patch("ddpui.websockets.dashboard_chat_consumer.run_dashboard_chat_turn.delay")
@patch("ddpui.websockets.dashboard_chat_consumer.create_dashboard_chat_user_message")
@patch("ddpui.websockets.dashboard_chat_consumer.get_or_create_dashboard_chat_session")
def test_dashboard_chat_consumer_send_message_creates_session_and_dispatches_task(
    mock_get_or_create_session,
    mock_create_user_message,
    mock_delay,
    mock_publish_event,
):
    session = Mock(session_id="session-123")
    user_message = Mock(id=17)
    mock_get_or_create_session.return_value = session
    mock_create_user_message.return_value = user_message

    consumer = DashboardChatConsumer()
    consumer.dashboard = Mock(id=42)
    consumer.orguser = Mock()
    consumer._chat_available = Mock(return_value=(True, ""))
    consumer._subscribe_to_session = Mock()

    consumer.websocket_receive(
        {
            "text": json.dumps(
                {
                    "action": "send_message",
                    "message": "Why did funding drop?",
                    "client_message_id": "ui-1",
                }
            )
        }
    )

    mock_get_or_create_session.assert_called_once()
    mock_create_user_message.assert_called_once()
    consumer._subscribe_to_session.assert_called_once_with("session-123")
    mock_publish_event.assert_called_once()
    mock_delay.assert_called_once_with("session-123", 17)
