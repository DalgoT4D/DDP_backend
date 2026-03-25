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
    assert payload["status"] == "error"
    assert payload["message"] == "Message is required"


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
    assert payload["status"] == "error"
    assert payload["message"] == "Chat unavailable"


@patch("ddpui.websockets.dashboard_chat_consumer.serialize_dashboard_chat_message")
@patch("ddpui.websockets.dashboard_chat_consumer.execute_dashboard_chat_turn")
@patch("ddpui.websockets.dashboard_chat_consumer.create_dashboard_chat_user_message_with_status")
@patch("ddpui.websockets.dashboard_chat_consumer.get_or_create_dashboard_chat_session")
def test_dashboard_chat_consumer_send_message_creates_session_and_runs_inline(
    mock_get_or_create_session,
    mock_create_user_message,
    mock_execute_turn,
    mock_serialize_message,
):
    session = Mock(session_id="session-123")
    user_message = Mock(id=17)
    assistant_message = Mock(id=18)
    mock_get_or_create_session.return_value = session
    mock_create_user_message.return_value = Mock(message=user_message, created=True)
    mock_execute_turn.return_value = {"status": "completed", "assistant_message": assistant_message}
    mock_serialize_message.return_value = {"id": "18", "role": "assistant"}

    consumer = DashboardChatConsumer()
    consumer.dashboard = Mock(id=42)
    consumer.orguser = Mock()
    consumer.send = Mock()
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
    mock_execute_turn.assert_called_once_with("session-123", 17)

    first_payload = json.loads(consumer.send.call_args_list[0].kwargs["text_data"])
    second_payload = json.loads(consumer.send.call_args_list[1].kwargs["text_data"])
    assert first_payload["status"] == "success"
    assert first_payload["data"]["event_type"] == "progress"
    assert second_payload["status"] == "success"
    assert second_payload["data"]["event_type"] == "assistant_message"


@patch(
    "ddpui.websockets.dashboard_chat_consumer.execute_dashboard_chat_turn",
    side_effect=RuntimeError("inline failed"),
)
@patch("ddpui.websockets.dashboard_chat_consumer.create_dashboard_chat_user_message_with_status")
@patch("ddpui.websockets.dashboard_chat_consumer.get_or_create_dashboard_chat_session")
def test_dashboard_chat_consumer_send_message_returns_error_when_inline_turn_fails(
    mock_get_or_create_session,
    mock_create_user_message,
    mock_execute_turn,
):
    session = Mock(session_id="session-123")
    user_message = Mock(id=17)
    mock_get_or_create_session.return_value = session
    mock_create_user_message.return_value = Mock(message=user_message, created=True)

    consumer = DashboardChatConsumer()
    consumer.dashboard = Mock(id=42)
    consumer.orguser = Mock()
    consumer.send = Mock()
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

    mock_execute_turn.assert_called_once_with("session-123", 17)
    consumer._subscribe_to_session.assert_called_once_with("session-123")

    payload = json.loads(consumer.send.call_args_list[-1].kwargs["text_data"])
    assert payload["status"] == "error"
    assert payload["message"] == "Something went wrong while generating the response"


@patch("ddpui.websockets.dashboard_chat_consumer.serialize_dashboard_chat_message")
@patch("ddpui.websockets.dashboard_chat_consumer.find_dashboard_chat_assistant_reply")
@patch("ddpui.websockets.dashboard_chat_consumer.create_dashboard_chat_user_message_with_status")
@patch("ddpui.websockets.dashboard_chat_consumer.get_or_create_dashboard_chat_session")
def test_dashboard_chat_consumer_reuses_existing_turn_without_running_duplicate_turn(
    mock_get_or_create_session,
    mock_create_user_message,
    mock_find_assistant_reply,
    mock_serialize_message,
):
    session = Mock(session_id="session-123")
    user_message = Mock(id=17)
    assistant_message = Mock(id=22)
    mock_get_or_create_session.return_value = session
    mock_create_user_message.return_value = Mock(message=user_message, created=False)
    mock_find_assistant_reply.return_value = assistant_message
    mock_serialize_message.return_value = {"id": "22", "role": "assistant"}

    consumer = DashboardChatConsumer()
    consumer.dashboard = Mock(id=42)
    consumer.orguser = Mock()
    consumer.send = Mock()
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

    consumer._subscribe_to_session.assert_called_once_with("session-123")
    payload = json.loads(consumer.send.call_args.kwargs["text_data"])
    assert payload["status"] == "success"
    assert payload["data"]["event_type"] == "assistant_message"
