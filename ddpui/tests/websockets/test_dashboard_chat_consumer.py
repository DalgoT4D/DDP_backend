import json
from types import SimpleNamespace
from unittest.mock import Mock, patch

from ddpui.models.dashboard_chat import DashboardChatTurnStatus
from ddpui.websockets.dashboard_chat_consumer import DashboardChatConsumer


def build_consumer() -> DashboardChatConsumer:
    consumer = DashboardChatConsumer()
    consumer.send = Mock()
    consumer.dashboard = Mock(id=42)
    consumer.orguser = Mock()
    consumer.active_session_group = None
    consumer._subscribe_to_session = Mock()
    consumer._assert_chat_available = Mock(return_value=None)
    return consumer


def latest_payload(consumer: DashboardChatConsumer) -> dict:
    return json.loads(consumer.send.call_args.kwargs["text_data"])


def test_dashboard_chat_consumer_send_message_requires_message():
    consumer = build_consumer()

    consumer.websocket_receive({"text": json.dumps({"action": "send_message"})})

    payload = latest_payload(consumer)
    assert payload["status"] == "error"
    assert payload["message"] == "Message is required"


def test_dashboard_chat_consumer_send_message_requires_available_chat():
    consumer = build_consumer()
    consumer._assert_chat_available.side_effect = Exception("Chat unavailable")

    consumer.websocket_receive(
        {"text": json.dumps({"action": "send_message", "message": "Why did funding drop?"})}
    )

    payload = latest_payload(consumer)
    assert payload["status"] == "error"
    assert payload["message"] == "Chat unavailable"


@patch("ddpui.websockets.dashboard_chat_consumer.start_dashboard_chat_turn_background")
@patch("ddpui.websockets.dashboard_chat_consumer.publish_dashboard_chat_progress")
@patch("ddpui.websockets.dashboard_chat_consumer.update_dashboard_chat_turn")
@patch("ddpui.websockets.dashboard_chat_consumer.create_dashboard_chat_user_message_with_status")
@patch("ddpui.websockets.dashboard_chat_consumer.get_or_create_dashboard_chat_session")
def test_dashboard_chat_consumer_send_message_starts_background_turn(
    mock_get_or_create_session,
    mock_create_user_message,
    mock_update_turn,
    mock_publish_progress,
    mock_start_background_turn,
):
    session = Mock(session_id="session-123")
    user_message = Mock(id=17)
    turn = Mock(id=23, status=DashboardChatTurnStatus.QUEUED, assistant_message=None)

    mock_get_or_create_session.return_value = session
    mock_create_user_message.return_value = SimpleNamespace(message=user_message, created=True)

    turn_manager = Mock()
    turn_manager.get_or_create.return_value = (turn, True)
    turn_manager.select_related.return_value.get.return_value = turn

    consumer = build_consumer()

    with patch("ddpui.websockets.dashboard_chat_consumer.DashboardChatTurn.objects", turn_manager):
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

    consumer._subscribe_to_session.assert_called_once_with(session)
    mock_publish_progress.assert_called_once()
    mock_update_turn.assert_called_once_with(23, progress_label="Understanding question")
    mock_start_background_turn.assert_called_once_with(23)
    consumer.send.assert_not_called()


@patch("ddpui.websockets.dashboard_chat_consumer.publish_dashboard_chat_assistant_message")
@patch("ddpui.websockets.dashboard_chat_consumer.create_dashboard_chat_user_message_with_status")
@patch("ddpui.websockets.dashboard_chat_consumer.get_or_create_dashboard_chat_session")
def test_dashboard_chat_consumer_duplicate_send_reuses_completed_turn(
    mock_get_or_create_session,
    mock_create_user_message,
    mock_publish_assistant_message,
):
    session = Mock(session_id="session-123")
    user_message = Mock(id=17)
    assistant_message = Mock(id=31)
    turn = Mock(
        id=23,
        status=DashboardChatTurnStatus.COMPLETED,
        assistant_message=assistant_message,
    )

    mock_get_or_create_session.return_value = session
    mock_create_user_message.return_value = SimpleNamespace(message=user_message, created=False)

    turn_manager = Mock()
    turn_manager.get_or_create.return_value = (turn, False)
    turn_manager.select_related.return_value.get.return_value = turn

    consumer = build_consumer()

    with patch(
        "ddpui.websockets.dashboard_chat_consumer.DashboardChatTurn.objects",
        turn_manager,
    ), patch(
        "ddpui.websockets.dashboard_chat_consumer.start_dashboard_chat_turn_background"
    ) as mock_start_background_turn:
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

    mock_publish_assistant_message.assert_called_once_with(
        session=session,
        turn=turn,
        message=assistant_message,
    )
    mock_start_background_turn.assert_not_called()


@patch("ddpui.websockets.dashboard_chat_consumer.publish_dashboard_chat_progress")
@patch("ddpui.websockets.dashboard_chat_consumer.create_dashboard_chat_user_message_with_status")
@patch("ddpui.websockets.dashboard_chat_consumer.get_or_create_dashboard_chat_session")
def test_dashboard_chat_consumer_duplicate_send_reuses_in_flight_turn(
    mock_get_or_create_session,
    mock_create_user_message,
    mock_publish_progress,
):
    session = Mock(session_id="session-123")
    user_message = Mock(id=17)
    turn = Mock(
        id=23,
        status=DashboardChatTurnStatus.RUNNING,
        progress_label="Searching relevant sources",
        user_message_id=17,
    )

    mock_get_or_create_session.return_value = session
    mock_create_user_message.return_value = SimpleNamespace(message=user_message, created=False)

    turn_manager = Mock()
    turn_manager.get_or_create.return_value = (turn, False)
    turn_manager.select_related.return_value.get.return_value = turn

    consumer = build_consumer()

    with patch(
        "ddpui.websockets.dashboard_chat_consumer.DashboardChatTurn.objects",
        turn_manager,
    ), patch(
        "ddpui.websockets.dashboard_chat_consumer.start_dashboard_chat_turn_background"
    ) as mock_start_background_turn:
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

    mock_publish_progress.assert_called_once_with(
        session=session,
        turn=turn,
        label="Searching relevant sources",
        stage=None,
        message_id=17,
    )
    mock_start_background_turn.assert_not_called()


@patch("ddpui.websockets.dashboard_chat_consumer.start_dashboard_chat_turn_background")
@patch("ddpui.websockets.dashboard_chat_consumer.publish_dashboard_chat_progress")
@patch("ddpui.websockets.dashboard_chat_consumer.update_dashboard_chat_turn")
@patch("ddpui.websockets.dashboard_chat_consumer.create_dashboard_chat_user_message_with_status")
@patch("ddpui.websockets.dashboard_chat_consumer.get_or_create_dashboard_chat_session")
def test_dashboard_chat_consumer_retry_starts_background_turn_if_turn_row_was_missing(
    mock_get_or_create_session,
    mock_create_user_message,
    mock_update_turn,
    mock_publish_progress,
    mock_start_background_turn,
):
    session = Mock(session_id="session-123")
    user_message = Mock(id=17)
    turn = Mock(id=23, status=DashboardChatTurnStatus.QUEUED, assistant_message=None)

    mock_get_or_create_session.return_value = session
    mock_create_user_message.return_value = SimpleNamespace(message=user_message, created=False)

    turn_manager = Mock()
    turn_manager.get_or_create.return_value = (turn, True)
    turn_manager.select_related.return_value.get.return_value = turn

    consumer = build_consumer()

    with patch("ddpui.websockets.dashboard_chat_consumer.DashboardChatTurn.objects", turn_manager):
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

    mock_publish_progress.assert_called_once()
    mock_update_turn.assert_called_once_with(23, progress_label="Understanding question")
    mock_start_background_turn.assert_called_once_with(23)


@patch("ddpui.websockets.dashboard_chat_consumer.publish_dashboard_chat_cancelled")
@patch("ddpui.websockets.dashboard_chat_consumer.update_dashboard_chat_turn")
@patch("ddpui.websockets.dashboard_chat_consumer.get_or_create_dashboard_chat_session")
def test_dashboard_chat_consumer_cancel_queued_turn_cancels_immediately(
    mock_get_or_create_session,
    mock_update_turn,
    mock_publish_cancelled,
):
    session = Mock(session_id="session-123")
    turn = Mock(id=23, status=DashboardChatTurnStatus.QUEUED)
    updated_turn = Mock(id=23)

    mock_get_or_create_session.return_value = session
    mock_update_turn.return_value = updated_turn

    turn_manager = Mock()
    turn_manager.filter.return_value.filter.return_value.order_by.return_value.first.return_value = (
        turn
    )

    consumer = build_consumer()

    with patch("ddpui.websockets.dashboard_chat_consumer.DashboardChatTurn.objects", turn_manager):
        consumer.websocket_receive(
            {
                "text": json.dumps(
                    {
                        "action": "cancel_message",
                        "session_id": "session-123",
                        "turn_id": "23",
                    }
                )
            }
        )

    mock_update_turn.assert_called_once()
    mock_publish_cancelled.assert_called_once_with(session=session, turn=updated_turn)


@patch("ddpui.websockets.dashboard_chat_consumer.publish_dashboard_chat_progress")
@patch("ddpui.websockets.dashboard_chat_consumer.update_dashboard_chat_turn")
@patch("ddpui.websockets.dashboard_chat_consumer.get_or_create_dashboard_chat_session")
def test_dashboard_chat_consumer_cancel_running_turn_marks_cancel_requested(
    mock_get_or_create_session,
    mock_update_turn,
    mock_publish_progress,
):
    session = Mock(session_id="session-123")
    turn = Mock(id=23, status=DashboardChatTurnStatus.RUNNING)
    updated_turn = Mock(id=23)

    mock_get_or_create_session.return_value = session
    mock_update_turn.return_value = updated_turn

    turn_manager = Mock()
    turn_manager.filter.return_value.filter.return_value.order_by.return_value.first.return_value = (
        turn
    )

    consumer = build_consumer()

    with patch("ddpui.websockets.dashboard_chat_consumer.DashboardChatTurn.objects", turn_manager):
        consumer.websocket_receive(
            {
                "text": json.dumps(
                    {
                        "action": "cancel_message",
                        "session_id": "session-123",
                        "turn_id": "23",
                    }
                )
            }
        )

    mock_update_turn.assert_called_once()
    mock_publish_progress.assert_called_once()


def test_dashboard_chat_consumer_cancel_requires_session_id():
    consumer = build_consumer()

    consumer.websocket_receive({"text": json.dumps({"action": "cancel_message"})})

    payload = latest_payload(consumer)
    assert payload["status"] == "error"
    assert payload["message"] == "session_id is required to cancel a message"


@patch("ddpui.websockets.dashboard_chat_consumer.get_or_create_dashboard_chat_session")
def test_dashboard_chat_consumer_cancel_rejects_non_integer_turn_id(mock_get_or_create_session):
    session = Mock(session_id="session-123")
    mock_get_or_create_session.return_value = session
    consumer = build_consumer()

    turn_manager = Mock()
    turn_manager.filter.return_value = turn_manager

    with patch("ddpui.websockets.dashboard_chat_consumer.DashboardChatTurn.objects", turn_manager):
        consumer.websocket_receive(
            {
                "text": json.dumps(
                    {
                        "action": "cancel_message",
                        "session_id": "session-123",
                        "turn_id": "not-an-int",
                    }
                )
            }
        )

    payload = latest_payload(consumer)
    assert payload["status"] == "error"
    assert payload["message"] == "turn_id must be an integer"
