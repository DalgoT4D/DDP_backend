from unittest.mock import Mock, patch

from ddpui.core.dashboard_chat.sessions.session_service import _run_dashboard_chat_turn_in_background
from ddpui.models.dashboard_chat import DashboardChatTurnStatus


@patch("ddpui.core.dashboard_chat.sessions.session_service.close_old_connections")
@patch("ddpui.core.dashboard_chat.sessions.session_service.publish_dashboard_chat_cancelled")
@patch("ddpui.core.dashboard_chat.sessions.session_service.update_dashboard_chat_turn")
def test_background_runner_skips_duplicate_cancelled_event(
    mock_update_turn,
    mock_publish_cancelled,
    mock_close_old_connections,
):
    turn = Mock(
        id=23,
        status=DashboardChatTurnStatus.CANCELLED,
        session=Mock(),
        user_message=Mock(),
    )
    turn_manager = Mock()
    turn_manager.select_related.return_value.filter.return_value.first.return_value = turn

    with patch(
        "ddpui.core.dashboard_chat.sessions.session_service.DashboardChatTurn.objects",
        turn_manager,
    ):
        _run_dashboard_chat_turn_in_background(23)

    mock_close_old_connections.assert_called()
    mock_update_turn.assert_not_called()
    mock_publish_cancelled.assert_not_called()
