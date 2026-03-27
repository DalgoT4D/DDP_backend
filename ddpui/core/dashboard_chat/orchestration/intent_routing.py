"""Graph intent-routing helpers for dashboard chat orchestration."""

from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.state.accessors import get_intent_decision


def route_after_intent(state: DashboardChatGraphState) -> str:
    """Return the next node name for the current classified intent."""
    return get_intent_decision(state).intent.value
