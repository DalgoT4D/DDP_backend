"""Graph intent-routing helpers for dashboard chat orchestration."""

from ddpui.core.dashboard_chat.contracts.intent_contracts import DashboardChatIntentDecision
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState


def route_after_intent(state: DashboardChatGraphState) -> str:
    """Return the next node name for the current classified intent."""
    return DashboardChatIntentDecision.model_validate(
        state.get("intent_decision") or {}
    ).intent.value
