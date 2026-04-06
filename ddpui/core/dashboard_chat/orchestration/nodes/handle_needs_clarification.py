"""Handle-needs-clarification node for dashboard chat graph."""

from typing import Any

from ddpui.core.dashboard_chat.contracts.intent_contracts import (
    DashboardChatIntent,
    DashboardChatIntentDecision,
)
from ddpui.core.dashboard_chat.contracts.response_contracts import DashboardChatResponse
from ddpui.core.dashboard_chat.orchestration.response_composer import (
    build_usage_summary,
    clarification_fallback,
)
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState


def handle_needs_clarification_node(
    state: DashboardChatGraphState, llm_client, vector_store
) -> dict[str, Any]:
    """Ask for clarification when the router says the query is underspecified."""
    intent_decision = DashboardChatIntentDecision.model_validate(state.get("intent_decision") or {})
    return {
        "response": DashboardChatResponse(
            answer_text=(
                intent_decision.clarification_question
                or clarification_fallback(intent_decision.missing_info)
            ),
            intent=DashboardChatIntent.NEEDS_CLARIFICATION,
            usage=build_usage_summary(llm_client, vector_store),
        ).to_dict()
    }
