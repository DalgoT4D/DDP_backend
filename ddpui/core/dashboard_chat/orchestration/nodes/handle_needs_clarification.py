"""Handle-needs-clarification node for dashboard chat graph."""

from typing import Any

from ddpui.core.dashboard_chat.contracts import DashboardChatIntent, DashboardChatResponse

from ..presentation import build_usage_summary, clarification_fallback
from ..state import DashboardChatRuntimeState


def handle_needs_clarification_node(
    state: DashboardChatRuntimeState, llm_client, vector_store
) -> dict[str, Any]:
    """Ask for clarification when the router says the query is underspecified."""
    intent_decision = state["intent_decision"]
    return {
        "response": DashboardChatResponse(
            answer_text=(
                intent_decision.clarification_question
                or clarification_fallback(intent_decision.missing_info)
            ),
            intent=DashboardChatIntent.NEEDS_CLARIFICATION,
            usage=build_usage_summary(llm_client, vector_store),
        )
    }
