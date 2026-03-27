"""Handle-needs-clarification node for dashboard chat graph."""

from typing import Any

from ddpui.core.dashboard_chat.contracts import DashboardChatIntent, DashboardChatResponse

from ddpui.core.dashboard_chat.orchestration.response_composer import (
    build_usage_summary,
    clarification_fallback,
)
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.state.payload_codec import serialize_response
from ddpui.core.dashboard_chat.orchestration.state.accessors import get_intent_decision


def handle_needs_clarification_node(
    state: DashboardChatGraphState, llm_client, vector_store
) -> dict[str, Any]:
    """Ask for clarification when the router says the query is underspecified."""
    intent_decision = get_intent_decision(state)
    return {
        "response": serialize_response(
            DashboardChatResponse(
                answer_text=(
                    intent_decision.clarification_question
                    or clarification_fallback(intent_decision.missing_info)
                ),
                intent=DashboardChatIntent.NEEDS_CLARIFICATION,
                usage=build_usage_summary(llm_client, vector_store),
            )
        )
    }
