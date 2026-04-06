"""Handle-small-talk node for dashboard chat graph."""

from typing import Any

from ddpui.core.dashboard_chat.contracts.intent_contracts import DashboardChatIntent
from ddpui.core.dashboard_chat.contracts.response_contracts import DashboardChatResponse

from ddpui.core.dashboard_chat.orchestration.response_composer import (
    build_usage_summary,
    compose_small_talk_response,
)
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState


def handle_small_talk_node(
    state: DashboardChatGraphState, llm_client, vector_store
) -> dict[str, Any]:
    """Handle simple social turns without any tool use."""
    return {
        "response": DashboardChatResponse(
            answer_text=(
                state.get("small_talk_response")
                or compose_small_talk_response(llm_client, state["user_query"])
            ),
            intent=DashboardChatIntent.SMALL_TALK,
            usage=build_usage_summary(llm_client, vector_store),
        ).to_dict()
    }
