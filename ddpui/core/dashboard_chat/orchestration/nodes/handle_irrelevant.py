"""Handle-irrelevant node for dashboard chat graph."""

from typing import Any

from ddpui.core.dashboard_chat.contracts import DashboardChatIntent, DashboardChatResponse

from ddpui.core.dashboard_chat.orchestration.response_composer import build_usage_summary
from ddpui.core.dashboard_chat.orchestration.state.payload_codec import serialize_response
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState


def handle_irrelevant_node(
    state: DashboardChatGraphState, llm_client, vector_store
) -> dict[str, Any]:
    """Handle questions outside dashboard chat scope."""
    return {
        "response": serialize_response(
            DashboardChatResponse(
                answer_text=(
                    "I can only answer questions about this dashboard, its charts, and the data behind them."
                ),
                intent=DashboardChatIntent.IRRELEVANT,
                usage=build_usage_summary(llm_client, vector_store),
            )
        )
    }
