"""Handle-irrelevant node for dashboard chat graph."""

from typing import Any

from ddpui.core.dashboard_chat.contracts import DashboardChatIntent, DashboardChatResponse

from ..presentation import build_usage_summary
from ..state import DashboardChatRuntimeState


def handle_irrelevant_node(
    state: DashboardChatRuntimeState, llm_client, vector_store
) -> dict[str, Any]:
    """Handle questions outside dashboard chat scope."""
    return {
        "response": DashboardChatResponse(
            answer_text=(
                "I can only answer questions about this dashboard, its charts, and the data behind them."
            ),
            intent=DashboardChatIntent.IRRELEVANT,
            usage=build_usage_summary(llm_client, vector_store),
        )
    }
