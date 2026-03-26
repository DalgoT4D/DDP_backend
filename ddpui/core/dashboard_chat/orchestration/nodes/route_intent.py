"""Route-intent node for dashboard chat graph."""

from typing import Any

from ddpui.core.dashboard_chat.orchestration.conversation import extract_conversation_context
from ddpui.core.dashboard_chat.orchestration.presentation import (
    build_fast_path_intent,
    build_fast_path_small_talk_response,
)
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatRuntimeState


def route_intent_node(state: DashboardChatRuntimeState, llm_client) -> dict[str, Any]:
    """Use the prototype router prompt for all non-trivial routing."""
    conversation_context = extract_conversation_context(state["conversation_history"])
    fast_path_intent = build_fast_path_intent(state["user_query"])
    if fast_path_intent is not None:
        return {
            "conversation_context": conversation_context,
            "intent_decision": fast_path_intent,
            "small_talk_response": build_fast_path_small_talk_response(state["user_query"]),
        }
    intent_decision = llm_client.classify_intent(
        user_query=state["user_query"],
        conversation_context=conversation_context,
    )
    return {
        "conversation_context": conversation_context,
        "intent_decision": intent_decision,
    }
