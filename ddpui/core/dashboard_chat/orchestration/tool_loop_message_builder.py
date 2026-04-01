"""Prompt-message stack helpers for dashboard chat graph execution."""

from typing import Any

from ddpui.models.dashboard_chat import DashboardChatPromptTemplateKey

from ddpui.core.dashboard_chat.orchestration.conversation_context import (
    build_follow_up_context_prompt,
    detect_sql_modification_type,
)
from ddpui.core.dashboard_chat.contracts import DashboardChatConversationContext
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState


def build_new_query_messages(
    llm_client,
    state: DashboardChatGraphState,
) -> list[dict[str, Any]]:
    """Build the new-query message stack."""
    system_prompt = llm_client.get_prompt(DashboardChatPromptTemplateKey.NEW_QUERY_SYSTEM)
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": state["user_query"]},
    ]


def build_follow_up_messages(
    llm_client,
    state: DashboardChatGraphState,
) -> list[dict[str, Any]]:
    """Build the follow-up message stack."""
    modification_type = detect_sql_modification_type(state["user_query"])
    system_prompt = llm_client.get_prompt(DashboardChatPromptTemplateKey.FOLLOW_UP_SYSTEM)
    return [
        {"role": "system", "content": system_prompt},
        {
            "role": "system",
            "content": build_follow_up_context_prompt(
                DashboardChatConversationContext.model_validate(
                    state.get("conversation_context") or {}
                ),
                state["user_query"],
            ),
        },
        {"role": "system", "content": f"MODIFICATION_TYPE: {modification_type}"},
        {"role": "user", "content": state["user_query"]},
    ]
