"""Prompt-message stack helpers for dashboard chat graph execution."""

from collections.abc import Sequence
from typing import Any

from ddpui.core.dashboard_chat.runtime_types import DashboardChatConversationMessage
from ddpui.models.dashboard_chat import DashboardChatPromptTemplateKey

from .state import DashboardChatRuntimeState


def _build_new_query_messages(
    self,
    state: DashboardChatRuntimeState,
) -> list[dict[str, Any]]:
    """Build the prototype new-query message stack."""
    system_prompt = self.llm_client.get_prompt(
        DashboardChatPromptTemplateKey.NEW_QUERY_SYSTEM
    )
    return [
        {
            "role": "system",
            "content": system_prompt,
        },
        {"role": "user", "content": state["user_query"]},
    ]


def _build_follow_up_messages(
    self,
    state: DashboardChatRuntimeState,
) -> list[dict[str, Any]]:
    """Build the prototype follow-up message stack."""
    modification_type = self._detect_sql_modification_type(state["user_query"])
    system_prompt = self.llm_client.get_prompt(
        DashboardChatPromptTemplateKey.FOLLOW_UP_SYSTEM
    )
    return [
        {
            "role": "system",
            "content": system_prompt,
        },
        {
            "role": "system",
            "content": self._build_follow_up_context_prompt(
                state["conversation_context"],
                state["user_query"],
            ),
        },
        {"role": "system", "content": f"MODIFICATION_TYPE: {modification_type}"},
        {"role": "user", "content": state["user_query"]},
    ]


def _normalize_conversation_history(
    conversation_history: Sequence[DashboardChatConversationMessage | dict[str, Any]] | None,
) -> list[DashboardChatConversationMessage]:
    """Normalize stored history into the typed runtime message format."""
    normalized_messages: list[DashboardChatConversationMessage] = []
    for item in conversation_history or []:
        if isinstance(item, DashboardChatConversationMessage):
            normalized_messages.append(item)
            continue
        normalized_messages.append(
            DashboardChatConversationMessage(
                role=str(item.get("role") or "user"),
                content=str(item.get("content") or ""),
                payload=item.get("payload") or {},
            )
        )
    return normalized_messages
