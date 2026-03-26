"""LLM interface contract for dashboard chat."""

from typing import Any, Protocol

from ddpui.core.dashboard_chat.contracts import (
    DashboardChatConversationContext,
    DashboardChatIntent,
    DashboardChatIntentDecision,
    DashboardChatRetrievedDocument,
)
from ddpui.models.dashboard_chat import DashboardChatPromptTemplateKey


class DashboardChatLlmClient(Protocol):
    """LLM contract used by the dashboard chat LangGraph runtime."""

    def classify_intent(
        self,
        user_query: str,
        conversation_context: DashboardChatConversationContext,
    ) -> DashboardChatIntentDecision:
        """Classify the incoming query."""

    def compose_small_talk(self, user_query: str) -> str:
        """Compose a brief small-talk response describing dashboard chat capabilities."""

    def get_prompt(self, prompt_key: DashboardChatPromptTemplateKey | str) -> str:
        """Return one stored dashboard chat prompt."""

    def reset_usage(self) -> None:
        """Reset per-turn usage tracking before a new runtime invocation."""

    def run_tool_loop_turn(
        self,
        *,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        tool_choice: str,
        operation: str,
    ) -> dict[str, Any]:
        """Run one prototype-style tool-loop completion."""

    def compose_final_answer(
        self,
        *,
        user_query: str,
        intent: DashboardChatIntent,
        response_format: str,
        draft_answer: str | None,
        retrieved_documents: list[DashboardChatRetrievedDocument],
        sql: str | None,
        sql_results: list[dict[str, Any]] | None,
        warnings: list[str],
    ) -> str:
        """Compose the final user-facing markdown answer."""
