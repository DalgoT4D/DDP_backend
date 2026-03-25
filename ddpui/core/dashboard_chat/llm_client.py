"""Direct OpenAI client wrapper for dashboard chat runtime."""

import json
import logging
import os
from time import sleep
from typing import Any, Protocol

from ddpui.core.dashboard_chat.prompt_store import DashboardChatPromptStore
from ddpui.core.dashboard_chat.runtime_types import (
    DashboardChatConversationContext,
    DashboardChatFollowUpContext,
    DashboardChatIntent,
    DashboardChatIntentDecision,
    DashboardChatRetrievedDocument,
)
from ddpui.models.dashboard_chat import DashboardChatPromptTemplateKey

logger = logging.getLogger("ddpui")


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


class OpenAIDashboardChatLlmClient:
    """Direct OpenAI SDK adapter with JSON-mode helpers."""

    TECHNICAL_DIFFICULTIES_MESSAGE = (
        "I'm experiencing technical difficulties. Please try again."
    )
    TABLE_SUMMARY_JSON_INSTRUCTIONS = """
For table-like responses, return valid JSON only with this shape:
{
  "title": "short heading or null",
  "summary": "1-2 sentence narrative summary",
  "key_points": ["short point", "short point"]
}

Rules:
- Do not include markdown tables.
- Do not include pipe characters or ASCII table formatting.
- Do not repeat every row from the result set.
- The UI will render the structured table separately from sql_results.
- Keep key_points to at most 3 concise bullets.
""".strip()

    def __init__(
        self,
        api_key: str | None = None,
        model: str = "gpt-4o-mini",
        timeout_ms: int = 12000,
        max_attempts: int = 1,
        client: Any = None,
        prompt_store: DashboardChatPromptStore | None = None,
    ):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model
        self.timeout_ms = timeout_ms
        self.max_attempts = max(1, max_attempts)
        self.prompt_store = prompt_store or DashboardChatPromptStore()
        self.usage_events: list[dict[str, Any]] = []
        if client is None:
            if not self.api_key:
                raise ValueError("OPENAI_API_KEY must be set for dashboard chat runtime")
            from openai import OpenAI

            client = OpenAI(
                api_key=self.api_key,
                timeout=timeout_ms / 1000,
                max_retries=0,
            )
        self.client = client

    def reset_usage(self) -> None:
        """Reset aggregated OpenAI usage before one new chat turn."""
        self.usage_events = []

    def classify_intent(
        self,
        user_query: str,
        conversation_context: DashboardChatConversationContext,
    ) -> DashboardChatIntentDecision:
        """Classify intent with prototype-style conversation awareness."""
        system_prompt = self.prompt_store.get(
            DashboardChatPromptTemplateKey.INTENT_CLASSIFICATION
        )
        if conversation_context.last_sql_query or conversation_context.last_chart_ids:
            system_prompt += (
                "\n\nCONVERSATION CONTEXT:\n"
                f"- Previous SQL: {conversation_context.last_sql_query or 'None'}\n"
                f"- Previous tables: {', '.join(conversation_context.last_tables_used) or 'None'}\n"
                f"- Previous charts: {', '.join(conversation_context.last_chart_ids) or 'None'}\n"
                f"- Last response type: {conversation_context.last_response_type or 'None'}\n\n"
                "Use this context to detect follow-up queries that want to modify or expand on previous results."
            )
        try:
            result = self._complete_json(
                operation="intent_classification",
                system_prompt=system_prompt,
                user_prompt=f"Classify this query: {user_query}",
            )
        except Exception:
            logger.exception("Dashboard chat intent classification failed")
            return DashboardChatIntentDecision(
                intent=DashboardChatIntent.NEEDS_CLARIFICATION,
                confidence=0.0,
                reason="Intent classification failed",
                clarification_question=self.TECHNICAL_DIFFICULTIES_MESSAGE,
            )
        intent_value = result.get("intent", DashboardChatIntent.QUERY_WITHOUT_SQL.value)
        try:
            intent = DashboardChatIntent(intent_value)
        except ValueError:
            intent = DashboardChatIntent.QUERY_WITHOUT_SQL
        follow_up_result = result.get("follow_up_context") or {}
        follow_up_context = DashboardChatFollowUpContext(
            is_follow_up=bool(follow_up_result.get("is_follow_up")),
            follow_up_type=follow_up_result.get("follow_up_type"),
            reusable_elements=follow_up_result.get("reusable_elements") or {},
            modification_instruction=follow_up_result.get("modification_instruction"),
        )
        return DashboardChatIntentDecision(
            intent=intent,
            confidence=float(result.get("confidence") or 0.0),
            reason=str(result.get("reason") or "LLM classification"),
            missing_info=[str(item) for item in result.get("missing_info", []) if item],
            force_tool_usage=bool(
                result.get(
                    "force_tool_usage",
                    intent
                    in {
                        DashboardChatIntent.QUERY_WITH_SQL,
                        DashboardChatIntent.FOLLOW_UP_SQL,
                    },
                )
            ),
            clarification_question=result.get("clarification_question"),
            follow_up_context=follow_up_context,
        )

    def compose_small_talk(self, user_query: str) -> str:
        """Generate a brief friendly response using the prototype capabilities prompt."""
        response = self._create_chat_completion(
            messages=[
                {
                    "role": "system",
                    "content": self.prompt_store.get(
                        DashboardChatPromptTemplateKey.SMALL_TALK_CAPABILITIES
                    ),
                },
                {"role": "user", "content": user_query},
            ],
            temperature=0.5,
            max_tokens=80,
        )
        self._record_usage("small_talk", response)
        answer = response.choices[0].message.content or ""
        return answer.strip()

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
        """Compose the final user-facing markdown answer from tool-loop outputs."""
        context_payload = {
            "user_query": user_query,
            "intent": intent.value,
            "response_format": response_format,
            "draft_answer": draft_answer or None,
            "warnings": warnings[:5],
            "sql": sql,
            "sql_results": (sql_results or [])[:8],
            "row_count": len(sql_results or []),
            "retrieved_context": [
                {
                    "source_type": document.source_type,
                    "source_identifier": document.source_identifier,
                    "content": self._compact_snippet(document.content),
                }
                for document in retrieved_documents[:6]
            ],
        }
        if response_format in {"text_with_table", "table"}:
            result = self._complete_json(
                operation="final_answer_table_summary",
                system_prompt=(
                    self.prompt_store.get(
                        DashboardChatPromptTemplateKey.FINAL_ANSWER_COMPOSITION
                    )
                    + "\n\n"
                    + self.TABLE_SUMMARY_JSON_INSTRUCTIONS
                ),
                user_prompt=json.dumps(context_payload, ensure_ascii=False),
            )
            return self._format_table_summary_markdown(result)

        response = self._create_chat_completion(
            messages=[
                {
                    "role": "system",
                    "content": self.prompt_store.get(
                        DashboardChatPromptTemplateKey.FINAL_ANSWER_COMPOSITION
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(context_payload, ensure_ascii=False),
                },
            ],
            temperature=0.1,
            max_tokens=400,
        )
        self._record_usage("final_answer_composition", response)
        answer = response.choices[0].message.content or ""
        return answer.strip()

    def get_prompt(self, prompt_key: DashboardChatPromptTemplateKey | str) -> str:
        """Return one stored dashboard chat prompt."""
        return self.prompt_store.get(prompt_key)

    def run_tool_loop_turn(
        self,
        *,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        tool_choice: str,
        operation: str,
    ) -> dict[str, Any]:
        """Run one raw OpenAI tool-calling turn and normalize the response."""
        try:
            response = self._create_chat_completion(
                messages=messages,
                tools=tools,
                tool_choice=tool_choice,
                temperature=0,
            )
        except Exception:
            return {"content": self.TECHNICAL_DIFFICULTIES_MESSAGE, "tool_calls": []}
        self._record_usage(operation, response)
        message = response.choices[0].message
        tool_calls: list[dict[str, Any]] = []
        if message.tool_calls:
            for tool_call in message.tool_calls:
                tool_calls.append(
                    {
                        "id": tool_call.id,
                        "name": tool_call.function.name,
                        "args": tool_call.function.arguments,
                    }
                )
        return {"content": message.content or "", "tool_calls": tool_calls}

    def _complete_json(self, operation: str, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        """Run a JSON-mode chat completion and parse the result."""
        response = self._create_chat_completion(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
            response_format={"type": "json_object"},
        )
        self._record_usage(operation, response)
        content = response.choices[0].message.content or "{}"
        return json.loads(content)

    def usage_summary(self) -> dict[str, Any]:
        """Return aggregated OpenAI chat-completion usage for the current turn."""
        totals = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }
        for event in self.usage_events:
            totals["prompt_tokens"] += event.get("prompt_tokens", 0)
            totals["completion_tokens"] += event.get("completion_tokens", 0)
            totals["total_tokens"] += event.get("total_tokens", 0)
        return {
            "model": self.model,
            "calls": list(self.usage_events),
            "totals": totals,
        }

    def _record_usage(self, operation: str, response: Any) -> None:
        """Capture usage data from one OpenAI response when available."""
        usage = getattr(response, "usage", None)
        if usage is None:
            return
        self.usage_events.append(
            {
                "operation": operation,
                "model": self.model,
                "prompt_tokens": getattr(usage, "prompt_tokens", 0) or 0,
                "completion_tokens": getattr(usage, "completion_tokens", 0) or 0,
                "total_tokens": getattr(usage, "total_tokens", 0) or 0,
            }
        )

    def _create_chat_completion(self, **kwargs: Any) -> Any:
        """Run one OpenAI chat completion with a small interactive retry envelope."""
        last_error: Exception | None = None
        for attempt in range(self.max_attempts):
            try:
                return self.client.chat.completions.create(
                    model=self.model,
                    **kwargs,
                )
            except Exception as error:
                last_error = error
                if attempt == self.max_attempts - 1:
                    break
                sleep(min(2**attempt, 2))
        assert last_error is not None
        raise last_error

    @staticmethod
    def _compact_snippet(content: str, max_length: int = 320) -> str:
        """Trim retrieved context before feeding it into the final answer prompt."""
        normalized_content = " ".join(content.split())
        if len(normalized_content) <= max_length:
            return normalized_content
        return normalized_content[: max_length - 1].rstrip() + "…"

    @staticmethod
    def _format_table_summary_markdown(result: dict[str, Any]) -> str:
        """Render a structured table summary into short markdown without any table body."""
        title = str(result.get("title") or "").strip()
        summary = str(result.get("summary") or "").strip()
        raw_key_points = result.get("key_points") or []
        key_points = [
            str(point).strip()
            for point in raw_key_points
            if isinstance(point, str) and point.strip()
        ][:3]

        sections: list[str] = []
        if title:
            sections.append(f"### {title}")
        if summary:
            sections.append(summary)
        if key_points:
            sections.append("\n".join(f"- {point}" for point in key_points))
        return "\n\n".join(section for section in sections if section).strip()
