"""Direct OpenAI client wrapper for dashboard chat runtime."""

from collections.abc import Sequence
import json
import os
from typing import Any, Protocol

from ddpui.core.dashboard_chat.runtime_types import (
    DashboardChatConversationMessage,
    DashboardChatIntent,
    DashboardChatIntentDecision,
    DashboardChatPlanMode,
    DashboardChatQueryPlan,
    DashboardChatRetrievedDocument,
    DashboardChatSqlDraft,
    DashboardChatTextFilterPlan,
)


class DashboardChatLlmClient(Protocol):
    """LLM contract used by the dashboard chat LangGraph runtime."""

    def classify_intent(
        self,
        user_query: str,
        conversation_history: Sequence[DashboardChatConversationMessage],
        dashboard_summary: str,
    ) -> DashboardChatIntentDecision:
        """Classify the incoming query."""

    def plan_query(
        self,
        user_query: str,
        conversation_history: Sequence[DashboardChatConversationMessage],
        dashboard_summary: str,
        retrieved_documents: Sequence[DashboardChatRetrievedDocument],
        schema_prompt: str,
        allowlisted_tables: Sequence[str],
    ) -> DashboardChatQueryPlan:
        """Build a structured plan for the query."""

    def generate_sql(
        self,
        user_query: str,
        dashboard_summary: str,
        query_plan: DashboardChatQueryPlan,
        schema_prompt: str,
        distinct_values: dict[str, list[str]],
        allowlisted_tables: Sequence[str],
    ) -> DashboardChatSqlDraft:
        """Generate SQL from the structured plan."""

    def compose_answer(
        self,
        user_query: str,
        dashboard_summary: str,
        retrieved_documents: Sequence[DashboardChatRetrievedDocument],
        sql: str | None,
        sql_results: list[dict[str, Any]] | None,
        warnings: Sequence[str],
        related_dashboard_titles: Sequence[str],
    ) -> str:
        """Compose the final answer text."""


class OpenAIDashboardChatLlmClient:
    """Direct OpenAI SDK adapter with JSON-mode helpers."""

    def __init__(
        self,
        api_key: str | None = None,
        model: str = "gpt-4o-mini",
        timeout_ms: int = 45000,
        client: Any = None,
    ):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model
        self.timeout_ms = timeout_ms
        if client is None:
            if not self.api_key:
                raise ValueError("OPENAI_API_KEY must be set for dashboard chat runtime")
            from openai import OpenAI

            client = OpenAI(api_key=self.api_key, timeout=timeout_ms / 1000)
        self.client = client

    def classify_intent(
        self,
        user_query: str,
        conversation_history: Sequence[DashboardChatConversationMessage],
        dashboard_summary: str,
    ) -> DashboardChatIntentDecision:
        """Classify intent with lightweight conversation awareness."""
        prompt = {
            "dashboard_summary": dashboard_summary,
            "conversation_history": [message.__dict__ for message in conversation_history[-6:]],
            "user_query": user_query,
        }
        result = self._complete_json(
            system_prompt=(
                "Classify the user query for an NGO dashboard assistant. "
                "Return JSON with keys intent, reason, force_sql_path, clarification_question. "
                "Allowed intents: data_query, context_query, needs_clarification, small_talk, irrelevant. "
                "Set force_sql_path=true for any query that asks for counts, trends, breakdowns, comparisons, "
                "filters, or tabular data."
            ),
            user_prompt=json.dumps(prompt, ensure_ascii=True),
        )
        intent_value = result.get("intent", DashboardChatIntent.CONTEXT_QUERY.value)
        try:
            intent = DashboardChatIntent(intent_value)
        except ValueError:
            intent = DashboardChatIntent.CONTEXT_QUERY
        return DashboardChatIntentDecision(
            intent=intent,
            reason=str(result.get("reason") or "LLM classification"),
            force_sql_path=bool(result.get("force_sql_path", intent == DashboardChatIntent.DATA_QUERY)),
            clarification_question=result.get("clarification_question"),
        )

    def plan_query(
        self,
        user_query: str,
        conversation_history: Sequence[DashboardChatConversationMessage],
        dashboard_summary: str,
        retrieved_documents: Sequence[DashboardChatRetrievedDocument],
        schema_prompt: str,
        allowlisted_tables: Sequence[str],
    ) -> DashboardChatQueryPlan:
        """Generate a structured execution plan."""
        prompt = {
            "dashboard_summary": dashboard_summary,
            "conversation_history": [message.__dict__ for message in conversation_history[-6:]],
            "retrieved_documents": [
                {
                    "source_type": document.source_type,
                    "source_identifier": document.source_identifier,
                    "content": document.content[:500],
                }
                for document in retrieved_documents[:8]
            ],
            "schema_prompt": schema_prompt,
            "allowlisted_tables": list(allowlisted_tables),
            "user_query": user_query,
        }
        result = self._complete_json(
            system_prompt=(
                "Plan how to answer the dashboard question. "
                "Return JSON with keys mode, reason, relevant_tables, schema_lookup_tables, text_filters, "
                "answer_strategy, clarification_question. "
                "Allowed modes: sql, context, clarify. "
                "text_filters must be an array of objects with table_name, column_name, requested_value. "
                "If the question can be answered from context or retrieved docs without SQL, choose context. "
                "If the question needs row-level or aggregate data, choose sql."
            ),
            user_prompt=json.dumps(prompt, ensure_ascii=True),
        )
        mode_value = result.get("mode", DashboardChatPlanMode.CONTEXT.value)
        try:
            mode = DashboardChatPlanMode(mode_value)
        except ValueError:
            mode = DashboardChatPlanMode.CONTEXT
        return DashboardChatQueryPlan(
            mode=mode,
            reason=str(result.get("reason") or "LLM plan"),
            relevant_tables=_normalize_table_list(result.get("relevant_tables")),
            schema_lookup_tables=_normalize_table_list(result.get("schema_lookup_tables")),
            text_filters=[
                DashboardChatTextFilterPlan(
                    table_name=str(item.get("table_name") or "").lower(),
                    column_name=str(item.get("column_name") or ""),
                    requested_value=str(item.get("requested_value") or ""),
                )
                for item in result.get("text_filters", [])
                if item.get("table_name") and item.get("column_name") and item.get("requested_value")
            ],
            answer_strategy=result.get("answer_strategy"),
            clarification_question=result.get("clarification_question"),
        )

    def generate_sql(
        self,
        user_query: str,
        dashboard_summary: str,
        query_plan: DashboardChatQueryPlan,
        schema_prompt: str,
        distinct_values: dict[str, list[str]],
        allowlisted_tables: Sequence[str],
    ) -> DashboardChatSqlDraft:
        """Generate a single read-only SQL statement."""
        prompt = {
            "dashboard_summary": dashboard_summary,
            "query_plan": {
                "mode": query_plan.mode.value,
                "reason": query_plan.reason,
                "relevant_tables": query_plan.relevant_tables,
                "schema_lookup_tables": query_plan.schema_lookup_tables,
                "text_filters": [text_filter.__dict__ for text_filter in query_plan.text_filters],
                "answer_strategy": query_plan.answer_strategy,
            },
            "schema_prompt": schema_prompt,
            "distinct_values": distinct_values,
            "allowlisted_tables": list(allowlisted_tables),
            "user_query": user_query,
        }
        result = self._complete_json(
            system_prompt=(
                "Generate one safe read-only SQL query. "
                "Return JSON with keys sql, reason, warnings, clarification_question. "
                "The SQL must be a single SELECT or WITH...SELECT statement that only references allowlisted tables. "
                "Use exact values from the provided distinct_values map for text filters when available. "
                "If the question cannot be answered safely, return sql as null and provide clarification_question."
            ),
            user_prompt=json.dumps(prompt, ensure_ascii=True),
        )
        sql = result.get("sql")
        if sql is not None:
            sql = str(sql).strip()
        return DashboardChatSqlDraft(
            sql=sql or None,
            reason=str(result.get("reason") or "LLM SQL draft"),
            warnings=[str(warning) for warning in result.get("warnings", [])],
            clarification_question=result.get("clarification_question"),
        )

    def compose_answer(
        self,
        user_query: str,
        dashboard_summary: str,
        retrieved_documents: Sequence[DashboardChatRetrievedDocument],
        sql: str | None,
        sql_results: list[dict[str, Any]] | None,
        warnings: Sequence[str],
        related_dashboard_titles: Sequence[str],
    ) -> str:
        """Compose the final user-facing answer."""
        response = self.client.chat.completions.create(
            model=self.model,
            temperature=0,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You answer NGO dashboard questions. "
                        "Use plain language, cite data-backed claims carefully, and avoid exposing hidden reasoning. "
                        "If SQL results are empty, say that no matching rows were found."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "dashboard_summary": dashboard_summary,
                            "user_query": user_query,
                            "retrieved_documents": [
                                {
                                    "source_type": document.source_type,
                                    "source_identifier": document.source_identifier,
                                    "content": document.content[:400],
                                }
                                for document in retrieved_documents[:8]
                            ],
                            "sql": sql,
                            "sql_results": sql_results,
                            "warnings": list(warnings),
                            "related_dashboards": list(related_dashboard_titles),
                        },
                        ensure_ascii=True,
                    ),
                },
            ],
        )
        answer = response.choices[0].message.content or ""
        return answer.strip()

    def _complete_json(self, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        """Run a JSON-mode chat completion and parse the result."""
        response = self.client.chat.completions.create(
            model=self.model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        content = response.choices[0].message.content or "{}"
        return json.loads(content)


def _normalize_table_list(value: Any) -> list[str]:
    """Normalize a JSON value into a lowercased table list."""
    if not isinstance(value, list):
        return []
    return [str(table_name).lower() for table_name in value if table_name]
