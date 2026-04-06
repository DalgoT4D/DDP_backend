"""Conversation-history helpers for dashboard chat graph execution."""

from collections.abc import Sequence
import re
from typing import Any

from ddpui.core.dashboard_chat.contracts.conversation_contracts import (
    DashboardChatConversationContext,
    DashboardChatConversationMessage,
)
from ddpui.core.dashboard_chat.warehouse.sql_guard import DashboardChatSqlGuard

from ddpui.core.dashboard_chat.orchestration.source_identifier_parsing import (
    chart_id_from_source_identifier,
)


def extract_conversation_context(
    conversation_history: Sequence[DashboardChatConversationMessage | dict[str, Any]],
) -> DashboardChatConversationContext:
    """Extract reusable conversation context from message history."""
    context = DashboardChatConversationContext()
    recent_history = list(conversation_history)[-10:]

    for message in reversed(recent_history):
        if isinstance(message, dict):
            message = DashboardChatConversationMessage(
                role=str(message.get("role") or "user"),
                content=str(message.get("content") or ""),
                payload=message.get("payload") or {},
            )
        if message.role != "assistant":
            continue

        payload = message.payload or {}
        sql = payload.get("sql")
        metadata = payload.get("metadata") or {}
        citations = payload.get("citations") or []
        chart_ids = extract_chart_ids_from_payload(payload)

        if chart_ids and context.last_sql_query and not context.last_chart_ids:
            context = DashboardChatConversationContext(
                last_sql_query=context.last_sql_query,
                last_tables_used=context.last_tables_used,
                last_chart_ids=chart_ids,
                last_metrics=context.last_metrics,
                last_dimensions=context.last_dimensions,
                last_filters=context.last_filters,
                last_response_type=context.last_response_type,
                last_answer_text=context.last_answer_text,
                last_intent=context.last_intent,
            )
            break

        if sql and not context.last_sql_query:
            tables = [
                str(table_name).lower()
                for table_name in metadata.get("query_plan_tables") or []
                if table_name
            ]
            if not tables:
                tables = [
                    str(citation.get("table_name")).lower()
                    for citation in citations
                    if citation.get("table_name")
                ]
            if not tables:
                tables = DashboardChatSqlGuard._extract_table_names(str(sql))
            context = DashboardChatConversationContext(
                last_sql_query=str(sql),
                last_tables_used=list(dict.fromkeys(tables)),
                last_chart_ids=chart_ids,
                last_metrics=extract_metrics_from_sql(str(sql)),
                last_dimensions=extract_dimensions_from_sql(str(sql)),
                last_filters=extract_filters_from_sql(str(sql)),
                last_response_type="sql_result",
                last_answer_text=message.content,
                last_intent=str(payload.get("intent") or ""),
            )
            if chart_ids:
                break
            continue

        if payload and context.last_response_type is None:
            context = DashboardChatConversationContext(
                last_chart_ids=chart_ids,
                last_response_type="metadata_answer",
                last_answer_text=message.content,
                last_intent=str(payload.get("intent") or ""),
            )

    return context


def extract_chart_ids_from_payload(payload: dict[str, Any]) -> list[str]:
    """Extract chart ids from persisted metadata/citations."""
    metadata = payload.get("metadata") or {}
    chart_ids = [str(chart_id) for chart_id in metadata.get("chart_ids_used") or [] if chart_id]
    if chart_ids:
        return list(dict.fromkeys(chart_ids))

    extracted_chart_ids: list[str] = []
    for citation in payload.get("citations") or []:
        source_identifier = str(citation.get("source_identifier") or "")
        chart_id = chart_id_from_source_identifier(source_identifier)
        if chart_id is not None:
            extracted_chart_ids.append(str(chart_id))
    return list(dict.fromkeys(extracted_chart_ids))


def build_follow_up_context_prompt(
    conversation_context: DashboardChatConversationContext,
    user_query: str,
) -> str:
    """Build the follow-up context prompt injected into the message stack."""
    return "\n".join(
        [
            "PREVIOUS QUERY CONTEXT:",
            f"Last SQL: {conversation_context.last_sql_query or 'None'}",
            f"Tables used: {', '.join(conversation_context.last_tables_used) or 'None'}",
            f"Metrics: {', '.join(conversation_context.last_metrics) or 'None'}",
            f"Dimensions: {', '.join(conversation_context.last_dimensions) or 'None'}",
            f"Filters: {', '.join(conversation_context.last_filters) or 'None'}",
            "",
            f"NEW INSTRUCTION: {user_query}",
            "",
            "TASK: Modify the previous query based on the new instruction. Reuse tables and context where possible.",
        ]
    )


def detect_sql_modification_type(user_query: str) -> str:
    """Detect the coarse follow-up modification category from the user's phrasing."""
    lowered_query = user_query.lower()
    if any(keyword in lowered_query for keyword in ["by", "split by", "break down", "group by"]):
        return "add_dimension"
    if any(keyword in lowered_query for keyword in ["filter", "only", "exclude", "where"]):
        return "add_filter"
    if any(
        keyword in lowered_query
        for keyword in ["last", "this", "previous", "next", "monthly", "weekly", "quarterly"]
    ):
        return "modify_timeframe"
    if any(
        keyword in lowered_query
        for keyword in ["total", "sum", "count", "average", "avg", "maximum", "minimum"]
    ):
        return "change_aggregation"
    return "general_modification"


def extract_requested_follow_up_dimension(text: str) -> str | None:
    """Extract the requested follow-up dimension from the user's instruction."""
    normalized_text = text.strip().lower()
    patterns = [
        r"split\s+by\s+([a-zA-Z_][a-zA-Z0-9_\s]*)",
        r"break\s+down\s+by\s+([a-zA-Z_][a-zA-Z0-9_\s]*)",
        r"group\s+by\s+([a-zA-Z_][a-zA-Z0-9_\s]*)",
        r"\bby\s+([a-zA-Z_][a-zA-Z0-9_\s]*)",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized_text)
        if not match:
            continue
        candidate = re.split(
            r"\b(with|for|in|across|between)\b",
            match.group(1),
            maxsplit=1,
        )[0]
        candidate = re.sub(r"[^a-zA-Z0-9_\s]", " ", candidate)
        normalized_candidate = "_".join(part for part in candidate.split() if part)
        if normalized_candidate:
            return normalized_candidate
    return None


def extract_metrics_from_sql(sql: str) -> list[str]:
    """Extract aggregate expressions from SQL for follow-up prompts."""
    select_clause = DashboardChatSqlGuard._extract_outer_select_clause(sql)
    if not select_clause:
        return []
    metrics: list[str] = []
    for expression in DashboardChatSqlGuard._split_select_expressions(select_clause):
        normalized_expression = expression.strip()
        if normalized_expression and DashboardChatSqlGuard._contains_aggregate(
            normalized_expression
        ):
            metrics.append(normalized_expression)
    return metrics[:5]


def extract_dimensions_from_sql(sql: str) -> list[str]:
    """Extract GROUP BY dimensions from SQL."""
    match = re.search(
        r"\bGROUP\s+BY\s+(.+?)(?:\bORDER\b|\bLIMIT\b|$)",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return []
    return [
        dimension.strip().strip('`"')
        for dimension in match.group(1).split(",")
        if dimension.strip()
    ][:5]


def extract_filters_from_sql(sql: str) -> list[str]:
    """Extract WHERE-clause filters from SQL."""
    match = re.search(
        r"\bWHERE\s+(.+?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|$)",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return []

    where_clause = match.group(1).strip()
    filters: list[str] = []
    for pattern in [
        r"([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*'([^']+)'",
        r"([a-zA-Z_][a-zA-Z0-9_]*)\s+IN\s*\([^)]+\)",
    ]:
        for filter_match in re.findall(pattern, where_clause, flags=re.IGNORECASE):
            if isinstance(filter_match, tuple) and len(filter_match) == 2:
                filters.append(f"{filter_match[0]} = {filter_match[1]}")
            else:
                filters.append(str(filter_match))
    return filters[:5]


def normalize_conversation_history(
    conversation_history: Sequence[DashboardChatConversationMessage | dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Normalize stored history into checkpoint-safe message payloads."""
    normalized_messages: list[dict[str, Any]] = []
    for item in conversation_history or []:
        if isinstance(item, DashboardChatConversationMessage):
            normalized_messages.append(
                {
                    "role": item.role,
                    "content": item.content,
                    "payload": item.payload or {},
                }
            )
            continue
        normalized_messages.append(
            {
                "role": str(item.get("role") or "user"),
                "content": str(item.get("content") or ""),
                "payload": item.get("payload") or {},
            }
        )
    return normalized_messages
