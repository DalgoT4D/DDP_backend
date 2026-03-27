"""Serialization helpers for checkpoint-safe dashboard chat state payloads."""

from typing import Any

from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.contracts import (
    DashboardChatCitation,
    DashboardChatConversationContext,
    DashboardChatFollowUpContext,
    DashboardChatIntent,
    DashboardChatIntentDecision,
    DashboardChatResponse,
    DashboardChatRetrievedDocument,
    DashboardChatSchemaSnippet,
    DashboardChatSqlValidationResult,
)


def serialize_allowlist(allowlist: DashboardChatAllowlist) -> dict[str, Any]:
    """Convert an allowlist into a checkpoint-safe dictionary payload."""
    return {
        "chart_tables": sorted(allowlist.chart_tables),
        "upstream_tables": sorted(allowlist.upstream_tables),
        "allowed_tables": sorted(allowlist.allowed_tables),
        "allowed_unique_ids": sorted(allowlist.allowed_unique_ids),
        "unique_id_to_table": dict(allowlist.unique_id_to_table),
        "table_to_unique_ids": {
            table_name: sorted(unique_ids)
            for table_name, unique_ids in allowlist.table_to_unique_ids.items()
        },
    }


def deserialize_allowlist(payload: dict[str, Any] | None) -> DashboardChatAllowlist:
    """Rebuild an allowlist view from checkpoint-safe payload data."""
    payload = payload or {}
    return DashboardChatAllowlist(
        chart_tables=set(payload.get("chart_tables") or []),
        upstream_tables=set(payload.get("upstream_tables") or []),
        allowed_tables=set(payload.get("allowed_tables") or []),
        allowed_unique_ids=set(payload.get("allowed_unique_ids") or []),
        unique_id_to_table=dict(payload.get("unique_id_to_table") or {}),
        table_to_unique_ids={
            table_name: set(unique_ids)
            for table_name, unique_ids in (payload.get("table_to_unique_ids") or {}).items()
        },
    )


def serialize_schema_snippets(
    snippets: dict[str, DashboardChatSchemaSnippet],
) -> dict[str, Any]:
    """Convert schema snippets into checkpoint-safe dictionary payloads."""
    return {
        table_name: {
            "table_name": snippet.table_name,
            "columns": list(snippet.columns),
        }
        for table_name, snippet in snippets.items()
    }


def deserialize_schema_snippets(
    payload: dict[str, Any] | None,
) -> dict[str, DashboardChatSchemaSnippet]:
    """Rebuild schema snippet contracts from checkpoint payloads."""
    snippets: dict[str, DashboardChatSchemaSnippet] = {}
    for table_name, snippet_payload in (payload or {}).items():
        snippets[table_name.lower()] = DashboardChatSchemaSnippet(
            table_name=str(snippet_payload.get("table_name") or table_name),
            columns=list(snippet_payload.get("columns") or []),
        )
    return snippets


def serialize_distinct_payloads(
    validated_distinct_values: set[tuple[str, str, str]],
) -> dict[str, Any]:
    """Convert validated distinct values into a checkpoint-safe nested payload."""
    serialized: dict[str, dict[str, list[str]]] = {}
    for table_name, column_name, value in validated_distinct_values:
        serialized.setdefault(table_name, {}).setdefault(column_name, []).append(value)

    return {
        table_name: {
            column_name: sorted(set(values))
            for column_name, values in column_map.items()
        }
        for table_name, column_map in serialized.items()
    }


def deserialize_distinct_payloads(
    payload: dict[str, Any] | None,
) -> set[tuple[str, str, str]]:
    """Rebuild validated distinct values from checkpoint payloads."""
    validated_distinct_values: set[tuple[str, str, str]] = set()
    for table_name, column_map in (payload or {}).items():
        for column_name, values in (column_map or {}).items():
            for value in values or []:
                validated_distinct_values.add(
                    (str(table_name).lower(), str(column_name).lower(), str(value))
                )
    return validated_distinct_values


def serialize_conversation_context(
    context: DashboardChatConversationContext,
) -> dict[str, Any]:
    """Convert conversation context into a checkpoint-safe payload."""
    return {
        "last_sql_query": context.last_sql_query,
        "last_tables_used": list(context.last_tables_used),
        "last_chart_ids": list(context.last_chart_ids),
        "last_metrics": list(context.last_metrics),
        "last_dimensions": list(context.last_dimensions),
        "last_filters": list(context.last_filters),
        "last_response_type": context.last_response_type,
        "last_answer_text": context.last_answer_text,
        "last_intent": context.last_intent,
    }


def deserialize_conversation_context(
    payload: dict[str, Any] | None,
) -> DashboardChatConversationContext:
    """Rebuild conversation context from checkpoint payload data."""
    payload = payload or {}
    return DashboardChatConversationContext(
        last_sql_query=payload.get("last_sql_query"),
        last_tables_used=list(payload.get("last_tables_used") or []),
        last_chart_ids=list(payload.get("last_chart_ids") or []),
        last_metrics=list(payload.get("last_metrics") or []),
        last_dimensions=list(payload.get("last_dimensions") or []),
        last_filters=list(payload.get("last_filters") or []),
        last_response_type=payload.get("last_response_type"),
        last_answer_text=payload.get("last_answer_text"),
        last_intent=payload.get("last_intent"),
    )


def serialize_intent_decision(decision: DashboardChatIntentDecision) -> dict[str, Any]:
    """Convert one intent decision into a checkpoint-safe payload."""
    return {
        "intent": decision.intent.value,
        "confidence": decision.confidence,
        "reason": decision.reason,
        "missing_info": list(decision.missing_info),
        "force_tool_usage": decision.force_tool_usage,
        "clarification_question": decision.clarification_question,
        "follow_up_context": {
            "is_follow_up": decision.follow_up_context.is_follow_up,
            "follow_up_type": decision.follow_up_context.follow_up_type,
            "reusable_elements": dict(decision.follow_up_context.reusable_elements),
            "modification_instruction": decision.follow_up_context.modification_instruction,
        },
    }


def deserialize_intent_decision(payload: dict[str, Any] | None) -> DashboardChatIntentDecision:
    """Rebuild an intent decision from checkpoint payload data."""
    payload = payload or {}
    follow_up_payload = payload.get("follow_up_context") or {}
    return DashboardChatIntentDecision(
        intent=DashboardChatIntent(str(payload.get("intent") or DashboardChatIntent.IRRELEVANT.value)),
        confidence=float(payload.get("confidence") or 0.0),
        reason=str(payload.get("reason") or ""),
        missing_info=list(payload.get("missing_info") or []),
        force_tool_usage=bool(payload.get("force_tool_usage")),
        clarification_question=payload.get("clarification_question"),
        follow_up_context=DashboardChatFollowUpContext(
            is_follow_up=bool(follow_up_payload.get("is_follow_up")),
            follow_up_type=follow_up_payload.get("follow_up_type"),
            reusable_elements=dict(follow_up_payload.get("reusable_elements") or {}),
            modification_instruction=follow_up_payload.get("modification_instruction"),
        ),
    )


def serialize_retrieved_documents(
    documents: list[DashboardChatRetrievedDocument],
) -> list[dict[str, Any]]:
    """Convert retrieved document contracts into checkpoint-safe payloads."""
    return [
        {
            "document_id": document.document_id,
            "source_type": document.source_type,
            "source_identifier": document.source_identifier,
            "content": document.content,
            "dashboard_id": document.dashboard_id,
            "distance": document.distance,
        }
        for document in documents
    ]


def deserialize_retrieved_documents(
    payloads: list[dict[str, Any]] | None,
) -> list[DashboardChatRetrievedDocument]:
    """Rebuild retrieved document contracts from checkpoint payloads."""
    return [
        DashboardChatRetrievedDocument(
            document_id=str(payload.get("document_id") or ""),
            source_type=str(payload.get("source_type") or ""),
            source_identifier=str(payload.get("source_identifier") or ""),
            content=str(payload.get("content") or ""),
            dashboard_id=payload.get("dashboard_id"),
            distance=payload.get("distance"),
        )
        for payload in (payloads or [])
    ]


def serialize_citations(citations: list[DashboardChatCitation]) -> list[dict[str, Any]]:
    """Convert citations into checkpoint-safe payloads."""
    return [citation.to_dict() for citation in citations]


def deserialize_citations(
    payloads: list[dict[str, Any]] | None,
) -> list[DashboardChatCitation]:
    """Rebuild citation contracts from checkpoint payloads."""
    return [
        DashboardChatCitation(
            source_type=str(payload.get("source_type") or ""),
            source_identifier=str(payload.get("source_identifier") or ""),
            title=str(payload.get("title") or ""),
            snippet=str(payload.get("snippet") or ""),
            dashboard_id=payload.get("dashboard_id"),
            table_name=payload.get("table_name"),
        )
        for payload in (payloads or [])
    ]


def serialize_sql_validation_result(
    validation: DashboardChatSqlValidationResult | None,
) -> dict[str, Any] | None:
    """Convert SQL validation state into a checkpoint-safe payload."""
    if validation is None:
        return None
    return {
        "is_valid": validation.is_valid,
        "sanitized_sql": validation.sanitized_sql,
        "tables": list(validation.tables),
        "warnings": list(validation.warnings),
        "errors": list(validation.errors),
    }


def deserialize_sql_validation_result(
    payload: dict[str, Any] | None,
) -> DashboardChatSqlValidationResult | None:
    """Rebuild SQL validation state from checkpoint payload data."""
    if payload is None:
        return None
    return DashboardChatSqlValidationResult(
        is_valid=bool(payload.get("is_valid")),
        sanitized_sql=payload.get("sanitized_sql"),
        tables=list(payload.get("tables") or []),
        warnings=list(payload.get("warnings") or []),
        errors=list(payload.get("errors") or []),
    )


def serialize_response(response: DashboardChatResponse) -> dict[str, Any]:
    """Convert the final response contract into a checkpoint-safe payload."""
    return response.to_dict()


def deserialize_response(payload: dict[str, Any] | None) -> DashboardChatResponse:
    """Rebuild the final response contract from checkpoint payload data."""
    payload = payload or {}
    intent_value = str(payload.get("intent") or DashboardChatIntent.IRRELEVANT.value)
    return DashboardChatResponse(
        answer_text=str(payload.get("answer_text") or ""),
        intent=DashboardChatIntent(intent_value),
        citations=deserialize_citations(payload.get("citations") or []),
        warnings=list(payload.get("warnings") or []),
        sql=payload.get("sql"),
        sql_results=payload.get("sql_results"),
        usage=dict(payload.get("usage") or {}),
        tool_calls=list(payload.get("tool_calls") or []),
        metadata=dict(payload.get("metadata") or {}),
    )
