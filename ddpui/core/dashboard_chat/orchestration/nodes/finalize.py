"""Finalize node for dashboard chat graph."""

from typing import Any

from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.contracts import (
    DashboardChatCitation,
    DashboardChatIntentDecision,
    DashboardChatResponse,
    DashboardChatRetrievedDocument,
    DashboardChatSqlValidationResult,
)
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState


def finalize_node(state: DashboardChatGraphState) -> dict[str, Any]:
    """Attach warehouse citations and metadata to the finished response."""
    response = DashboardChatResponse.model_validate(state.get("response") or {})
    citations = list(response.citations)

    sql_validation_payload = state.get("sql_validation")
    sql_validation = (
        DashboardChatSqlValidationResult.model_validate(sql_validation_payload)
        if sql_validation_payload is not None
        else None
    )
    if (
        sql_validation is not None
        and sql_validation.is_valid
        and sql_validation.sanitized_sql is not None
    ):
        citations.extend(
            DashboardChatCitation(
                source_type="warehouse_table",
                source_identifier=table_name,
                title=f"Warehouse table: {table_name}",
                snippet=f"SQL executed against {table_name}.",
                table_name=table_name,
            )
            for table_name in sql_validation.tables
            if table_name
        )

    allowlist = DashboardChatAllowlist.model_validate(state.get("allowlist_payload") or {})
    intent_decision = DashboardChatIntentDecision.model_validate(state.get("intent_decision") or {})
    retrieved_documents = [
        DashboardChatRetrievedDocument.model_validate(p)
        for p in (state.get("retrieved_documents") or [])
    ]

    response_metadata = dict(response.metadata)
    response_metadata.update(
        {
            "dashboard_id": state["dashboard_id"],
            "retrieved_document_ids": [doc.document_id for doc in retrieved_documents],
            "allowlisted_tables": sorted(allowlist.allowed_tables),
            "sql_guard_errors": sql_validation.errors if sql_validation is not None else [],
            "intent_reason": intent_decision.reason,
            "missing_info": intent_decision.missing_info,
            "follow_up_type": intent_decision.follow_up_context.follow_up_type,
        }
    )
    return {
        "response": DashboardChatResponse(
            answer_text=response.answer_text,
            intent=response.intent,
            citations=list(dict.fromkeys(citations)),
            warnings=response.warnings,
            sql=response.sql,
            sql_results=response.sql_results,
            usage=response.usage,
            tool_calls=response.tool_calls,
            metadata=response_metadata,
        ).to_dict()
    }
