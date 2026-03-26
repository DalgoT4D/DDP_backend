"""Finalize node for dashboard chat graph."""

from typing import Any

from ddpui.core.dashboard_chat.context.allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.contracts import DashboardChatCitation, DashboardChatResponse

from ..state import DashboardChatRuntimeState


def finalize_node(state: DashboardChatRuntimeState) -> dict[str, Any]:
    """Attach warehouse citations and metadata to the finished response."""
    response = state["response"]
    citations = list(response.citations)
    sql_validation = state.get("sql_validation")
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

    allowlist = state.get("allowlist") or DashboardChatAllowlist()
    response_metadata = dict(response.metadata)
    response_metadata.update(
        {
            "dashboard_id": state["dashboard_id"],
            "retrieved_document_ids": [
                document.document_id for document in state.get("retrieved_documents") or []
            ],
            "allowlisted_tables": sorted(allowlist.allowed_tables),
            "sql_guard_errors": sql_validation.errors if sql_validation is not None else [],
            "intent_reason": state["intent_decision"].reason,
            "missing_info": state["intent_decision"].missing_info,
            "follow_up_type": state["intent_decision"].follow_up_context.follow_up_type,
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
        )
    }
