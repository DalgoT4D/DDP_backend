"""Finalize node for dashboard chat graph."""

from typing import Any

from ddpui.core.dashboard_chat.contracts import DashboardChatCitation, DashboardChatResponse

from ddpui.core.dashboard_chat.orchestration.state.payload_codec import serialize_response
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.state.accessors import (
    get_intent_decision,
    get_retrieved_documents,
    get_runtime_allowlist,
    get_runtime_response,
    get_sql_validation_result,
)


def finalize_node(state: DashboardChatGraphState) -> dict[str, Any]:
    """Attach warehouse citations and metadata to the finished response."""
    response = get_runtime_response(state)
    citations = list(response.citations)
    sql_validation = get_sql_validation_result(state)
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

    allowlist = get_runtime_allowlist(state)
    response_metadata = dict(response.metadata)
    response_metadata.update(
        {
            "dashboard_id": state["dashboard_id"],
            "retrieved_document_ids": [document.document_id for document in get_retrieved_documents(state)],
            "allowlisted_tables": sorted(allowlist.allowed_tables),
            "sql_guard_errors": sql_validation.errors if sql_validation is not None else [],
            "intent_reason": get_intent_decision(state).reason,
            "missing_info": get_intent_decision(state).missing_info,
            "follow_up_type": get_intent_decision(state).follow_up_context.follow_up_type,
        }
    )
    return {
        "response": serialize_response(
            DashboardChatResponse(
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
        )
    }
