"""Compose-response node for dashboard chat graph."""

from typing import Any

from ddpui.core.dashboard_chat.contracts import DashboardChatResponse

from ddpui.core.dashboard_chat.orchestration.response_composer import (
    build_usage_summary,
    compose_final_answer_text,
    determine_response_format,
    sql_result_columns,
)
from ddpui.core.dashboard_chat.orchestration.retrieval_support import build_citations
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.state.accessors import (
    get_intent_decision,
    get_retrieved_documents,
    get_runtime_allowlist,
    get_runtime_response,
)
from ddpui.core.dashboard_chat.orchestration.state.payload_codec import (
    serialize_citations,
    serialize_response,
)


def compose_response_node(
    state: DashboardChatGraphState,
    llm_client,
    vector_store,
) -> dict[str, Any]:
    """Compose the final dashboard-chat response from state accumulated by prior nodes."""
    if state.get("response") is not None:
        response = get_runtime_response(state)
        return {
            "response": serialize_response(
                DashboardChatResponse(
                    answer_text=response.answer_text,
                    intent=response.intent,
                    citations=response.citations,
                    warnings=response.warnings,
                    sql=response.sql,
                    sql_results=response.sql_results,
                    usage=response.usage,
                    tool_calls=response.tool_calls,
                    metadata=response.metadata,
                )
            )
        }

    allowlist = get_runtime_allowlist(state)
    retrieved_documents = get_retrieved_documents(state)
    citations = build_citations(
        retrieved_documents=retrieved_documents,
        dashboard_export=state.get("dashboard_export_payload") or {},
        allowlist=allowlist,
    )
    response_format = determine_response_format(
        user_query=state["user_query"],
        sql_results=state.get("sql_results"),
    )
    execution_result = {
        "answer_text": state.get("draft_answer_text"),
        "retrieved_documents": retrieved_documents,
        "sql": state.get("sql"),
        "sql_results": state.get("sql_results"),
        "warnings": list(state.get("warnings") or []),
        "tool_calls": list(state.get("tool_calls") or []),
    }
    return {
        "citations": serialize_citations(citations),
        "response": serialize_response(
            DashboardChatResponse(
                answer_text=compose_final_answer_text(
                    llm_client,
                    state,
                    execution_result,
                    response_format=response_format,
                ),
                intent=get_intent_decision(state).intent,
                citations=citations,
                warnings=list(state.get("warnings") or []),
                sql=state.get("sql"),
                sql_results=state.get("sql_results"),
                usage=build_usage_summary(llm_client, vector_store),
                tool_calls=list(state.get("tool_calls") or []),
                metadata={
                    "response_format": response_format,
                    "table_columns": sql_result_columns(state.get("sql_results")),
                },
            )
        ),
    }
