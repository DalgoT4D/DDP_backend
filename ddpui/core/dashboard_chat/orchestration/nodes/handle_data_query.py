"""Handle-data-query node for dashboard chat graph (covers query_with_sql and query_without_sql)."""

from typing import Any

from ddpui.core.dashboard_chat.contracts import DashboardChatResponse

from ..message_stack import build_new_query_messages
from ..presentation import (
    build_usage_summary,
    compose_final_answer_text,
    determine_response_format,
    sql_result_columns,
)
from ..retrieval import build_citations, get_cached_query_embedding
from ..state import DashboardChatRuntimeState
from ..tool_loop import execute_tool_loop
from .helpers import merge_tool_loop_timing


def handle_data_query_node(
    state: DashboardChatRuntimeState,
    llm_client,
    vector_store,
    warehouse_tools_factory,
    runtime_config,
    source_config,
    tool_specifications,
) -> dict[str, Any]:
    """Execute the new-query tool loop for SQL and context-only questions."""
    allowlist = state["allowlist"]
    query_embedding = get_cached_query_embedding(
        vector_store, state["user_query"], embedding_cache={}
    )
    messages = build_new_query_messages(llm_client, state)

    execution_result = execute_tool_loop(
        llm_client,
        warehouse_tools_factory,
        vector_store,
        source_config,
        runtime_config,
        tool_specifications,
        state=state,
        messages=messages,
        max_turns=15,
        initial_embedding_cache={state["user_query"]: query_embedding},
    )

    citations = build_citations(
        retrieved_documents=execution_result["retrieved_documents"],
        dashboard_export=state["dashboard_export"],
        allowlist=allowlist,
    )
    response_format = determine_response_format(
        user_query=state["user_query"],
        sql_results=execution_result["sql_results"],
    )

    return {
        "retrieved_documents": execution_result["retrieved_documents"],
        "citations": citations,
        "tool_calls": execution_result["tool_calls"],
        "sql": execution_result["sql"],
        "sql_validation": execution_result["sql_validation"],
        "sql_results": execution_result["sql_results"],
        "warnings": execution_result["warnings"],
        "timing_breakdown": merge_tool_loop_timing(state, execution_result),
        "response": DashboardChatResponse(
            answer_text=compose_final_answer_text(
                llm_client, state, execution_result, response_format=response_format
            ),
            intent=state["intent_decision"].intent,
            citations=citations,
            warnings=execution_result["warnings"],
            sql=execution_result["sql"],
            sql_results=execution_result["sql_results"],
            usage=build_usage_summary(llm_client, vector_store),
            tool_calls=execution_result["tool_calls"],
            metadata={
                "response_format": response_format,
                "table_columns": sql_result_columns(execution_result["sql_results"]),
            },
        ),
    }
