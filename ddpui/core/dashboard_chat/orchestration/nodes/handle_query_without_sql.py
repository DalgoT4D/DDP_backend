"""Node for context-only new dashboard-chat questions."""

from typing import Any

from ddpui.core.dashboard_chat.orchestration.tool_loop_message_builder import build_new_query_messages
from ddpui.core.dashboard_chat.orchestration.retrieval_support import get_or_embed_query
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.state.payload_codec import (
    serialize_retrieved_documents,
    serialize_sql_validation_result,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.runtime.tool_loop import execute_tool_loop
from ddpui.core.dashboard_chat.orchestration.timing_breakdown import merge_tool_loop_timing


def handle_query_without_sql_node(
    state: DashboardChatGraphState,
    llm_client,
    vector_store,
    warehouse_tools_factory,
    runtime_config,
    source_config,
    tool_specifications,
) -> dict[str, Any]:
    """Handle new questions that continue with retrieved context but no SQL requirement."""
    query_embedding = get_or_embed_query(
        vector_store, state["user_query"], query_embeddings={}
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
        initial_query_embeddings={state["user_query"]: query_embedding},
    )

    return {
        "retrieved_documents": serialize_retrieved_documents(
            execution_result["retrieved_documents"]
        ),
        "tool_calls": execution_result["tool_calls"],
        "draft_answer_text": execution_result["answer_text"],
        "sql": execution_result["sql"],
        "sql_validation": serialize_sql_validation_result(execution_result["sql_validation"]),
        "sql_results": execution_result["sql_results"],
        "warnings": execution_result["warnings"],
        "timing_breakdown": merge_tool_loop_timing(state, execution_result),
        "schema_snippet_payloads": execution_result["schema_snippet_payloads"],
        "validated_distinct_payloads": execution_result["validated_distinct_payloads"],
    }
