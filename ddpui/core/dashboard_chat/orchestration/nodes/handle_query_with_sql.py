"""Node for SQL-backed new dashboard-chat questions."""

from typing import Any

from ddpui.core.dashboard_chat.orchestration.tool_loop_message_builder import (
    build_new_query_messages,
)
from ddpui.core.dashboard_chat.orchestration.nodes.metadata_gate import (
    metadata_artifact_is_ready,
    metadata_blocked_response,
)
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.llm_tools.runtime.tool_loop import execute_tool_loop
from ddpui.core.dashboard_chat.orchestration.timing_breakdown import merge_tool_loop_timing


def handle_query_with_sql_node(
    state: DashboardChatGraphState,
    llm_client,
    warehouse_tools_factory,
    runtime_config,
    tool_specifications,
) -> dict[str, Any]:
    """Handle new questions that are expected to produce SQL-backed answers."""
    if not metadata_artifact_is_ready(state):
        return metadata_blocked_response(state, llm_client)

    messages = build_new_query_messages(llm_client, state)

    execution_result = execute_tool_loop(
        llm_client,
        warehouse_tools_factory,
        runtime_config,
        tool_specifications,
        state=state,
        messages=messages,
        max_turns=15,
    )

    sql_validation = execution_result["sql_validation"]
    return {
        "retrieved_documents": [
            d.model_dump(mode="json") for d in execution_result["retrieved_documents"]
        ],
        "tool_calls": execution_result["tool_calls"],
        "draft_answer_text": execution_result["answer_text"],
        "sql": execution_result["sql"],
        "attempted_sql": execution_result["attempted_sql"],
        "last_sql_error": execution_result["last_sql_error"],
        "last_sql_error_reason": execution_result["last_sql_error_reason"],
        "attempted_answer_plan": execution_result["attempted_answer_plan"],
        "sql_validation": sql_validation.model_dump(mode="json")
        if sql_validation is not None
        else None,
        "sql_results": execution_result["sql_results"],
        "sql_rejection": execution_result.get("sql_rejection"),
        "pii_value_map": execution_result["pii_value_map"],
        "warnings": execution_result["warnings"],
        "timing_breakdown": merge_tool_loop_timing(state, execution_result),
        "schema_snippet_payloads": execution_result["schema_snippet_payloads"],
        "validated_distinct_payloads": execution_result["validated_distinct_payloads"],
    }
