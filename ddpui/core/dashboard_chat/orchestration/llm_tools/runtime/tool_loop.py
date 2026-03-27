"""Prototype-style tool-loop execution helpers for dashboard chat."""

import json
from time import perf_counter
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder

from ddpui.core.dashboard_chat.warehouse.warehouse_access_tools import DashboardChatWarehouseToolsError
from ddpui.utils.custom_logger import CustomLogger

from ddpui.core.dashboard_chat.orchestration.response_composer import (
    serialize_tool_result,
    summarize_tool_call,
    max_turns_message,
    fallback_answer_text,
)
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.state.accessors import get_intent_decision
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.dbt_tools import (
    handle_get_dbt_model_info_tool,
    handle_search_dbt_models_tool,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.vector_retrieval_tool import (
    handle_retrieve_docs_tool,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.schema_tools import (
    handle_check_table_row_count_tool,
    handle_get_distinct_values_tool,
    handle_get_schema_snippets_tool,
    handle_list_tables_by_keyword_tool,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_execution_tools import (
    handle_run_sql_query_tool,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.runtime.turn_context import (
    DashboardChatTurnContext,
    current_validated_distinct_payloads,
    current_schema_snippet_payloads,
    seed_validated_distinct_values_from_previous_sql,
)

logger = CustomLogger("dashboard_chat")


def execute_tool_loop(
    llm_client,
    warehouse_tools_factory,
    vector_store,
    source_config,
    runtime_config,
    tool_specifications,
    *,
    state: DashboardChatGraphState,
    messages: list[dict[str, Any]],
    max_turns: int,
    initial_query_embeddings: dict[str, list[float]] | None = None,
) -> dict[str, Any]:
    """Execute the prototype's iterative tool loop."""
    turn_context = DashboardChatTurnContext.from_state(
        state,
        initial_query_embeddings=initial_query_embeddings,
    )
    tool_loop_started_at = perf_counter()
    seed_validated_distinct_values_from_previous_sql(state, turn_context)
    intent_decision = get_intent_decision(state)

    for turn_index in range(max_turns):
        tool_choice = "required" if intent_decision.force_tool_usage and turn_index == 0 else "auto"
        ai_message = llm_client.run_tool_loop_turn(
            messages=messages,
            tools=tool_specifications,
            tool_choice=tool_choice,
            operation=f"tool_loop_{intent_decision.intent.value}",
        )
        tool_calls = ai_message.get("tool_calls") or []
        assistant_record: dict[str, Any] = {
            "role": "assistant",
            "content": ai_message.get("content", "") or "",
        }
        if tool_calls:
            assistant_record["tool_calls"] = [
                {
                    "id": tool_call.get("id"),
                    "type": "function",
                    "function": {
                        "name": tool_call.get("name"),
                        "arguments": (
                            tool_call.get("args")
                            if isinstance(tool_call.get("args"), str)
                            else json.dumps(tool_call.get("args") or {})
                        ),
                    },
                }
                for tool_call in tool_calls
            ]
        messages.append(assistant_record)

        if not tool_calls:
            return build_tool_loop_result(
                answer_text=(
                    (ai_message.get("content") or "").strip()
                    or fallback_answer_text(
                        turn_context.retrieved_documents,
                        turn_context.last_sql_results,
                    )
                ),
                turn_context=turn_context,
                max_turns_reached=False,
                tool_loop_started_at=tool_loop_started_at,
            )

        for tool_call in tool_calls:
            raw_args = tool_call.get("args") or {}
            args = raw_args
            if isinstance(raw_args, str):
                try:
                    args = json.loads(raw_args)
                except json.JSONDecodeError:
                    args = {}
            tool_started_at = perf_counter()
            result = execute_tool_call(
                warehouse_tools_factory,
                vector_store,
                source_config,
                runtime_config,
                tool_name=str(tool_call.get("name") or ""),
                args=args,
                state=state,
                turn_context=turn_context,
            )
            tool_duration_ms = round((perf_counter() - tool_started_at) * 1000, 2)
            tool_name = str(tool_call.get("name") or "")
            turn_context.timing_breakdown["tool_calls_ms"].append(
                {"name": tool_name, "duration_ms": tool_duration_ms}
            )
            turn_context.tool_calls.append(
                summarize_tool_call(
                    tool_name=tool_name,
                    args=args,
                    result=result,
                    duration_ms=tool_duration_ms,
                )
            )
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call.get("id"),
                    "content": json.dumps(
                        serialize_tool_result(result),
                        cls=DjangoJSONEncoder,
                    ),
                }
            )
            if tool_name == "run_sql_query" and result.get("success"):
                return build_tool_loop_result(
                    answer_text="",
                    turn_context=turn_context,
                    max_turns_reached=False,
                    tool_loop_started_at=tool_loop_started_at,
                )

    return build_tool_loop_result(
        answer_text=max_turns_message(
            state["user_query"],
            turn_context.retrieved_documents,
        ),
        turn_context=turn_context,
        max_turns_reached=True,
        tool_loop_started_at=tool_loop_started_at,
    )


def execute_tool_call(
    warehouse_tools_factory,
    vector_store,
    source_config,
    runtime_config,
    *,
    tool_name: str,
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
) -> dict[str, Any]:
    """Execute one prototype tool against the Dalgo runtime primitives."""
    try:
        if tool_name == "retrieve_docs":
            return handle_retrieve_docs_tool(
                vector_store, source_config, runtime_config, args, state, turn_context
            )
        if tool_name == "get_schema_snippets":
            return handle_get_schema_snippets_tool(
                warehouse_tools_factory, args, state, turn_context
            )
        if tool_name == "search_dbt_models":
            return handle_search_dbt_models_tool(args, state, turn_context)
        if tool_name == "get_dbt_model_info":
            return handle_get_dbt_model_info_tool(args, state, turn_context)
        if tool_name == "get_distinct_values":
            return handle_get_distinct_values_tool(
                warehouse_tools_factory, args, state, turn_context
            )
        if tool_name == "run_sql_query":
            return handle_run_sql_query_tool(
                warehouse_tools_factory, runtime_config, args, state, turn_context
            )
        if tool_name == "list_tables_by_keyword":
            return handle_list_tables_by_keyword_tool(
                warehouse_tools_factory, args, state, turn_context
            )
        if tool_name == "check_table_row_count":
            return handle_check_table_row_count_tool(
                warehouse_tools_factory, args, state, turn_context
            )
        return {"error": f"Unknown tool: {tool_name}"}
    except DashboardChatWarehouseToolsError as error:
        logger.warning("Dashboard chat tool %s failed: %s", tool_name, error)
        turn_context.warnings.append(str(error))
        return {"error": str(error)}
    except Exception as error:
        logger.exception("Dashboard chat tool %s failed", tool_name)
        turn_context.warnings.append(str(error))
        return {"error": str(error)}


def build_tool_loop_result(
    *,
    answer_text: str,
    turn_context: DashboardChatTurnContext,
    max_turns_reached: bool,
    tool_loop_started_at: float,
) -> dict[str, Any]:
    """Normalize tool-loop state into one runtime response payload."""
    if max_turns_reached:
        turn_context.tool_calls.append({"name": "max_turns_reached"})
    warnings = list(dict.fromkeys(turn_context.warnings))
    timing_breakdown = dict(turn_context.timing_breakdown)
    timing_breakdown["tool_loop_ms"] = round((perf_counter() - tool_loop_started_at) * 1000, 2)
    return {
        "answer_text": answer_text.strip(),
        "retrieved_documents": turn_context.retrieved_documents,
        "tool_calls": turn_context.tool_calls,
        "timing_breakdown": timing_breakdown,
        "schema_snippet_payloads": current_schema_snippet_payloads(turn_context),
        "validated_distinct_payloads": current_validated_distinct_payloads(turn_context),
        "sql": turn_context.last_sql,
        "sql_validation": turn_context.last_sql_validation,
        "sql_results": turn_context.last_sql_results,
        "warnings": warnings,
    }
