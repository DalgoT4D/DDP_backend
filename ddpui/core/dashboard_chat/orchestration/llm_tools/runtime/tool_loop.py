"""Prototype-style tool-loop execution helpers for dashboard chat."""

import json
from time import perf_counter
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder

from ddpui.core.dashboard_chat.warehouse.warehouse_access_tools import (
    DashboardChatWarehouseToolsError,
)
from ddpui.utils.custom_logger import CustomLogger

from ddpui.core.dashboard_chat.orchestration.response_composer import (
    serialize_tool_result,
    summarize_tool_call,
    max_turns_message,
    fallback_answer_text,
)
from ddpui.core.dashboard_chat.contracts.event_contracts import DashboardChatProgressStage
from ddpui.core.dashboard_chat.contracts.intent_contracts import DashboardChatIntent, DashboardChatIntentDecision
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.metadata_tools import (
    handle_get_chart_table_metadata_tool,
    handle_get_column_metadata_tool,
    handle_get_join_paths_tool,
    handle_get_related_tables_tool,
    handle_get_table_metadata_tool,
    handle_get_table_statistics_tool,
    handle_read_full_metadata_tool,
    handle_resolve_time_scope_tool,
    handle_search_columns_by_name_tool,
    handle_search_metadata_tool,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.schema_tools import (
    handle_check_table_row_count_tool,
    handle_get_distinct_values_tool,
    handle_get_schema_snippets_tool,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_execution_tools import (
    handle_run_sql_query_tool,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_plan_validation import (
    handle_set_sql_query_plan_tool,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.runtime.turn_context import (
    DashboardChatTurnContext,
    current_validated_distinct_payloads,
    current_schema_snippet_payloads,
    seed_validated_distinct_values_from_previous_sql,
)
from ddpui.core.dashboard_chat.orchestration.runtime_signals import (
    publish_runtime_progress,
    raise_if_runtime_cancelled,
)

logger = CustomLogger("dashboard_chat")

TOOL_PROGRESS = {
    "get_chart_table_metadata": (
        DashboardChatProgressStage.SEARCHING_CONTEXT,
        "Inspecting chart tables",
    ),
    "search_metadata": (
        DashboardChatProgressStage.SEARCHING_CONTEXT,
        "Inspecting metadata",
    ),
    "get_table_metadata": (
        DashboardChatProgressStage.SEARCHING_CONTEXT,
        "Inspecting metadata",
    ),
    "get_column_metadata": (
        DashboardChatProgressStage.SEARCHING_CONTEXT,
        "Inspecting columns",
    ),
    "search_columns_by_name": (
        DashboardChatProgressStage.SEARCHING_CONTEXT,
        "Searching columns",
    ),
    "get_join_paths": (
        DashboardChatProgressStage.SEARCHING_CONTEXT,
        "Inspecting join paths",
    ),
    "get_related_tables": (
        DashboardChatProgressStage.SEARCHING_CONTEXT,
        "Inspecting related tables",
    ),
    "get_table_statistics": (
        DashboardChatProgressStage.SEARCHING_CONTEXT,
        "Inspecting table statistics",
    ),
    "resolve_time_scope": (
        DashboardChatProgressStage.SEARCHING_CONTEXT,
        "Resolving time scope",
    ),
    "read_full_metadata": (
        DashboardChatProgressStage.SEARCHING_CONTEXT,
        "Reading full metadata",
    ),
    "get_schema_snippets": (
        DashboardChatProgressStage.VALIDATING_QUERY,
        "Validating query",
    ),
    "get_distinct_values": (
        DashboardChatProgressStage.VALIDATING_QUERY,
        "Validating filters",
    ),
    "set_sql_query_plan": (
        DashboardChatProgressStage.VALIDATING_QUERY,
        "Planning query",
    ),
    "check_table_row_count": (
        DashboardChatProgressStage.VALIDATING_QUERY,
        "Validating query",
    ),
    "run_sql_query": (
        DashboardChatProgressStage.VALIDATING_QUERY,
        "Validating query",
    ),
}


def execute_tool_loop(
    llm_client,
    warehouse_tools_factory,
    runtime_config,
    tool_specifications,
    *,
    state: DashboardChatGraphState,
    messages: list[dict[str, Any]],
    max_turns: int,
) -> dict[str, Any]:
    """Execute the iterative dashboard-chat tool loop."""
    turn_context = DashboardChatTurnContext.from_state(state)
    tool_loop_started_at = perf_counter()
    seed_validated_distinct_values_from_previous_sql(state, turn_context)
    intent_decision = DashboardChatIntentDecision.model_validate(state.get("intent_decision") or {})

    for turn_index in range(max_turns):
        raise_if_runtime_cancelled()
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
            answer_text = (ai_message.get("content") or "").strip()
            if _should_force_sql_continuation(intent_decision, turn_context):
                turn_context.forced_sql_continuations += 1
                messages.append(
                    {
                        "role": "user",
                        "content": _build_forced_sql_continuation_message(turn_context),
                    }
                )
                continue
            if _is_sql_intent(intent_decision) and turn_context.last_sql_results is None:
                answer_text = _sql_terminal_failure_message(turn_context)
            if (
                turn_context.semantic_verifier_rejections
                and turn_context.last_sql_results is None
                and turn_context.last_sql is None
            ):
                answer_text = (
                    "I couldn't produce a validated SQL query for this question. "
                    "The generated SQL was rejected because it did not faithfully match "
                    "the requested measure, grain, filters, or output shape. "
                    "Please rephrase the question or make the expected metric and scope more explicit."
                )
            if ai_message.get("error") and (
                turn_context.last_sql_results is not None or turn_context.retrieved_documents
            ):
                answer_text = ""
            return build_tool_loop_result(
                answer_text=(
                    answer_text
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
            raise_if_runtime_cancelled()
            raw_args = tool_call.get("args") or {}
            args = raw_args
            if isinstance(raw_args, str):
                try:
                    args = json.loads(raw_args)
                except json.JSONDecodeError:
                    args = {}
            tool_started_at = perf_counter()
            result = execute_tool_call(
                llm_client,
                warehouse_tools_factory,
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
            raise_if_runtime_cancelled()
            if tool_name == "run_sql_query" and result.get("success"):
                return build_tool_loop_result(
                    answer_text=fallback_answer_text(
                        turn_context.retrieved_documents,
                        turn_context.last_sql_results,
                    ),
                    turn_context=turn_context,
                    max_turns_reached=False,
                    tool_loop_started_at=tool_loop_started_at,
                )

    answer_text = (
        _sql_terminal_failure_message(turn_context)
        if _is_sql_intent(intent_decision) and turn_context.last_sql_results is None
        else max_turns_message(
            state["user_query"],
            turn_context.retrieved_documents,
        )
    )
    return build_tool_loop_result(
        answer_text=answer_text,
        turn_context=turn_context,
        max_turns_reached=True,
        tool_loop_started_at=tool_loop_started_at,
    )


def execute_tool_call(
    llm_client,
    warehouse_tools_factory,
    runtime_config,
    *,
    tool_name: str,
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
) -> dict[str, Any]:
    """Execute one dashboard-chat tool against the runtime primitives."""
    try:
        progress = TOOL_PROGRESS.get(tool_name)
        if progress is not None:
            publish_runtime_progress(progress[1], progress[0])
        if tool_name == "get_chart_table_metadata":
            return handle_get_chart_table_metadata_tool(args, state, turn_context)
        if tool_name == "search_metadata":
            return handle_search_metadata_tool(args, state, turn_context)
        if tool_name == "get_table_metadata":
            return handle_get_table_metadata_tool(args, state, turn_context)
        if tool_name == "get_column_metadata":
            return handle_get_column_metadata_tool(args, state, turn_context)
        if tool_name == "search_columns_by_name":
            return handle_search_columns_by_name_tool(args, state, turn_context)
        if tool_name == "get_join_paths":
            return handle_get_join_paths_tool(args, state, turn_context)
        if tool_name == "get_table_statistics":
            return handle_get_table_statistics_tool(args, state, turn_context)
        if tool_name == "get_related_tables":
            return handle_get_related_tables_tool(args, state, turn_context)
        if tool_name == "resolve_time_scope":
            return handle_resolve_time_scope_tool(args, state, turn_context)
        if tool_name == "read_full_metadata":
            return handle_read_full_metadata_tool(args, state, turn_context)
        if tool_name == "get_schema_snippets":
            return handle_get_schema_snippets_tool(
                warehouse_tools_factory, args, state, turn_context
            )
        if tool_name == "get_distinct_values":
            return handle_get_distinct_values_tool(
                warehouse_tools_factory, args, state, turn_context
            )
        if tool_name == "set_sql_query_plan":
            return handle_set_sql_query_plan_tool(args, turn_context)
        if tool_name == "run_sql_query":
            return handle_run_sql_query_tool(
                llm_client, warehouse_tools_factory, runtime_config, args, state, turn_context
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
        "attempted_sql": turn_context.last_attempted_sql,
        "last_sql_error": turn_context.last_sql_error,
        "last_sql_error_reason": turn_context.last_sql_error_reason,
        "attempted_answer_plan": turn_context.last_attempted_answer_plan,
        "sql_validation": turn_context.last_sql_validation,
        "sql_results": turn_context.last_sql_results,
        "sql_rejection": {
            "count": turn_context.semantic_verifier_rejections,
            "reason": turn_context.last_sql_rejection_reason,
            "message": turn_context.last_sql_rejection_message,
        }
        if turn_context.semantic_verifier_rejections and turn_context.last_sql is None
        else None,
        "pii_value_map": dict(turn_context.pii_value_map),
        "warnings": warnings,
    }


def _should_force_sql_continuation(
    intent_decision: DashboardChatIntentDecision,
    turn_context: DashboardChatTurnContext,
) -> bool:
    """Prevent SQL-routed turns from ending before a query succeeds."""
    if not _is_sql_intent(intent_decision):
        return False
    if turn_context.last_sql_results is not None:
        return False
    if turn_context.forced_sql_continuations >= 2:
        return False
    return True


def _is_sql_intent(intent_decision: DashboardChatIntentDecision) -> bool:
    """Return whether this route must produce SQL-backed evidence."""
    return intent_decision.intent in {
        DashboardChatIntent.QUERY_WITH_SQL,
        DashboardChatIntent.FOLLOW_UP_SQL,
    }


def _build_forced_sql_continuation_message(
    turn_context: DashboardChatTurnContext,
) -> str:
    """Tell the model to repair or execute SQL instead of returning no-data text."""
    if turn_context.last_attempted_sql:
        error_context = (
            f" The previous SQL failed with {turn_context.last_sql_error}: "
            f"{turn_context.last_sql_error_reason}."
            if turn_context.last_sql_error
            else ""
        )
        return (
            "The routed intent requires a SQL-backed answer. Do not return an insufficient-data "
            "answer yet. Repair the previous SQL if needed, then call run_sql_query again."
            f"{error_context}"
        )
    return (
        "The routed intent requires a SQL-backed answer. Do not return an insufficient-data "
        "answer. Use the metadata/schema tools if needed, call set_sql_query_plan for complex "
        "metric, trend, ranking, threshold, or name-list questions, then call run_sql_query."
    )


def _sql_terminal_failure_message(turn_context: DashboardChatTurnContext) -> str:
    """Return a concrete SQL failure instead of a misleading insufficient-data answer."""
    if turn_context.last_attempted_sql:
        error = turn_context.last_sql_error or "sql_not_executed"
        reason = turn_context.last_sql_error_reason or "The SQL did not complete execution."
        return (
            "I could not complete the SQL-backed answer because the generated SQL did not pass "
            f"the runtime checks. Last error: {error}. {reason}"
        )
    return (
        "I could not complete the SQL-backed answer because no executable SQL query was produced "
        "within the tool loop."
    )


def _looks_like_premature_insufficient_data_answer(answer_text: str) -> bool:
    normalized = answer_text.lower()
    return any(
        phrase in normalized
        for phrase in [
            "couldn't find enough data",
            "could not find enough data",
            "couldn't find enough context",
            "could not find enough context",
            "not enough data",
            "not enough context",
            "insufficient data",
            "insufficient context",
        ]
    )
