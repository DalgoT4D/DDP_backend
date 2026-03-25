"""Prototype-style tool-loop execution helpers for dashboard chat."""

import json
import logging
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder

from ddpui.core.dashboard_chat.warehouse_tools import DashboardChatWarehouseToolsError

from .state import DashboardChatRuntimeState

logger = logging.getLogger(__name__)


def _execute_tool_loop(
    self,
    *,
    state: DashboardChatRuntimeState,
    messages: list[dict[str, Any]],
    max_turns: int,
    initial_embedding_cache: dict[str, list[float]] | None = None,
) -> dict[str, Any]:
    """Execute the prototype's iterative tool loop."""
    execution_context: dict[str, Any] = {
        "distinct_cache": set(state.get("session_distinct_cache") or set()),
        "embedding_cache": dict(initial_embedding_cache or {}),
        "schema_cache": dict(state.get("session_schema_cache") or {}),
        "retrieved_documents": [],
        "retrieved_document_ids": set(),
        "tool_calls": [],
        "warnings": list(state.get("warnings", [])),
        "warehouse_tools": None,
        "last_sql": None,
        "last_sql_results": None,
        "last_sql_validation": None,
    }
    self._seed_distinct_cache_from_previous_sql(state, execution_context)
    intent_decision = state["intent_decision"]

    for turn_index in range(max_turns):
        tool_choice = "required" if intent_decision.force_tool_usage and turn_index == 0 else "auto"
        ai_message = self.llm_client.run_tool_loop_turn(
            messages=messages,
            tools=self.TOOL_SPECIFICATIONS,
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
            return self._build_tool_loop_result(
                answer_text=(
                    (ai_message.get("content") or "").strip()
                    or self._fallback_answer_text(
                        execution_context["retrieved_documents"],
                        execution_context["last_sql_results"],
                    )
                ),
                execution_context=execution_context,
                max_turns_reached=False,
            )

        for tool_call in tool_calls:
            raw_args = tool_call.get("args") or {}
            args = raw_args
            if isinstance(raw_args, str):
                try:
                    args = json.loads(raw_args)
                except json.JSONDecodeError:
                    args = {}
            result = self._execute_tool_call(
                tool_name=str(tool_call.get("name") or ""),
                args=args,
                state=state,
                execution_context=execution_context,
            )
            execution_context["tool_calls"].append(
                self._summarize_tool_call(
                    tool_name=str(tool_call.get("name") or ""),
                    args=args,
                    result=result,
                )
            )
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call.get("id"),
                    "content": json.dumps(
                        self._serialize_tool_result(result),
                        cls=DjangoJSONEncoder,
                    ),
                }
            )
            if str(tool_call.get("name") or "") == "run_sql_query" and result.get("success"):
                return self._build_tool_loop_result(
                    answer_text="",
                    execution_context=execution_context,
                    max_turns_reached=False,
                )

    return self._build_tool_loop_result(
        answer_text=self._max_turns_message(
            state["user_query"],
            execution_context["retrieved_documents"],
        ),
        execution_context=execution_context,
        max_turns_reached=True,
    )


def _execute_tool_call(
    self,
    *,
    tool_name: str,
    args: dict[str, Any],
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
) -> dict[str, Any]:
    """Execute one prototype tool against the Dalgo runtime primitives."""
    try:
        if tool_name == "retrieve_docs":
            return self._handle_retrieve_docs_tool(args, state, execution_context)
        if tool_name == "get_schema_snippets":
            return self._handle_get_schema_snippets_tool(args, state, execution_context)
        if tool_name == "search_dbt_models":
            return self._handle_search_dbt_models_tool(args, state, execution_context)
        if tool_name == "get_dbt_model_info":
            return self._handle_get_dbt_model_info_tool(args, state, execution_context)
        if tool_name == "get_distinct_values":
            return self._handle_get_distinct_values_tool(args, state, execution_context)
        if tool_name == "run_sql_query":
            return self._run_sql_with_distinct_guard(args, state, execution_context)
        if tool_name == "list_tables_by_keyword":
            return self._handle_list_tables_by_keyword_tool(args, state, execution_context)
        if tool_name == "check_table_row_count":
            return self._handle_check_table_row_count_tool(args, state, execution_context)
        return {"error": f"Unknown tool: {tool_name}"}
    except DashboardChatWarehouseToolsError as error:
        logger.warning("Dashboard chat tool %s failed: %s", tool_name, error)
        execution_context["warnings"].append(str(error))
        return {"error": str(error)}
    except Exception as error:
        logger.exception("Dashboard chat tool %s failed", tool_name)
        execution_context["warnings"].append(str(error))
        return {"error": str(error)}


def _build_tool_loop_result(
    self,
    *,
    answer_text: str,
    execution_context: dict[str, Any],
    max_turns_reached: bool,
) -> dict[str, Any]:
    """Normalize tool-loop state into one runtime response payload."""
    if max_turns_reached:
        execution_context["tool_calls"].append({"name": "max_turns_reached"})
    warnings = list(dict.fromkeys(execution_context["warnings"]))
    return {
        "answer_text": answer_text.strip(),
        "retrieved_documents": execution_context["retrieved_documents"],
        "tool_calls": execution_context["tool_calls"],
        "sql": execution_context["last_sql"],
        "sql_validation": execution_context["last_sql_validation"],
        "sql_results": execution_context["last_sql_results"],
        "warnings": warnings,
    }
