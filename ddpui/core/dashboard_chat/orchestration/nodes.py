"""LangGraph node handlers for dashboard chat."""

from ddpui.core.dashboard_chat.context.allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.contracts import (
    DashboardChatCitation,
    DashboardChatIntent,
    DashboardChatResponse,
)

from .state import DashboardChatRuntimeState


def _node_load_context(self, state: DashboardChatRuntimeState) -> DashboardChatRuntimeState:
    """Load or reuse the session-stable dashboard context snapshot."""
    snapshot = self._load_session_snapshot(state)
    state["dashboard_export"] = snapshot["dashboard_export"]
    state["dbt_index"] = snapshot["dbt_index"]
    state["allowlist"] = snapshot["allowlist"]
    state["session_schema_cache"] = snapshot["schema_cache"]
    state["session_distinct_cache"] = snapshot["distinct_cache"]
    return state


def _node_route_intent(self, state: DashboardChatRuntimeState) -> DashboardChatRuntimeState:
    """Use the prototype router prompt for all non-trivial routing."""
    conversation_context = self._extract_conversation_context(state["conversation_history"])
    fast_path_intent = self._build_fast_path_intent(state["user_query"])
    if fast_path_intent is not None:
        state["conversation_context"] = conversation_context
        state["intent_decision"] = fast_path_intent
        state["small_talk_response"] = self._build_fast_path_small_talk_response(
            state["user_query"]
        )
        return state
    intent_decision = self.llm_client.classify_intent(
        user_query=state["user_query"],
        conversation_context=conversation_context,
    )
    state["conversation_context"] = conversation_context
    state["intent_decision"] = intent_decision
    return state


def _node_handle_small_talk(
    self,
    state: DashboardChatRuntimeState,
) -> DashboardChatRuntimeState:
    """Handle simple social turns without any tool use."""
    state["response"] = DashboardChatResponse(
        answer_text=state.get("small_talk_response")
        or self._compose_small_talk_response(state["user_query"]),
        intent=DashboardChatIntent.SMALL_TALK,
        usage=self._build_usage_summary(),
    )
    return state


def _node_handle_irrelevant(
    self,
    state: DashboardChatRuntimeState,
) -> DashboardChatRuntimeState:
    """Handle questions outside dashboard chat scope."""
    state["response"] = DashboardChatResponse(
        answer_text=(
            "I can only answer questions about this dashboard, its charts, and the data behind them."
        ),
        intent=DashboardChatIntent.IRRELEVANT,
        usage=self._build_usage_summary(),
    )
    return state


def _node_handle_needs_clarification(
    self,
    state: DashboardChatRuntimeState,
) -> DashboardChatRuntimeState:
    """Ask for clarification when the router says the query is underspecified."""
    intent_decision = state["intent_decision"]
    state["response"] = DashboardChatResponse(
        answer_text=(
            intent_decision.clarification_question
            or self._clarification_fallback(intent_decision.missing_info)
        ),
        intent=DashboardChatIntent.NEEDS_CLARIFICATION,
        usage=self._build_usage_summary(),
    )
    return state


def _node_handle_query_with_sql(
    self,
    state: DashboardChatRuntimeState,
) -> DashboardChatRuntimeState:
    """Run the prototype new-query tool loop for SQL-routed questions."""
    return self._run_intent_tool_loop(state, max_turns=15, follow_up=False)


def _node_handle_query_without_sql(
    self,
    state: DashboardChatRuntimeState,
) -> DashboardChatRuntimeState:
    """Run the prototype new-query tool loop for context-only questions."""
    return self._run_intent_tool_loop(state, max_turns=15, follow_up=False)


def _node_handle_follow_up_sql(
    self,
    state: DashboardChatRuntimeState,
) -> DashboardChatRuntimeState:
    """Run the prototype follow-up loop for SQL-modifying turns."""
    return self._run_intent_tool_loop(state, max_turns=6, follow_up=True)


def _node_handle_follow_up_context(
    self,
    state: DashboardChatRuntimeState,
) -> DashboardChatRuntimeState:
    """Run the prototype follow-up loop for explanatory follow-ups."""
    return self._run_intent_tool_loop(state, max_turns=6, follow_up=True)


def _run_intent_tool_loop(
    self,
    state: DashboardChatRuntimeState,
    *,
    max_turns: int,
    follow_up: bool,
) -> DashboardChatRuntimeState:
    """Execute one prototype-style tool loop and store the response on state."""
    allowlist = state["allowlist"]

    query_embedding = self._get_cached_query_embedding(
        state["user_query"],
        embedding_cache={},
    )

    messages = (
        self._build_follow_up_messages(state)
        if follow_up
        else self._build_new_query_messages(state)
    )
    execution_result = self._execute_tool_loop(
        state=state,
        messages=messages,
        max_turns=max_turns,
        initial_embedding_cache={state["user_query"]: query_embedding},
    )

    state["retrieved_documents"] = execution_result["retrieved_documents"]
    state["citations"] = self._build_citations(
        retrieved_documents=execution_result["retrieved_documents"],
        dashboard_export=state["dashboard_export"],
        allowlist=allowlist,
    )
    state["tool_calls"] = execution_result["tool_calls"]
    existing_timing_breakdown = dict(state.get("timing_breakdown") or {})
    execution_timing_breakdown = dict(execution_result.get("timing_breakdown") or {})
    merged_timing_breakdown = dict(existing_timing_breakdown)
    if "graph_nodes_ms" in existing_timing_breakdown or "graph_nodes_ms" in execution_timing_breakdown:
        merged_timing_breakdown["graph_nodes_ms"] = {
            **dict(existing_timing_breakdown.get("graph_nodes_ms") or {}),
            **dict(execution_timing_breakdown.get("graph_nodes_ms") or {}),
        }
    if "tool_calls_ms" in existing_timing_breakdown or "tool_calls_ms" in execution_timing_breakdown:
        merged_timing_breakdown["tool_calls_ms"] = list(
            execution_timing_breakdown.get("tool_calls_ms")
            or existing_timing_breakdown.get("tool_calls_ms")
            or []
        )
    for key, value in execution_timing_breakdown.items():
        if key not in {"graph_nodes_ms", "tool_calls_ms"}:
            merged_timing_breakdown[key] = value
    state["timing_breakdown"] = merged_timing_breakdown
    state["sql"] = execution_result["sql"]
    state["sql_validation"] = execution_result["sql_validation"]
    state["sql_results"] = execution_result["sql_results"]
    state["warnings"] = execution_result["warnings"]
    response_format = self._determine_response_format(
        user_query=state["user_query"],
        sql_results=execution_result["sql_results"],
    )
    state["response"] = DashboardChatResponse(
        answer_text=self._compose_final_answer_text(
            state,
            execution_result,
            response_format=response_format,
        ),
        intent=state["intent_decision"].intent,
        citations=state["citations"],
        warnings=execution_result["warnings"],
        sql=execution_result["sql"],
        sql_results=execution_result["sql_results"],
        usage=self._build_usage_summary(),
        tool_calls=execution_result["tool_calls"],
        metadata={
            "response_format": response_format,
            "table_columns": self._sql_result_columns(execution_result["sql_results"]),
        },
    )
    return state


def _node_finalize_response(
    self,
    state: DashboardChatRuntimeState,
) -> DashboardChatRuntimeState:
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
    state["response"] = DashboardChatResponse(
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
    return state


def _route_after_intent(state: DashboardChatRuntimeState) -> str:
    """Route to one explicit handler per prototype intent."""
    return state["intent_decision"].intent.value
