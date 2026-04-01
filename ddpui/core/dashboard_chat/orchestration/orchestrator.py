"""Main dashboard chat LangGraph orchestrator."""

from collections.abc import Callable, Sequence
from functools import lru_cache
from time import perf_counter
from typing import Any, Union

from langgraph.graph import END, START, StateGraph

from ddpui.core.dashboard_chat.config import DashboardChatRuntimeConfig, DashboardChatSourceConfig
from ddpui.core.dashboard_chat.agents.llm_client_interface import DashboardChatLlmClient
from ddpui.core.dashboard_chat.agents.openai_llm_client import OpenAIDashboardChatLlmClient
from ddpui.core.dashboard_chat.contracts import DashboardChatResponse
from ddpui.core.dashboard_chat.vector.org_vector_store import OrgVectorStore
from ddpui.core.dashboard_chat.warehouse.warehouse_access_tools import DashboardChatWarehouseTools
from ddpui.models.org import Org

from ddpui.core.dashboard_chat.orchestration.conversation_context import (
    normalize_conversation_history,
)
from ddpui.core.dashboard_chat.orchestration.checkpoints import get_dashboard_chat_checkpointer
from ddpui.core.dashboard_chat.orchestration.nodes.compose_response import compose_response_node
from ddpui.core.dashboard_chat.orchestration.nodes.finalize import finalize_node
from ddpui.core.dashboard_chat.orchestration.nodes.handle_query_with_sql import (
    handle_query_with_sql_node,
)
from ddpui.core.dashboard_chat.orchestration.nodes.handle_query_without_sql import (
    handle_query_without_sql_node,
)
from ddpui.core.dashboard_chat.orchestration.nodes.handle_follow_up_context import (
    handle_follow_up_context_node,
)
from ddpui.core.dashboard_chat.orchestration.nodes.handle_follow_up_sql import (
    handle_follow_up_sql_node,
)
from ddpui.core.dashboard_chat.orchestration.nodes.handle_irrelevant import handle_irrelevant_node
from ddpui.core.dashboard_chat.orchestration.nodes.handle_needs_clarification import (
    handle_needs_clarification_node,
)
from ddpui.core.dashboard_chat.orchestration.nodes.handle_small_talk import handle_small_talk_node
from ddpui.core.dashboard_chat.orchestration.intent_routing import route_after_intent
from ddpui.core.dashboard_chat.orchestration.nodes.load_context import load_context_node
from ddpui.core.dashboard_chat.orchestration.nodes.route_intent import route_intent_node
from ddpui.core.dashboard_chat.orchestration.state import (
    DashboardChatGraphState,
)

from ddpui.core.dashboard_chat.orchestration.llm_tools.runtime.tool_specifications import (
    DASHBOARD_CHAT_TOOL_SPECIFICATIONS,
)


def _timed_node(node_name: str, handler):
    """Wrap one graph node so per-node duration is recorded in timing_breakdown."""

    def wrapped(state: DashboardChatGraphState) -> dict:
        started_at = perf_counter()
        updates = handler(state)
        elapsed_ms = round((perf_counter() - started_at) * 1000, 2)
        existing_timing = dict(state.get("timing_breakdown") or {})
        new_timing = dict(updates.get("timing_breakdown") or existing_timing)
        graph_nodes_ms = dict(new_timing.get("graph_nodes_ms") or {})
        graph_nodes_ms[node_name] = elapsed_ms
        new_timing["graph_nodes_ms"] = graph_nodes_ms
        updates["timing_breakdown"] = new_timing
        return updates

    return wrapped


def _build_graph(
    llm_client,
    vector_store,
    warehouse_tools_factory,
    runtime_config,
    source_config,
    tool_specifications,
    *,
    checkpointer=None,
):
    """Build the intent-routing graph with all deps injected via closures."""

    def _load_context(state):
        return load_context_node(state)

    def _route_intent(state):
        return route_intent_node(state, llm_client)

    def _handle_small_talk(state):
        return handle_small_talk_node(state, llm_client, vector_store)

    def _handle_irrelevant(state):
        return handle_irrelevant_node(state, llm_client, vector_store)

    def _handle_needs_clarification(state):
        return handle_needs_clarification_node(state, llm_client, vector_store)

    def _handle_query_with_sql(state):
        return handle_query_with_sql_node(
            state,
            llm_client,
            vector_store,
            warehouse_tools_factory,
            runtime_config,
            source_config,
            tool_specifications,
        )

    def _handle_query_without_sql(state):
        return handle_query_without_sql_node(
            state,
            llm_client,
            vector_store,
            warehouse_tools_factory,
            runtime_config,
            source_config,
            tool_specifications,
        )

    def _handle_follow_up_sql(state):
        return handle_follow_up_sql_node(
            state,
            llm_client,
            vector_store,
            warehouse_tools_factory,
            runtime_config,
            source_config,
            tool_specifications,
        )

    def _handle_follow_up_context(state):
        return handle_follow_up_context_node(
            state,
            llm_client,
            vector_store,
            warehouse_tools_factory,
            runtime_config,
            source_config,
            tool_specifications,
        )

    def _finalize(state):
        return finalize_node(state)

    def _compose_response(state):
        return compose_response_node(state, llm_client, vector_store)

    graph = StateGraph(DashboardChatGraphState)
    graph.add_node("load_context", _timed_node("load_context", _load_context))
    graph.add_node("route_intent", _timed_node("route_intent", _route_intent))
    graph.add_node("handle_small_talk", _timed_node("handle_small_talk", _handle_small_talk))
    graph.add_node("handle_irrelevant", _timed_node("handle_irrelevant", _handle_irrelevant))
    graph.add_node(
        "handle_needs_clarification",
        _timed_node("handle_needs_clarification", _handle_needs_clarification),
    )
    graph.add_node(
        "handle_query_with_sql",
        _timed_node("handle_query_with_sql", _handle_query_with_sql),
    )
    graph.add_node(
        "handle_query_without_sql",
        _timed_node("handle_query_without_sql", _handle_query_without_sql),
    )
    graph.add_node(
        "handle_follow_up_sql",
        _timed_node("handle_follow_up_sql", _handle_follow_up_sql),
    )
    graph.add_node(
        "handle_follow_up_context",
        _timed_node("handle_follow_up_context", _handle_follow_up_context),
    )
    graph.add_node("compose_response", _timed_node("compose_response", _compose_response))
    graph.add_node("finalize", _timed_node("finalize", _finalize))

    graph.add_edge(START, "load_context")
    graph.add_edge("load_context", "route_intent")
    graph.add_conditional_edges(
        "route_intent",
        route_after_intent,
        {
            "small_talk": "handle_small_talk",
            "irrelevant": "handle_irrelevant",
            "needs_clarification": "handle_needs_clarification",
            "query_with_sql": "handle_query_with_sql",
            "query_without_sql": "handle_query_without_sql",
            "follow_up_sql": "handle_follow_up_sql",
            "follow_up_context": "handle_follow_up_context",
        },
    )
    graph.add_edge("handle_small_talk", "compose_response")
    graph.add_edge("handle_irrelevant", "compose_response")
    graph.add_edge("handle_needs_clarification", "compose_response")
    graph.add_edge("handle_query_with_sql", "compose_response")
    graph.add_edge("handle_query_without_sql", "compose_response")
    graph.add_edge("handle_follow_up_sql", "compose_response")
    graph.add_edge("handle_follow_up_context", "compose_response")
    graph.add_edge("compose_response", "finalize")
    graph.add_edge("finalize", END)
    if checkpointer is not None:
        return graph.compile(checkpointer=checkpointer)
    return graph.compile()


class DashboardChatRuntime:
    """Run dashboard chat turns with the prototype's explicit intent routing and tool loop."""

    def __init__(
        self,
        vector_store: Union[OrgVectorStore, None] = None,
        llm_client: Union[DashboardChatLlmClient, None] = None,
        warehouse_tools_factory: Union[Callable[[Org], DashboardChatWarehouseTools], None] = None,
        runtime_config: Union[DashboardChatRuntimeConfig, None] = None,
        source_config: Union[DashboardChatSourceConfig, None] = None,
        checkpointer=None,
    ):
        self.runtime_config = runtime_config or DashboardChatRuntimeConfig.from_env()
        self.source_config = source_config or DashboardChatSourceConfig.from_env()
        self.vector_store = vector_store or OrgVectorStore()
        self.llm_client = llm_client or OpenAIDashboardChatLlmClient(
            model=self.runtime_config.llm_model,
            timeout_ms=self.runtime_config.llm_timeout_ms,
            max_attempts=self.runtime_config.llm_max_attempts,
        )
        self.warehouse_tools_factory = warehouse_tools_factory or (
            lambda org: DashboardChatWarehouseTools(
                org=org,
                max_rows=self.runtime_config.max_query_rows,
            )
        )
        self.graph = _build_graph(
            self.llm_client,
            self.vector_store,
            self.warehouse_tools_factory,
            self.runtime_config,
            self.source_config,
            DASHBOARD_CHAT_TOOL_SPECIFICATIONS,
        )
        self.persistent_graph = (
            _build_graph(
                self.llm_client,
                self.vector_store,
                self.warehouse_tools_factory,
                self.runtime_config,
                self.source_config,
                DASHBOARD_CHAT_TOOL_SPECIFICATIONS,
                checkpointer=checkpointer,
            )
            if checkpointer is not None
            else None
        )

    @staticmethod
    def _thread_config(
        session_id: str,
        checkpoint_id: str | None = None,
    ) -> dict[str, Any]:
        """Build the top-level LangGraph thread config for one dashboard-chat session.

        LangGraph uses ``checkpoint_ns`` for graph/subgraph namespaces. Dashboard chat runs
        as one top-level graph, so we keep that namespace empty and scope persistence
        through ``thread_id=session_id``.
        """
        configurable: dict[str, Any] = {
            "thread_id": session_id,
            "checkpoint_ns": "",
        }
        if checkpoint_id:
            configurable["checkpoint_id"] = checkpoint_id
        return {"configurable": configurable}

    def _persistent_graph_or_raise(self):
        """Return the persistent graph or fail loudly if checkpointing is unavailable."""
        if self.persistent_graph is None:
            raise RuntimeError("Dashboard chat runtime was not initialized with checkpointing")
        return self.persistent_graph

    def get_state_snapshot(
        self,
        session_id: str,
        checkpoint_id: str | None = None,
    ):
        """Return the current persisted LangGraph state snapshot for one chat session."""
        try:
            return self._persistent_graph_or_raise().get_state(
                self._thread_config(session_id, checkpoint_id)
            )
        except ValueError:
            return None

    def run(
        self,
        org: Org,
        dashboard_id: int,
        user_query: str,
        session_id: str | None = None,
        vector_collection_name: str | None = None,
        conversation_history: Sequence[dict[str, Any]] | None = None,
        interrupt_before: Sequence[str] | None = None,
        interrupt_after: Sequence[str] | None = None,
    ) -> DashboardChatResponse:
        """Run one dashboard chat turn."""
        if hasattr(self.llm_client, "reset_usage"):
            self.llm_client.reset_usage()
        if hasattr(self.vector_store, "reset_usage"):
            self.vector_store.reset_usage()
        initial_state: DashboardChatGraphState = {
            "org_id": org.id,
            "dashboard_id": dashboard_id,
            "session_id": session_id,
            "vector_collection_name": vector_collection_name,
            "user_query": user_query,
            "conversation_history": normalize_conversation_history(conversation_history),
            "small_talk_response": None,
            "retrieved_documents": [],
            "citations": [],
            "tool_calls": [],
            "sql": None,
            "sql_validation": None,
            "sql_results": None,
            "timing_breakdown": {
                "graph_nodes_ms": {},
                "tool_calls_ms": [],
            },
            "draft_answer_text": None,
            "warnings": [],
            "usage": {},
            # The checkpointed graph persists terminal response state across turns.
            # Each new user turn must clear that field so compose_response rebuilds
            # a fresh answer instead of short-circuiting on the previous turn's reply.
            "response": None,
        }
        runtime_started_at = perf_counter()
        invocation_config = None
        graph = self.graph
        if session_id and self.persistent_graph is not None:
            graph = self.persistent_graph
            invocation_config = self._thread_config(session_id)
        final_state = graph.invoke(
            initial_state,
            config=invocation_config,
            interrupt_before=interrupt_before,
            interrupt_after=interrupt_after,
        )
        if final_state.get("response") is None:
            raise RuntimeError(
                "Dashboard chat run was interrupted before a final response was produced. "
                "Resume the session from its persisted checkpoint."
            )
        runtime_total_ms = round((perf_counter() - runtime_started_at) * 1000, 2)
        response = DashboardChatResponse.model_validate(final_state.get("response") or {})
        timing_breakdown = dict(final_state.get("timing_breakdown") or {})
        timing_breakdown["runtime_total_ms"] = runtime_total_ms
        response_metadata = dict(response.metadata)
        response_metadata["timing_breakdown"] = timing_breakdown
        return DashboardChatResponse(
            answer_text=response.answer_text,
            intent=response.intent,
            citations=response.citations,
            warnings=response.warnings,
            sql=response.sql,
            sql_results=response.sql_results,
            usage=response.usage,
            tool_calls=response.tool_calls,
            metadata=response_metadata,
        )

    def resume(
        self,
        session_id: str,
        checkpoint_id: str | None = None,
    ) -> DashboardChatResponse:
        """Resume a previously interrupted dashboard-chat run from persisted LangGraph state.

        This operates at graph-step boundaries only. If the checkpointed session is already
        complete, the latest stored response is returned directly.
        """
        graph = self._persistent_graph_or_raise()
        config = self._thread_config(session_id, checkpoint_id)
        state_snapshot = graph.get_state(config)
        if state_snapshot is None:
            raise ValueError(f"No checkpointed dashboard chat state found for session {session_id}")

        persisted_state = state_snapshot.values or {}
        if not state_snapshot.next:
            if persisted_state.get("response") is None:
                raise ValueError(f"Session {session_id} has no resumable or completed response")
            response = DashboardChatResponse.model_validate(persisted_state.get("response") or {})
            return response

        runtime_started_at = perf_counter()
        final_state = graph.invoke(None, config=config)
        runtime_total_ms = round((perf_counter() - runtime_started_at) * 1000, 2)
        response = DashboardChatResponse.model_validate(final_state.get("response") or {})
        timing_breakdown = dict(final_state.get("timing_breakdown") or {})
        timing_breakdown["runtime_total_ms"] = runtime_total_ms
        response_metadata = dict(response.metadata)
        response_metadata["timing_breakdown"] = timing_breakdown
        return DashboardChatResponse(
            answer_text=response.answer_text,
            intent=response.intent,
            citations=response.citations,
            warnings=response.warnings,
            sql=response.sql,
            sql_results=response.sql_results,
            usage=response.usage,
            tool_calls=response.tool_calls,
            metadata=response_metadata,
        )


@lru_cache(maxsize=1)
def get_dashboard_chat_runtime() -> DashboardChatRuntime:
    """Return the shared dashboard chat runtime used by live chat turns."""
    return DashboardChatRuntime(checkpointer=get_dashboard_chat_checkpointer().saver)


def reset_dashboard_chat_runtime() -> None:
    """Clear the shared runtime cache so tests can release checkpoint resources cleanly."""
    get_dashboard_chat_runtime.cache_clear()
