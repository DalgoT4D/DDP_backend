"""Main dashboard chat LangGraph orchestrator."""

from collections.abc import Callable, Sequence
from functools import lru_cache
from time import perf_counter
from typing import Any

from langgraph.graph import END, START, StateGraph

from ddpui.core.dashboard_chat.config import DashboardChatRuntimeConfig, DashboardChatSourceConfig
from ddpui.core.dashboard_chat.agents.interface import DashboardChatLlmClient
from ddpui.core.dashboard_chat.agents.openai import OpenAIDashboardChatLlmClient
from ddpui.core.dashboard_chat.contracts import DashboardChatResponse
from ddpui.core.dashboard_chat.vector.store import ChromaDashboardChatVectorStore
from ddpui.core.dashboard_chat.warehouse.tools import DashboardChatWarehouseTools
from ddpui.models.org import Org

from .conversation import normalize_conversation_history
from .nodes.finalize import finalize_node
from .nodes.handle_data_query import handle_data_query_node
from .nodes.handle_follow_up import handle_follow_up_node
from .nodes.handle_irrelevant import handle_irrelevant_node
from .nodes.handle_needs_clarification import handle_needs_clarification_node
from .nodes.handle_small_talk import handle_small_talk_node
from .nodes.helpers import route_after_intent
from .nodes.load_context import load_context_node
from .nodes.route_intent import route_intent_node
from .state import DashboardChatRuntimeState, SMALL_TALK_FAST_PATH_PATTERN
from .tools.specifications import DASHBOARD_CHAT_TOOL_SPECIFICATIONS


def _timed_node(node_name: str, handler):
    """Wrap one graph node so per-node duration is recorded in timing_breakdown."""

    def wrapped(state: DashboardChatRuntimeState) -> dict:
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

    def _handle_data_query(state):
        return handle_data_query_node(
            state,
            llm_client,
            vector_store,
            warehouse_tools_factory,
            runtime_config,
            source_config,
            tool_specifications,
        )

    def _handle_follow_up(state):
        return handle_follow_up_node(
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

    graph = StateGraph(DashboardChatRuntimeState)
    graph.add_node("load_context", _timed_node("load_context", _load_context))
    graph.add_node("route_intent", _timed_node("route_intent", _route_intent))
    graph.add_node("handle_small_talk", _timed_node("handle_small_talk", _handle_small_talk))
    graph.add_node("handle_irrelevant", _timed_node("handle_irrelevant", _handle_irrelevant))
    graph.add_node(
        "handle_needs_clarification",
        _timed_node("handle_needs_clarification", _handle_needs_clarification),
    )
    graph.add_node("handle_data_query", _timed_node("handle_data_query", _handle_data_query))
    graph.add_node("handle_follow_up", _timed_node("handle_follow_up", _handle_follow_up))
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
            "query_with_sql": "handle_data_query",
            "query_without_sql": "handle_data_query",
            "follow_up_sql": "handle_follow_up",
            "follow_up_context": "handle_follow_up",
        },
    )
    graph.add_edge("handle_small_talk", "finalize")
    graph.add_edge("handle_irrelevant", "finalize")
    graph.add_edge("handle_needs_clarification", "finalize")
    graph.add_edge("handle_data_query", "finalize")
    graph.add_edge("handle_follow_up", "finalize")
    graph.add_edge("finalize", END)
    return graph.compile()


class DashboardChatRuntime:
    """Run dashboard chat turns with the prototype's explicit intent routing and tool loop."""

    def __init__(
        self,
        vector_store: ChromaDashboardChatVectorStore | None = None,
        llm_client: DashboardChatLlmClient | None = None,
        warehouse_tools_factory: Callable[[Org], DashboardChatWarehouseTools] | None = None,
        runtime_config: DashboardChatRuntimeConfig | None = None,
        source_config: DashboardChatSourceConfig | None = None,
    ):
        self.runtime_config = runtime_config or DashboardChatRuntimeConfig.from_env()
        self.source_config = source_config or DashboardChatSourceConfig.from_env()
        self.vector_store = vector_store or ChromaDashboardChatVectorStore()
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

    def run(
        self,
        org: Org,
        dashboard_id: int,
        user_query: str,
        session_id: str | None = None,
        vector_collection_name: str | None = None,
        conversation_history: Sequence[dict[str, Any]] | None = None,
    ) -> DashboardChatResponse:
        """Run one dashboard chat turn."""
        if hasattr(self.llm_client, "reset_usage"):
            self.llm_client.reset_usage()
        if hasattr(self.vector_store, "reset_usage"):
            self.vector_store.reset_usage()
        initial_state: DashboardChatRuntimeState = {
            "org": org,
            "dashboard_id": dashboard_id,
            "session_id": session_id,
            "vector_collection_name": vector_collection_name,
            "user_query": user_query,
            "conversation_history": normalize_conversation_history(conversation_history),
            "timing_breakdown": {
                "graph_nodes_ms": {},
                "tool_calls_ms": [],
            },
            "warnings": [],
            "usage": {},
        }
        runtime_started_at = perf_counter()
        final_state = self.graph.invoke(initial_state)
        runtime_total_ms = round((perf_counter() - runtime_started_at) * 1000, 2)
        response = final_state["response"]
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
    return DashboardChatRuntime()
