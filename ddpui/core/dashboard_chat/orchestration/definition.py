"""Graph definition helpers for dashboard chat orchestration."""

from time import perf_counter

from langgraph.graph import END, START, StateGraph

from .state import DashboardChatRuntimeState


def _timed_node(node_name, handler):
    """Wrap one graph node so per-node duration is persisted on state."""

    def wrapped(state: DashboardChatRuntimeState) -> DashboardChatRuntimeState:
        started_at = perf_counter()
        next_state = handler(state)
        elapsed_ms = round((perf_counter() - started_at) * 1000, 2)
        timing_breakdown = dict(next_state.get("timing_breakdown") or {})
        graph_nodes_ms = dict(timing_breakdown.get("graph_nodes_ms") or {})
        graph_nodes_ms[node_name] = elapsed_ms
        timing_breakdown["graph_nodes_ms"] = graph_nodes_ms
        next_state["timing_breakdown"] = timing_breakdown
        return next_state

    return wrapped


def build_dashboard_chat_graph(runtime):
    """Build the explicit prototype-aligned intent graph."""
    graph = StateGraph(DashboardChatRuntimeState)
    graph.add_node("load_context", _timed_node("load_context", runtime._node_load_context))
    graph.add_node("route_intent", _timed_node("route_intent", runtime._node_route_intent))
    graph.add_node(
        "handle_small_talk",
        _timed_node("handle_small_talk", runtime._node_handle_small_talk),
    )
    graph.add_node(
        "handle_irrelevant",
        _timed_node("handle_irrelevant", runtime._node_handle_irrelevant),
    )
    graph.add_node(
        "handle_needs_clarification",
        _timed_node("handle_needs_clarification", runtime._node_handle_needs_clarification),
    )
    graph.add_node(
        "handle_query_with_sql",
        _timed_node("handle_query_with_sql", runtime._node_handle_query_with_sql),
    )
    graph.add_node(
        "handle_query_without_sql",
        _timed_node("handle_query_without_sql", runtime._node_handle_query_without_sql),
    )
    graph.add_node(
        "handle_follow_up_sql",
        _timed_node("handle_follow_up_sql", runtime._node_handle_follow_up_sql),
    )
    graph.add_node(
        "handle_follow_up_context",
        _timed_node("handle_follow_up_context", runtime._node_handle_follow_up_context),
    )
    graph.add_node("finalize", _timed_node("finalize", runtime._node_finalize_response))

    graph.add_edge(START, "load_context")
    graph.add_edge("load_context", "route_intent")
    graph.add_conditional_edges(
        "route_intent",
        runtime._route_after_intent,
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
    graph.add_edge("handle_small_talk", "finalize")
    graph.add_edge("handle_irrelevant", "finalize")
    graph.add_edge("handle_needs_clarification", "finalize")
    graph.add_edge("handle_query_with_sql", "finalize")
    graph.add_edge("handle_query_without_sql", "finalize")
    graph.add_edge("handle_follow_up_sql", "finalize")
    graph.add_edge("handle_follow_up_context", "finalize")
    graph.add_edge("finalize", END)
    return graph.compile()
