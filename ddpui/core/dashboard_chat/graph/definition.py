"""Graph definition helpers for dashboard chat orchestration."""

from langgraph.graph import END, START, StateGraph

from .state import DashboardChatRuntimeState


def build_dashboard_chat_graph(runtime):
    """Build the explicit prototype-aligned intent graph."""
    graph = StateGraph(DashboardChatRuntimeState)
    graph.add_node("load_context", runtime._node_load_context)
    graph.add_node("route_intent", runtime._node_route_intent)
    graph.add_node("handle_small_talk", runtime._node_handle_small_talk)
    graph.add_node("handle_irrelevant", runtime._node_handle_irrelevant)
    graph.add_node("handle_needs_clarification", runtime._node_handle_needs_clarification)
    graph.add_node("handle_query_with_sql", runtime._node_handle_query_with_sql)
    graph.add_node("handle_query_without_sql", runtime._node_handle_query_without_sql)
    graph.add_node("handle_follow_up_sql", runtime._node_handle_follow_up_sql)
    graph.add_node("handle_follow_up_context", runtime._node_handle_follow_up_context)
    graph.add_node("finalize", runtime._node_finalize_response)

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
