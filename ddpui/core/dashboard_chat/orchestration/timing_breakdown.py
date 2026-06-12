"""Timing-breakdown helpers for dashboard chat orchestration."""

from typing import Any

from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState


def merge_tool_loop_timing(
    state: DashboardChatGraphState,
    execution_result: dict[str, Any],
) -> dict[str, Any]:
    """Merge tool-loop timing into the current graph timing payload."""
    existing = dict(state.get("timing_breakdown") or {})
    from_loop = dict(execution_result.get("timing_breakdown") or {})
    merged = dict(existing)
    if "graph_nodes_ms" in existing or "graph_nodes_ms" in from_loop:
        merged["graph_nodes_ms"] = {
            **dict(existing.get("graph_nodes_ms") or {}),
            **dict(from_loop.get("graph_nodes_ms") or {}),
        }
    if "tool_calls_ms" in existing or "tool_calls_ms" in from_loop:
        merged["tool_calls_ms"] = list(
            from_loop.get("tool_calls_ms") or existing.get("tool_calls_ms") or []
        )
    for key, value in from_loop.items():
        if key not in {"graph_nodes_ms", "tool_calls_ms"}:
            merged[key] = value
    return merged
