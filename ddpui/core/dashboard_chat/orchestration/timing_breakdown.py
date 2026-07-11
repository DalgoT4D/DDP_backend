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


def build_latency_summary(
    *,
    timing_breakdown: dict[str, Any],
    usage: dict[str, Any],
) -> dict[str, Any]:
    """Return compact p50/p95 timing plus LLM call/token summaries."""
    return {
        "runtime_total_ms": timing_breakdown.get("runtime_total_ms"),
        "graph_nodes": _summarize_named_durations(
            [
                {"name": name, "duration_ms": duration}
                for name, duration in dict(timing_breakdown.get("graph_nodes_ms") or {}).items()
            ]
        ),
        "tool_calls": _summarize_named_durations(
            list(timing_breakdown.get("tool_calls_ms") or [])
        ),
        "llm": _summarize_llm_usage(usage),
    }


def _summarize_named_durations(entries: list[dict[str, Any]]) -> dict[str, Any]:
    by_name: dict[str, list[float]] = {}
    for entry in entries:
        name = str(entry.get("name") or "").strip()
        if not name:
            continue
        try:
            duration = float(entry.get("duration_ms") or 0)
        except (TypeError, ValueError):
            continue
        by_name.setdefault(name, []).append(duration)
    return {
        name: {
            "count": len(values),
            "total_ms": round(sum(values), 2),
            "p50_ms": _percentile(values, 50),
            "p95_ms": _percentile(values, 95),
        }
        for name, values in sorted(by_name.items())
    }


def _summarize_llm_usage(usage: dict[str, Any]) -> dict[str, Any]:
    llm_usage = dict((usage or {}).get("llm") or {})
    calls = list(llm_usage.get("calls") or [])
    by_operation: dict[str, dict[str, Any]] = {}
    for call in calls:
        operation = str(call.get("operation") or "unknown")
        summary = by_operation.setdefault(
            operation,
            {
                "calls": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
        )
        summary["calls"] += 1
        summary["prompt_tokens"] += int(call.get("prompt_tokens") or 0)
        summary["completion_tokens"] += int(call.get("completion_tokens") or 0)
        summary["total_tokens"] += int(call.get("total_tokens") or 0)
    totals = dict(llm_usage.get("totals") or {})
    return {
        "total_calls": len(calls),
        "prompt_tokens": int(totals.get("prompt_tokens") or 0),
        "completion_tokens": int(totals.get("completion_tokens") or 0),
        "total_tokens": int(totals.get("total_tokens") or 0),
        "by_operation": by_operation,
    }


def _percentile(values: list[float], percentile: int) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return round(ordered[0], 2)
    rank = (len(ordered) - 1) * (percentile / 100)
    lower_index = int(rank)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    lower = ordered[lower_index]
    upper = ordered[upper_index]
    interpolated = lower + (upper - lower) * (rank - lower_index)
    return round(interpolated, 2)
