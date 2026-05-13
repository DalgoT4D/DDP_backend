"""Deterministic dbt lookup tool handlers."""

from typing import Any

from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.llm_tools.runtime.turn_context import (
    DashboardChatTurnContext,
    dbt_resources_by_unique_id,
)


def handle_search_dbt_models_tool(
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
) -> dict[str, Any]:
    """Search allowlisted dbt nodes by name, description, and column metadata."""
    del turn_context
    query = str(args.get("query") or "").strip().lower()
    limit = max(1, min(int(args.get("limit", 8)), 20))
    if not query:
        return {"models": [], "count": 0}

    results: list[dict[str, Any]] = []
    for node in dbt_resources_by_unique_id(state).values():
        table_name = node.get("table")
        haystacks = [
            str(node.get("name") or ""),
            str(node.get("description") or ""),
            str(table_name or ""),
        ]
        for column in node.get("columns") or []:
            haystacks.append(str(column.get("name") or ""))
            haystacks.append(str(column.get("description") or ""))
        if query not in " ".join(haystacks).lower():
            continue
        results.append(
            {
                "name": str(node.get("name") or ""),
                "schema": str(node.get("schema") or ""),
                "database": str(node.get("database") or ""),
                "description": str(node.get("description") or ""),
                "columns": [
                    str(column.get("name") or "") for column in (node.get("columns") or [])
                ][:20],
                "table": table_name,
            }
        )
        if len(results) >= limit:
            break

    return {"models": results, "count": len(results)}


def handle_get_dbt_model_info_tool(
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
) -> dict[str, Any]:
    """Return one dbt model's description, columns, and lineage."""
    del turn_context
    model_name = str(args.get("model_name") or "").strip().lower()
    if not model_name:
        return {"error": "model_name is required"}

    matched_unique_id: str | None = None
    matched_node: dict[str, Any] | None = None
    for unique_id, node in dbt_resources_by_unique_id(state).items():
        table_name = node.get("table")
        candidates = {
            str(node.get("name") or "").lower(),
            str(table_name or "").lower(),
        }
        if model_name not in candidates:
            continue
        matched_unique_id = unique_id
        matched_node = node
        break

    if matched_unique_id is None or matched_node is None:
        return {"error": f"Model not found: {model_name}"}

    return {
        "model": str(matched_node.get("name") or ""),
        "schema": str(matched_node.get("schema") or ""),
        "database": str(matched_node.get("database") or ""),
        "description": str(matched_node.get("description") or ""),
        "columns": list(matched_node.get("columns") or [])[:50],
        "upstream": list(matched_node.get("upstream") or []),
        "downstream": list(matched_node.get("downstream") or []),
    }
