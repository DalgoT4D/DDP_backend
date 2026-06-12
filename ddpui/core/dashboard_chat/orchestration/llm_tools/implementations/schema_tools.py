"""Schema and metadata lookup tool handlers."""

from typing import Any

from ddpui.core.dashboard_chat.warehouse.sql_guard import DashboardChatSqlGuard
from ddpui.utils.custom_logger import CustomLogger

from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_parsing import (
    find_tables_with_column,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.runtime.turn_context import (
    DashboardChatTurnContext,
    get_or_load_schema_snippets,
    get_turn_warehouse_tools,
    record_validated_distinct_values,
)

logger = CustomLogger("dashboard_chat")


def handle_get_schema_snippets_tool(
    warehouse_tools_factory,
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
) -> dict[str, Any]:
    """Return schema snippets for allowlisted tables only."""
    allowlist = DashboardChatAllowlist.model_validate(state.get("allowlist_payload") or {})
    requested_tables = [str(table_name).lower() for table_name in args.get("tables") or []]
    allowed_tables = [
        table_name for table_name in requested_tables if allowlist.is_allowed(table_name)
    ]
    filtered_tables = sorted(set(requested_tables) - set(allowed_tables))
    schema_snippets_by_table = get_or_load_schema_snippets(
        warehouse_tools_factory,
        state,
        turn_context,
        tables=allowed_tables,
    )
    tables_payload = [
        {"table": table_name, "columns": snippet.columns}
        for table_name, snippet in schema_snippets_by_table.items()
        if table_name in allowed_tables
    ]
    response: dict[str, Any] = {"tables": tables_payload}
    if filtered_tables:
        response["filtered_tables"] = filtered_tables
        response[
            "filter_note"
        ] = f"{len(filtered_tables)} tables were filtered out because they are not used by the current dashboard."
    return response


def handle_get_distinct_values_tool(
    warehouse_tools_factory,
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
) -> dict[str, Any]:
    """Return distinct values and persist validated filter values for the session."""
    allowlist = DashboardChatAllowlist.model_validate(state.get("allowlist_payload") or {})
    table_name = str(args.get("table") or "").lower()
    column_name = str(args.get("column") or "")
    limit = max(1, min(int(args.get("limit", 50)), 200))
    if not allowlist.is_allowed(table_name):
        return {
            "error": "table_not_allowed",
            "table": table_name,
            "message": (f"Table {table_name} is not accessible in the current dashboard context."),
        }

    schema_snippets_by_table = get_or_load_schema_snippets(
        warehouse_tools_factory, state, turn_context
    )
    snippet = schema_snippets_by_table.get(table_name)
    normalized_column_name = column_name.lower()
    if snippet is not None and normalized_column_name not in {
        str(column.get("name") or "").lower() for column in snippet.columns
    }:
        candidates = find_tables_with_column(normalized_column_name, schema_snippets_by_table)
        return {
            "error": "column_not_in_table",
            "table": table_name,
            "column": column_name,
            "candidates": candidates,
            "message": (
                f"Column {column_name} is not available on {table_name}. "
                "Use a table that contains it, inspect that schema, and retry the lookup."
            ),
        }

    values = get_turn_warehouse_tools(
        warehouse_tools_factory,
        turn_context,
        state,
    ).get_distinct_values(
        table_name=table_name,
        column_name=column_name,
        limit=limit,
    )
    record_validated_distinct_values(
        turn_context=turn_context,
        table_name=table_name,
        column_name=column_name,
        values=values,
    )
    return {
        "table": table_name,
        "column": column_name,
        "values": values,
        "count": len(values),
    }


def handle_list_tables_by_keyword_tool(
    warehouse_tools_factory,
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
) -> dict[str, Any]:
    """Search allowlisted tables by table name or column name."""
    allowlist = DashboardChatAllowlist.model_validate(state.get("allowlist_payload") or {})
    keyword = str(args.get("keyword") or "").strip().lower()
    limit = max(1, min(int(args.get("limit", 15)), 50))
    if not keyword:
        return {"tables": []}

    allowlist_tables_source = allowlist.prioritized_tables() or sorted(allowlist.allowed_tables)
    allowlisted_tables = list(
        dict.fromkeys(table_name.lower() for table_name in allowlist_tables_source)
    )
    direct_match_tables = [
        table_name
        for table_name in allowlisted_tables
        if keyword in table_name or keyword in table_name.rsplit(".", 1)[-1]
    ]

    schema_snippets_by_table: dict[str, Any] = {}
    lookup_tables = direct_match_tables or allowlisted_tables
    if lookup_tables:
        try:
            schema_snippets_by_table = get_or_load_schema_snippets(
                warehouse_tools_factory,
                state,
                turn_context,
                tables=lookup_tables,
            )
        except Exception as error:
            logger.warning("Dashboard chat keyword table lookup fell back to names only: %s", error)
            turn_context.warnings.append(str(error))

    matches: list[dict[str, Any]] = []
    seen_tables: set[str] = set()

    for table_name in direct_match_tables:
        column_names = [
            str(column.get("name") or "")
            for column in getattr(schema_snippets_by_table.get(table_name), "columns", [])
        ]
        matches.append({"table": table_name, "columns": column_names[:40]})
        seen_tables.add(table_name)
        if len(matches) >= limit:
            break

    for table_name, snippet in schema_snippets_by_table.items():
        if table_name in seen_tables:
            continue
        column_names = [str(column.get("name") or "") for column in snippet.columns]
        if not any(keyword in column_name.lower() for column_name in column_names):
            continue
        matches.append({"table": table_name, "columns": column_names[:40]})
        if len(matches) >= limit:
            break

    if matches:
        return {
            "tables": matches,
            "hint": (
                f"Found {len(matches)} allowlisted tables. Check schema before assuming table structure."
            ),
        }
    return {
        "tables": [],
        "hint": (
            f"No allowlisted tables matched '{keyword}'. Try a broader keyword or retrieve chart docs first."
        ),
    }


def handle_check_table_row_count_tool(
    warehouse_tools_factory,
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
) -> dict[str, Any]:
    """Count rows in one allowlisted table."""
    allowlist = DashboardChatAllowlist.model_validate(state.get("allowlist_payload") or {})
    table_name = str(args.get("table") or "").lower()
    if not allowlist.is_allowed(table_name):
        return {
            "error": "table_not_allowed",
            "table": table_name,
            "message": (f"Table {table_name} is not accessible in the current dashboard context."),
        }

    sql = f"SELECT COUNT(*) AS row_count FROM {table_name} LIMIT 1"
    validation = DashboardChatSqlGuard(
        allowlist=allowlist,
        max_rows=1,
    ).validate(sql)
    if not validation.is_valid or not validation.sanitized_sql:
        return {"error": "sql_validation_failed", "issues": validation.errors}

    rows = get_turn_warehouse_tools(
        warehouse_tools_factory,
        turn_context,
        state,
    ).execute_sql(validation.sanitized_sql)
    row_count = 0
    if rows:
        row_count = int(rows[0].get("row_count") or 0)
    return {"table": table_name, "row_count": row_count, "has_data": row_count > 0}
