"""Schema and metadata lookup tool handlers."""

from typing import Any

from ddpui.core.dashboard_chat.warehouse.sql_guard import DashboardChatSqlGuard

from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_parsing import (
    find_tables_with_column,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.runtime.turn_context import (
    DashboardChatTurnContext,
    get_or_load_schema_snippets,
    get_turn_warehouse_tools,
    metadata_column_is_pii,
    record_validated_distinct_values,
)
from ddpui.core.dashboard_chat.orchestration.pii_masking import mask_distinct_values_for_llm


def _build_table_profile(columns: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize whether a table is row-grain, aggregate-like, or score-bearing."""
    column_names = [str(column.get("name") or "") for column in columns]
    normalized_column_names = [column_name.lower() for column_name in column_names]

    join_key_columns = [
        column_name
        for column_name in column_names
        if column_name.lower() == "id" or column_name.lower().endswith("_id")
    ]
    entity_name_columns = [
        column_name for column_name in column_names if "name" in column_name.lower()
    ]
    numeric_score_columns = [
        column_name
        for column_name in column_names
        if any(
            marker in column_name.lower()
            for marker in ("_perc", "_mastery", "score", "percentage", "percent")
        )
    ]
    performance_columns = [
        column_name
        for column_name in column_names
        if any(
            marker in column_name.lower()
            for marker in (
                "_status",
                "_level",
                "grade_level",
                "_perc",
                "_mastery",
                "score",
            )
        )
    ]
    aggregate_count_columns = [
        column_name for column_name in column_names if "count" in column_name.lower()
    ]
    is_aggregate_like = bool(aggregate_count_columns) and not join_key_columns

    return {
        "join_key_columns": join_key_columns[:10],
        "entity_name_columns": entity_name_columns[:10],
        "numeric_score_columns": numeric_score_columns[:10],
        "performance_columns": performance_columns[:15],
        "aggregate_count_columns": aggregate_count_columns[:10],
        "is_aggregate_like": is_aggregate_like,
        "has_join_key": bool(join_key_columns),
        "has_entity_name_columns": bool(entity_name_columns),
        "has_numeric_score_columns": bool(numeric_score_columns),
        "has_performance_columns": bool(performance_columns),
        "normalized_columns": normalized_column_names[:50],
    }


def _build_table_hint(table_name: str, profile: dict[str, Any]) -> str | None:
    """Emit a short model-usage hint for the LLM when a schema is obviously aggregate-like."""
    if profile["is_aggregate_like"]:
        if not profile["has_entity_name_columns"]:
            return (
                f"{table_name} looks aggregate-like: it has summary count columns but no row-level id "
                "or entity-name columns. Do not use it for named-entity rankings or lists if deeper "
                "fact/dimension models are needed."
            )
        return (
            f"{table_name} looks aggregate-like: it summarizes counts and does not expose row-level ids. "
            "Use it for chart totals, but inspect deeper models before answering named-entity or "
            "score-threshold questions."
        )
    if profile["has_numeric_score_columns"]:
        return (
            f"{table_name} includes direct numeric score/mastery fields. Prefer these columns for explicit "
            "percentage thresholds, averages, and best-performing comparisons."
        )
    return None


def handle_get_schema_snippets_tool(
    warehouse_tools_factory,
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
) -> dict[str, Any]:
    """Return schema snippets for allowlisted tables only."""
    allowlist = DashboardChatAllowlist.model_validate(state.get("allowlist_payload") or {})
    requested_tables = [str(table_name).strip() for table_name in args.get("tables") or []]
    allowed_tables: list[str] = []
    filtered_tables: list[str] = []
    for table_name in requested_tables:
        resolved_table_name = allowlist.resolve_allowed_table_name(table_name)
        if resolved_table_name is None:
            filtered_tables.append(table_name)
            continue
        allowed_tables.append(resolved_table_name)
    allowed_tables = list(dict.fromkeys(allowed_tables))
    filtered_tables = sorted(set(filtered_tables))
    schema_snippets_by_table = get_or_load_schema_snippets(
        warehouse_tools_factory,
        state,
        turn_context,
        tables=allowed_tables,
    )
    tables_payload = []
    for table_name, snippet in schema_snippets_by_table.items():
        if table_name not in allowed_tables:
            continue
        profile = _build_table_profile(snippet.columns)
        table_payload: dict[str, Any] = {
            "table": table_name,
            "columns": snippet.columns,
            "profile": profile,
        }
        hint = _build_table_hint(table_name, profile)
        if hint is not None:
            table_payload["hint"] = hint
        tables_payload.append(table_payload)
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
    requested_table_name = str(args.get("table") or "").strip()
    table_name = allowlist.resolve_allowed_table_name(requested_table_name)
    column_name = str(args.get("column") or "")
    limit = max(1, min(int(args.get("limit", 50)), 200))
    if table_name is None:
        return {
            "error": "table_not_allowed",
            "table": requested_table_name,
            "message": (
                f"Table {requested_table_name} is not accessible in the current dashboard context."
            ),
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

    if metadata_column_is_pii(
        state,
        table_name=table_name,
        column_name=column_name,
    ):
        return {
            "error": "pii_distinct_values_unavailable",
            "table": table_name,
            "column": column_name,
            "message": (
                "Distinct values are not available for columns marked as PII. "
                "Write the SQL without fetching or exposing the distinct values."
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
        column_values_exhaustive=len(values) < limit,
    )
    masked_values = mask_distinct_values_for_llm(
        state=state,
        turn_context=turn_context,
        table_name=table_name,
        column_name=column_name,
        values=values,
    )
    return {
        "table": table_name,
        "column": column_name,
        "values": masked_values,
        "count": len(masked_values),
    }


def handle_check_table_row_count_tool(
    warehouse_tools_factory,
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
) -> dict[str, Any]:
    """Count rows in one allowlisted table."""
    allowlist = DashboardChatAllowlist.model_validate(state.get("allowlist_payload") or {})
    requested_table_name = str(args.get("table") or "").strip()
    table_name = allowlist.resolve_allowed_table_name(requested_table_name)
    if table_name is None:
        return {
            "error": "table_not_allowed",
            "table": requested_table_name,
            "message": (
                f"Table {requested_table_name} is not accessible in the current dashboard context."
            ),
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
