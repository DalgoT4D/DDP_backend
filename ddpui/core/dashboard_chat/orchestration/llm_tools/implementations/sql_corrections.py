"""Structured SQL correction helpers for model-authored warehouse queries."""

import re
from typing import Any

from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_parsing import (
    best_table_for_missing_columns,
    find_tables_with_column,
    primary_table_name,
    referenced_sql_identifier_refs,
    resolve_identifier_table,
    resolve_table_qualifier,
    table_references,
    tables_with_column,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.runtime.turn_context import (
    DashboardChatTurnContext,
    get_or_load_schema_snippets,
)


def missing_columns_in_primary_table(
    warehouse_tools_factory,
    *,
    sql: str,
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
) -> dict[str, Any] | None:
    """Return a corrective tool error when SQL references absent columns."""
    table_refs = table_references(sql)
    referenced_tables = [
        reference["table_name"] for reference in table_refs if reference.get("table_name")
    ]
    if not referenced_tables:
        return None

    schema_snippets_by_table = get_or_load_schema_snippets(
        warehouse_tools_factory,
        state,
        turn_context,
        tables=referenced_tables,
    )
    all_schema_snippets_by_table = get_or_load_schema_snippets(warehouse_tools_factory, state, turn_context)
    missing_columns_by_table: dict[str, set[str]] = {}
    candidate_tables_by_column: dict[str, list[str]] = {}
    tables_in_query = list(dict.fromkeys(referenced_tables))

    for qualifier, column_name in referenced_sql_identifier_refs(sql):
        resolved_table = resolve_identifier_table(
            qualifier=qualifier,
            column_name=column_name,
            table_refs=table_refs,
            schema_snippets_by_table=schema_snippets_by_table,
        )
        if resolved_table is not None:
            continue

        if qualifier is not None:
            target_table = (
                resolve_table_qualifier(qualifier, table_refs)
                or primary_table_name(sql)
                or tables_in_query[0]
            )
        else:
            matching_tables = tables_with_column(column_name, tables_in_query, schema_snippets_by_table)
            if len(matching_tables) > 1:
                continue
            target_table = primary_table_name(sql) or tables_in_query[0]

        missing_columns_by_table.setdefault(target_table, set()).add(column_name)
        candidate_tables_by_column[column_name] = find_tables_with_column(
            column_name,
            all_schema_snippets_by_table,
        )

    missing_columns = sorted(
        {column_name for columns in missing_columns_by_table.values() for column_name in columns}
    )
    if not missing_columns:
        return None

    primary = primary_table_name(sql) or tables_in_query[0]
    target_table = (
        next(iter(missing_columns_by_table)) if len(missing_columns_by_table) == 1 else primary
    )
    best_table = best_table_for_missing_columns(missing_columns, all_schema_snippets_by_table)
    message = (
        f"Column(s) {', '.join(missing_columns)} do not exist on {target_table}. "
        "Use a table that contains the requested dimension or measure, and rewrite the SQL using columns from that table."
    )
    if best_table:
        message += f" Best candidate table: {best_table}."
    result = {
        "error": "column_not_in_table",
        "table": target_table,
        "missing_columns": missing_columns,
        "candidate_tables": candidate_tables_by_column,
        "best_table": best_table,
        "message": message,
    }
    if len(missing_columns) == 1:
        column_name = missing_columns[0]
        result["column"] = column_name
        result["candidates"] = candidate_tables_by_column.get(column_name, [])
    return result


def structured_sql_execution_error(
    warehouse_tools_factory,
    *,
    sql: str,
    error: Exception,
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
) -> dict[str, Any] | None:
    """Convert warehouse execution errors into structured corrective feedback."""
    error_text = str(error)
    missing_column_match = re.search(
        r'column "(?:[\w]+\.)?([^"]+)" does not exist',
        error_text,
        flags=re.IGNORECASE,
    )
    if missing_column_match:
        missing_column = missing_column_match.group(1).lower()
        schema_snippets_by_table = get_or_load_schema_snippets(warehouse_tools_factory, state, turn_context)
        candidate_tables = find_tables_with_column(missing_column, schema_snippets_by_table)
        return {
            "error": "column_not_in_table",
            "table": primary_table_name(sql),
            "column": missing_column,
            "missing_columns": [missing_column],
            "candidates": candidate_tables,
            "candidate_tables": {missing_column: candidate_tables},
            "best_table": candidate_tables[0] if candidate_tables else None,
            "message": (
                f"Column {missing_column} is not available on the current table. "
                "Pick a table that contains it, inspect that schema, and rewrite the SQL using that table's real columns."
            ),
            "sql_used": sql,
        }
    return None
