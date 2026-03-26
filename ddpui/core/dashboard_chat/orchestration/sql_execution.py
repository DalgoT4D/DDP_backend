"""SQL execution and guardrail helpers for dashboard chat graph execution."""

import json
import re
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder

from ddpui.core.dashboard_chat.context.allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.contracts import DashboardChatIntent
from ddpui.core.dashboard_chat.warehouse.sql_guard import DashboardChatSqlGuard

from .conversation import extract_requested_follow_up_dimension
from .sql_parsing import (
    table_references,
    resolve_identifier_table,
    tables_with_column,
    extract_text_filter_values,
    find_tables_with_column,
    primary_table_name,
    referenced_sql_identifier_refs,
    resolve_table_qualifier,
    best_table_for_missing_columns,
    structural_dimensions_from_sql,
    normalize_dimension_name,
)
from .state import DashboardChatRuntimeState
from .tool_handlers import (
    get_turn_warehouse_tools,
    get_cached_schema_snippets,
    has_validated_distinct_value,
    is_text_type,
    record_validated_distinct_values,
    record_validated_filters_from_sql,
)


def validate_sql_allowlist(
    sql: str,
    allowlist: DashboardChatAllowlist,
) -> dict[str, Any]:
    """Validate that all referenced tables are in the dashboard allowlist."""
    referenced_tables = DashboardChatSqlGuard._extract_table_names(sql)
    invalid_tables = [
        table_name for table_name in referenced_tables if not allowlist.is_allowed(table_name)
    ]
    if invalid_tables:
        return {
            "valid": False,
            "invalid_tables": invalid_tables,
            "message": (
                "SQL references tables not available in the current dashboard: "
                + ", ".join(invalid_tables)
                + ". Use list_tables_by_keyword to find allowed tables."
            ),
        }
    return {"valid": True, "invalid_tables": [], "message": ""}


def run_sql_with_distinct_guard(
    warehouse_tools_factory,
    runtime_config,
    args: dict[str, Any],
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
) -> dict[str, Any]:
    """Validate SQL like the prototype and let the tool loop self-correct on failures."""
    sql = str(args.get("sql") or "").strip()
    if not sql:
        return {"error": "sql_missing", "message": "SQL is required"}

    allowlist_validation = validate_sql_allowlist(sql, state["allowlist"])
    if not allowlist_validation["valid"]:
        return {
            "error": "table_not_allowed",
            "invalid_tables": allowlist_validation["invalid_tables"],
            "message": allowlist_validation["message"],
        }

    follow_up_dimension_validation = validate_follow_up_dimension_usage(
        warehouse_tools_factory,
        sql=sql,
        state=state,
        execution_context=execution_context,
    )
    if follow_up_dimension_validation is not None:
        return follow_up_dimension_validation

    missing_distinct = check_missing_distinct(
        warehouse_tools_factory, sql, state, execution_context
    )
    if missing_distinct:
        return {
            "error": "must_fetch_distinct_values",
            "missing": missing_distinct,
            "message": (
                "Call get_distinct_values for these columns, then regenerate the SQL using one of the returned values."
            ),
        }

    validation = DashboardChatSqlGuard(
        allowlist=state["allowlist"],
        max_rows=runtime_config.max_query_rows,
    ).validate(sql)
    execution_context["last_sql_validation"] = validation
    if not validation.is_valid or not validation.sanitized_sql:
        return {
            "error": "sql_validation_failed",
            "issues": validation.errors,
            "warnings": validation.warnings,
        }

    missing_columns = missing_columns_in_primary_table(
        warehouse_tools_factory,
        sql=validation.sanitized_sql,
        state=state,
        execution_context=execution_context,
    )
    if missing_columns is not None:
        return missing_columns

    execution_context["last_sql"] = validation.sanitized_sql
    try:
        rows = get_turn_warehouse_tools(
            warehouse_tools_factory,
            execution_context,
            state["org"],
        ).execute_sql(validation.sanitized_sql)
    except Exception as error:
        structured_error = structured_sql_execution_error(
            warehouse_tools_factory,
            sql=validation.sanitized_sql,
            error=error,
            state=state,
            execution_context=execution_context,
        )
        if structured_error is not None:
            return structured_error
        return {
            "success": False,
            "error": str(error),
            "sql_used": validation.sanitized_sql,
        }

    serialized_rows = json.loads(json.dumps(rows, cls=DjangoJSONEncoder))
    execution_context["last_sql_results"] = serialized_rows
    record_validated_filters_from_sql(
        state=state,
        execution_context=execution_context,
        sql=validation.sanitized_sql,
    )
    return {
        "success": True,
        "row_count": len(serialized_rows),
        "error": None,
        "sql_used": validation.sanitized_sql,
        "columns": list(serialized_rows[0].keys()) if serialized_rows else [],
        "rows": serialized_rows,
    }


def missing_columns_in_primary_table(
    warehouse_tools_factory,
    *,
    sql: str,
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
) -> dict[str, Any] | None:
    """Return a corrective tool error when SQL references columns absent from the referenced query tables."""
    table_refs = table_references(sql)
    referenced_tables = [
        reference["table_name"] for reference in table_refs if reference.get("table_name")
    ]
    if not referenced_tables:
        return None

    schema_cache = get_cached_schema_snippets(
        warehouse_tools_factory,
        state,
        execution_context,
        tables=referenced_tables,
    )
    all_schema_cache = get_cached_schema_snippets(warehouse_tools_factory, state, execution_context)
    missing_columns_by_table: dict[str, set[str]] = {}
    candidate_tables_by_column: dict[str, list[str]] = {}
    tables_in_query = list(dict.fromkeys(referenced_tables))

    for qualifier, column_name in referenced_sql_identifier_refs(sql):
        resolved_table = resolve_identifier_table(
            qualifier=qualifier,
            column_name=column_name,
            table_refs=table_refs,
            schema_cache=schema_cache,
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
            matching_tables = tables_with_column(column_name, tables_in_query, schema_cache)
            if len(matching_tables) > 1:
                continue
            target_table = primary_table_name(sql) or tables_in_query[0]

        missing_columns_by_table.setdefault(target_table, set()).add(column_name)
        candidate_tables_by_column[column_name] = find_tables_with_column(
            column_name,
            all_schema_cache,
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
    best_table = best_table_for_missing_columns(missing_columns, all_schema_cache)
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
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
) -> dict[str, Any] | None:
    """Convert warehouse execution errors into prototype-style corrective feedback when possible."""
    error_text = str(error)
    missing_column_match = re.search(
        r'column "(?:[\w]+\.)?([^"]+)" does not exist',
        error_text,
        flags=re.IGNORECASE,
    )
    if missing_column_match:
        missing_column = missing_column_match.group(1).lower()
        schema_cache = get_cached_schema_snippets(warehouse_tools_factory, state, execution_context)
        candidate_tables = find_tables_with_column(missing_column, schema_cache)
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


def validate_follow_up_dimension_usage(
    warehouse_tools_factory,
    *,
    sql: str,
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
) -> dict[str, Any] | None:
    """Keep add-dimension follow-ups from succeeding without actually changing query granularity."""
    intent_decision = state["intent_decision"]
    if intent_decision.intent != DashboardChatIntent.FOLLOW_UP_SQL:
        return None
    if intent_decision.follow_up_context.follow_up_type != "add_dimension":
        return None

    requested_dimension = extract_requested_follow_up_dimension(
        intent_decision.follow_up_context.modification_instruction or state["user_query"]
    )
    if not requested_dimension:
        return None

    previous_sql = state["conversation_context"].last_sql_query or ""
    current_dimensions = structural_dimensions_from_sql(sql)
    previous_dimensions = structural_dimensions_from_sql(previous_sql)
    normalized_requested_dimension = normalize_dimension_name(requested_dimension)
    if (
        normalized_requested_dimension in current_dimensions
        and normalized_requested_dimension not in previous_dimensions
    ):
        return None

    candidate_tables = find_tables_with_column(
        requested_dimension,
        get_cached_schema_snippets(warehouse_tools_factory, state, execution_context),
    )
    return {
        "error": "requested_dimension_missing",
        "requested_dimension": requested_dimension,
        "previous_dimensions": sorted(previous_dimensions),
        "current_dimensions": sorted(current_dimensions),
        "candidate_tables": candidate_tables,
        "message": (
            f"The follow-up asked to split by '{requested_dimension}', but the SQL does not use that column. "
            "Use the requested dimension exactly, or pick a table that contains it."
        ),
    }


def check_missing_distinct(
    warehouse_tools_factory,
    sql: str,
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
) -> list[dict[str, Any]]:
    """Detect text filters that require a prior distinct-values call."""
    where_match = re.search(
        r"\bWHERE\s+(.+?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|$)",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not where_match:
        return []

    table_refs = table_references(sql)
    query_tables = [
        reference["table_name"] for reference in table_refs if reference.get("table_name")
    ]
    if not query_tables:
        return []
    primary = primary_table_name(sql) or query_tables[0]

    full_schema_cache = get_cached_schema_snippets(
        warehouse_tools_factory,
        state,
        execution_context,
        tables=query_tables,
    )
    all_schema_cache = get_cached_schema_snippets(warehouse_tools_factory, state, execution_context)

    column_types = {
        table_name: {
            str(column.get("name") or "")
            .lower(): str(column.get("data_type") or column.get("type") or "")
            .lower()
            for column in getattr(snippet, "columns", [])
        }
        for table_name, snippet in full_schema_cache.items()
    }
    missing: list[dict[str, Any]] = []
    for qualifier, column_name, value in extract_text_filter_values(where_match.group(1)):
        normalized_column = column_name.lower()
        resolved_table = resolve_identifier_table(
            qualifier=qualifier,
            column_name=normalized_column,
            table_refs=table_refs,
            schema_cache=full_schema_cache,
        )
        if resolved_table is None and qualifier is None:
            matching_tables = tables_with_column(normalized_column, query_tables, full_schema_cache)
            if len(matching_tables) > 1:
                continue
        if resolved_table is None:
            candidate_tables = find_tables_with_column(normalized_column, all_schema_cache)
            if qualifier is None and candidate_tables:
                continue
            missing.append(
                {
                    "table": primary,
                    "column": column_name,
                    "error": "column_not_in_table",
                    "candidates": candidate_tables,
                }
            )
            continue
        data_type = column_types.get(resolved_table, {}).get(normalized_column, "")
        if not data_type:
            continue
        if not is_text_type(data_type):
            continue
        if not has_validated_distinct_value(
            execution_context["distinct_cache"],
            table_name=resolved_table,
            column_name=normalized_column,
            value=value,
        ):
            missing.append({"table": resolved_table, "column": column_name, "value": value})
    return missing
