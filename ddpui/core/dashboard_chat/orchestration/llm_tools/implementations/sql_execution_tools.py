"""SQL execution orchestration for the LLM tool loop."""

import json
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder

from ddpui.core.dashboard_chat.warehouse.sql_guard import DashboardChatSqlGuard

from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.state.accessors import get_runtime_allowlist
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_corrections import (
    missing_columns_in_primary_table,
    structured_sql_execution_error,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_validation import (
    find_missing_distinct_filters,
    validate_follow_up_dimension_usage,
    validate_sql_allowlist,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.runtime.turn_context import (
    DashboardChatTurnContext,
    get_turn_warehouse_tools,
    record_validated_filters_from_sql,
)


def handle_run_sql_query_tool(
    warehouse_tools_factory,
    runtime_config,
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
) -> dict[str, Any]:
    """Validate SQL and let the tool loop self-correct on structured failures."""
    allowlist = get_runtime_allowlist(state)
    sql = str(args.get("sql") or "").strip()
    if not sql:
        return {"error": "sql_missing", "message": "SQL is required"}

    allowlist_validation = validate_sql_allowlist(sql, allowlist)
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
        turn_context=turn_context,
    )
    if follow_up_dimension_validation is not None:
        return follow_up_dimension_validation

    missing_distinct = find_missing_distinct_filters(
        warehouse_tools_factory,
        sql,
        state,
        turn_context,
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
        allowlist=allowlist,
        max_rows=runtime_config.max_query_rows,
    ).validate(sql)
    turn_context.last_sql_validation = validation
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
        turn_context=turn_context,
    )
    if missing_columns is not None:
        return missing_columns

    turn_context.last_sql = validation.sanitized_sql
    try:
        rows = get_turn_warehouse_tools(
            warehouse_tools_factory,
            turn_context,
            state,
        ).execute_sql(validation.sanitized_sql)
    except Exception as error:
        structured_error = structured_sql_execution_error(
            warehouse_tools_factory,
            sql=validation.sanitized_sql,
            error=error,
            state=state,
            turn_context=turn_context,
        )
        if structured_error is not None:
            return structured_error
        return {
            "success": False,
            "error": str(error),
            "sql_used": validation.sanitized_sql,
        }

    serialized_rows = json.loads(json.dumps(rows, cls=DjangoJSONEncoder))
    turn_context.last_sql_results = serialized_rows
    record_validated_filters_from_sql(
        turn_context=turn_context,
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
