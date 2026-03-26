"""SQL execution and guardrail helpers for dashboard chat graph execution."""

from collections.abc import Sequence
import json
import re
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder

from ddpui.core.dashboard_chat.context.allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.contracts import DashboardChatIntent
from ddpui.core.dashboard_chat.warehouse.sql_guard import DashboardChatSqlGuard

from .state import DashboardChatRuntimeState


def _validate_sql_allowlist(
    self,
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


def _run_sql_with_distinct_guard(
    self,
    args: dict[str, Any],
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
) -> dict[str, Any]:
    """Validate SQL like the prototype and let the tool loop self-correct on failures."""
    sql = str(args.get("sql") or "").strip()
    if not sql:
        return {"error": "sql_missing", "message": "SQL is required"}

    allowlist_validation = self._validate_sql_allowlist(sql, state["allowlist"])
    if not allowlist_validation["valid"]:
        return {
            "error": "table_not_allowed",
            "invalid_tables": allowlist_validation["invalid_tables"],
            "message": allowlist_validation["message"],
        }

    follow_up_dimension_validation = self._validate_follow_up_dimension_usage(
        sql=sql,
        state=state,
        execution_context=execution_context,
    )
    if follow_up_dimension_validation is not None:
        return follow_up_dimension_validation
    missing_distinct = self._missing_distinct(sql, state, execution_context)
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
        max_rows=self.runtime_config.max_query_rows,
    ).validate(sql)
    execution_context["last_sql_validation"] = validation
    if not validation.is_valid or not validation.sanitized_sql:
        return {
            "error": "sql_validation_failed",
            "issues": validation.errors,
            "warnings": validation.warnings,
        }

    missing_columns = self._missing_columns_in_primary_table(
        sql=validation.sanitized_sql,
        state=state,
        execution_context=execution_context,
    )
    if missing_columns is not None:
        return missing_columns

    execution_context["last_sql"] = validation.sanitized_sql
    try:
        rows = self._get_turn_warehouse_tools(
            execution_context,
            state["org"],
        ).execute_sql(
            validation.sanitized_sql
        )
    except Exception as error:
        structured_error = self._structured_sql_execution_error(
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
    self._record_validated_filters_from_sql(
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


def _missing_columns_in_primary_table(
    self,
    *,
    sql: str,
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
) -> dict[str, Any] | None:
    """Return a corrective tool error when SQL references columns absent from the referenced query tables."""
    table_references = self._table_references(sql)
    referenced_tables = [
        reference["table_name"]
        for reference in table_references
        if reference.get("table_name")
    ]
    if not referenced_tables:
        return None

    schema_cache = self._get_cached_schema_snippets(
        state,
        execution_context,
        tables=referenced_tables,
    )
    all_schema_cache = self._get_cached_schema_snippets(state, execution_context)
    missing_columns_by_table: dict[str, set[str]] = {}
    candidate_tables_by_column: dict[str, list[str]] = {}
    tables_in_query = list(dict.fromkeys(referenced_tables))

    for qualifier, column_name in self._referenced_sql_identifier_refs(sql):
        resolved_table = self._resolve_identifier_table(
            qualifier=qualifier,
            column_name=column_name,
            table_references=table_references,
            schema_cache=schema_cache,
        )
        if resolved_table is not None:
            continue

        if qualifier is not None:
            target_table = (
                self._resolve_table_qualifier(qualifier, table_references)
                or self._primary_table_name(sql)
                or tables_in_query[0]
            )
        else:
            matching_tables = self._tables_with_column(
                column_name,
                tables_in_query,
                schema_cache,
            )
            if len(matching_tables) > 1:
                continue
            target_table = self._primary_table_name(sql) or tables_in_query[0]

        missing_columns_by_table.setdefault(target_table, set()).add(column_name)
        candidate_tables_by_column[column_name] = self._find_tables_with_column(
            column_name,
            all_schema_cache,
        )

    missing_columns = sorted(
        {
            column_name
            for columns in missing_columns_by_table.values()
            for column_name in columns
        }
    )
    if not missing_columns:
        return None

    primary_table = self._primary_table_name(sql) or tables_in_query[0]
    target_table = (
        next(iter(missing_columns_by_table))
        if len(missing_columns_by_table) == 1
        else primary_table
    )
    best_table = self._best_table_for_missing_columns(
        missing_columns,
        all_schema_cache,
    )
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


def _structured_sql_execution_error(
    self,
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
        schema_cache = self._get_cached_schema_snippets(state, execution_context)
        candidate_tables = self._find_tables_with_column(missing_column, schema_cache)
        return {
            "error": "column_not_in_table",
            "table": self._primary_table_name(sql),
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


def _validate_follow_up_dimension_usage(
    self,
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

    requested_dimension = self._extract_requested_follow_up_dimension(
        intent_decision.follow_up_context.modification_instruction or state["user_query"]
    )
    if not requested_dimension:
        return None

    previous_sql = state["conversation_context"].last_sql_query or ""
    current_dimensions = self._structural_dimensions_from_sql(sql)
    previous_dimensions = self._structural_dimensions_from_sql(previous_sql)
    normalized_requested_dimension = self._normalize_dimension_name(requested_dimension)
    if (
        normalized_requested_dimension in current_dimensions
        and normalized_requested_dimension not in previous_dimensions
    ):
        return None

    candidate_tables = self._find_tables_with_column(
        requested_dimension,
        self._get_cached_schema_snippets(state, execution_context),
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


def _missing_distinct(
    self,
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

    table_references = self._table_references(sql)
    query_tables = [
        reference["table_name"]
        for reference in table_references
        if reference.get("table_name")
    ]
    if not query_tables:
        return []
    primary_table = self._primary_table_name(sql) or query_tables[0]

    full_schema_cache = self._get_cached_schema_snippets(
        state,
        execution_context,
        tables=query_tables,
    )
    all_schema_cache = self._get_cached_schema_snippets(state, execution_context)

    column_types = {
        table_name: {
            str(column.get("name") or "").lower(): str(
                column.get("data_type") or column.get("type") or ""
            ).lower()
            for column in getattr(snippet, "columns", [])
        }
        for table_name, snippet in full_schema_cache.items()
    }
    missing: list[dict[str, Any]] = []
    for qualifier, column_name, value in self._extract_text_filter_values(where_match.group(1)):
        normalized_column = column_name.lower()
        resolved_table = self._resolve_identifier_table(
            qualifier=qualifier,
            column_name=normalized_column,
            table_references=table_references,
            schema_cache=full_schema_cache,
        )
        if resolved_table is None and qualifier is None:
            matching_tables = self._tables_with_column(
                normalized_column,
                query_tables,
                full_schema_cache,
            )
            if len(matching_tables) > 1:
                continue
        if resolved_table is None:
            candidate_tables = self._find_tables_with_column(
                normalized_column,
                all_schema_cache,
            )
            if qualifier is None and candidate_tables:
                continue
            missing.append(
                {
                    "table": primary_table,
                    "column": column_name,
                    "error": "column_not_in_table",
                    "candidates": candidate_tables,
                }
            )
            continue
        data_type = column_types.get(resolved_table, {}).get(normalized_column, "")
        if not data_type:
            continue
        if not self._is_text_type(data_type):
            continue
        if (
            not self._has_validated_distinct_value(
                execution_context["distinct_cache"],
                table_name=resolved_table,
                column_name=normalized_column,
                value=value,
            )
        ):
            missing.append(
                {"table": resolved_table, "column": column_name, "value": value}
            )
    return missing


def _normalize_distinct_value(value: Any) -> str:
    """Normalize one distinct value for exact cache lookups."""
    return str(value).strip().lower()


def _has_validated_distinct_value(
    cls,
    distinct_cache: set[tuple[Any, ...]],
    *,
    table_name: str,
    column_name: str,
    value: Any,
) -> bool:
    """Return whether this exact text filter value was already validated in-session."""
    normalized_value = cls._normalize_distinct_value(value)
    normalized_column = column_name.lower()
    normalized_table = table_name.lower()
    return (
        (normalized_table, normalized_column, normalized_value) in distinct_cache
        or ("*", normalized_column, normalized_value) in distinct_cache
        or (normalized_table, normalized_column) in distinct_cache
        or ("*", normalized_column) in distinct_cache
    )


def _is_text_type(data_type: str) -> bool:
    """Treat common string-like warehouse types as requiring distinct-value lookup."""
    return any(text_token in data_type for text_token in ["char", "text", "string", "varchar"])


def _record_validated_distinct_values(
    self,
    *,
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
    table_name: str,
    column_name: str,
    values: Sequence[Any],
) -> None:
    """Persist exact validated filter values for the current session."""
    normalized_table = table_name.lower()
    normalized_column = column_name.lower()
    distinct_cache = execution_context["distinct_cache"]
    for value in values:
        normalized_value = self._normalize_distinct_value(value)
        distinct_cache.add((normalized_table, normalized_column, normalized_value))
        distinct_cache.add(("*", normalized_column, normalized_value))
    self._persist_session_distinct_cache(state, distinct_cache)


def _record_validated_filters_from_sql(
    self,
    *,
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
    sql: str,
) -> None:
    """Seed exact validated filter values from a successful SQL statement."""
    table_references = self._table_references(sql)
    if not table_references:
        return
    where_match = re.search(
        r"\bWHERE\s+(.+?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|$)",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not where_match:
        return

    query_tables = [
        reference["table_name"]
        for reference in table_references
        if reference.get("table_name")
    ]
    schema_cache = dict(execution_context.get("schema_cache") or {})
    values_by_target: dict[tuple[str, str], list[str]] = {}
    for qualifier, column_name, value in self._extract_text_filter_values(where_match.group(1)):
        normalized_column = column_name.lower()
        resolved_table = self._resolve_identifier_table(
            qualifier=qualifier,
            column_name=normalized_column,
            table_references=table_references,
            schema_cache=schema_cache,
        )
        if resolved_table is None and qualifier is None:
            if schema_cache:
                matching_tables = self._tables_with_column(
                    normalized_column,
                    query_tables,
                    schema_cache,
                )
                if len(matching_tables) == 1:
                    resolved_table = matching_tables[0]
            elif len(query_tables) == 1:
                resolved_table = query_tables[0]
        values_by_target.setdefault((resolved_table or "*", normalized_column), []).append(value)

    if not values_by_target:
        return

    for (table_name, column_name), values in values_by_target.items():
        self._record_validated_distinct_values(
            state=state,
            execution_context=execution_context,
            table_name=table_name,
            column_name=column_name,
            values=values,
        )
