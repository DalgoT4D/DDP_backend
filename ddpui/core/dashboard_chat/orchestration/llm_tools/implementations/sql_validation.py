"""Validation helpers that gate LLM-authored SQL before execution."""

import re
from typing import Any

from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.contracts import DashboardChatIntent

from ddpui.core.dashboard_chat.orchestration.conversation_context import (
    extract_requested_follow_up_dimension,
)
from ddpui.core.dashboard_chat.contracts import (
    DashboardChatConversationContext,
    DashboardChatIntentDecision,
)
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_parsing import (
    extract_text_filter_values,
    find_tables_with_column,
    normalize_dimension_name,
    primary_table_name,
    resolve_identifier_table,
    structural_dimensions_from_sql,
    table_references,
    tables_with_column,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.runtime.turn_context import (
    DashboardChatTurnContext,
    get_or_load_schema_snippets,
    has_validated_distinct_value,
    is_text_type,
)
from ddpui.core.dashboard_chat.warehouse.sql_guard import DashboardChatSqlGuard


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


def validate_follow_up_dimension_usage(
    warehouse_tools_factory,
    *,
    sql: str,
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
) -> dict[str, Any] | None:
    """Keep add-dimension follow-ups from succeeding without changing query granularity."""
    intent_decision = DashboardChatIntentDecision.model_validate(state.get("intent_decision") or {})
    if intent_decision.intent != DashboardChatIntent.FOLLOW_UP_SQL:
        return None
    if intent_decision.follow_up_context.follow_up_type != "add_dimension":
        return None

    requested_dimension = extract_requested_follow_up_dimension(
        intent_decision.follow_up_context.modification_instruction or state["user_query"]
    )
    if not requested_dimension:
        return None

    previous_sql = (
        DashboardChatConversationContext.model_validate(
            state.get("conversation_context") or {}
        ).last_sql_query
        or ""
    )
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
        get_or_load_schema_snippets(warehouse_tools_factory, state, turn_context),
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


def find_missing_distinct_filters(
    warehouse_tools_factory,
    sql: str,
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
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

    full_schema_snippets_by_table = get_or_load_schema_snippets(
        warehouse_tools_factory,
        state,
        turn_context,
        tables=query_tables,
    )
    all_schema_snippets_by_table = get_or_load_schema_snippets(
        warehouse_tools_factory, state, turn_context
    )

    column_types = {
        table_name: {
            str(column.get("name") or "")
            .lower(): str(column.get("data_type") or column.get("type") or "")
            .lower()
            for column in getattr(snippet, "columns", [])
        }
        for table_name, snippet in full_schema_snippets_by_table.items()
    }
    missing: list[dict[str, Any]] = []
    for qualifier, column_name, value in extract_text_filter_values(where_match.group(1)):
        normalized_column = column_name.lower()
        resolved_table = resolve_identifier_table(
            qualifier=qualifier,
            column_name=normalized_column,
            table_refs=table_refs,
            schema_snippets_by_table=full_schema_snippets_by_table,
        )
        if resolved_table is None and qualifier is None:
            matching_tables = tables_with_column(
                normalized_column, query_tables, full_schema_snippets_by_table
            )
            if len(matching_tables) > 1:
                continue
        if resolved_table is None:
            candidate_tables = find_tables_with_column(
                normalized_column, all_schema_snippets_by_table
            )
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
        if not data_type or not is_text_type(data_type):
            continue
        if not has_validated_distinct_value(
            turn_context.validated_distinct_values,
            table_name=resolved_table,
            column_name=normalized_column,
            value=value,
        ):
            missing.append({"table": resolved_table, "column": column_name, "value": value})
    return missing
