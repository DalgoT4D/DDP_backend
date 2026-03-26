"""Execution-context cache helpers for dashboard chat tool loop."""

from collections.abc import Sequence
import re
from typing import Any

from ddpui.core.dashboard_chat.warehouse.tools import DashboardChatWarehouseTools
from ddpui.models.org import Org
from ddpui.utils.custom_logger import CustomLogger

from ddpui.core.dashboard_chat.orchestration.retrieval import (
    retrieve_vector_documents,
    filter_allowlisted_dbt_results,
    dedupe_retrieved_documents,
    build_tool_document_payload,
    get_cached_query_embedding,
)
from ddpui.core.dashboard_chat.orchestration.session_snapshot import (
    persist_session_schema_cache,
    persist_session_distinct_cache,
)
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatRuntimeState
from ddpui.core.dashboard_chat.orchestration.tools.sql_parsing import (
    table_references as sql_table_references,
    resolve_identifier_table,
    tables_with_column,
    extract_text_filter_values,
)

logger = CustomLogger("dashboard_chat")


# ---------------------------------------------------------------------------
# Warehouse tools (lazily initialized per-turn)
# ---------------------------------------------------------------------------


def get_turn_warehouse_tools(
    warehouse_tools_factory,
    execution_context: dict[str, Any],
    org: Org,
) -> DashboardChatWarehouseTools:
    """Build the warehouse tool helper lazily for the turn."""
    warehouse_tools = execution_context.get("warehouse_tools")
    if warehouse_tools is None:
        warehouse_tools = warehouse_tools_factory(org)
        execution_context["warehouse_tools"] = warehouse_tools
    return warehouse_tools


# ---------------------------------------------------------------------------
# Schema snippet cache
# ---------------------------------------------------------------------------


def get_cached_schema_snippets(
    warehouse_tools_factory,
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
    tables: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Load and cache schema snippets for allowlisted tables."""
    requested_tables = [
        table_name.lower()
        for table_name in (
            tables if tables is not None else state["allowlist"].prioritized_tables()
        )
        if state["allowlist"].is_allowed(table_name)
    ]
    cache = execution_context["schema_cache"]
    missing_tables = [table_name for table_name in requested_tables if table_name not in cache]
    if missing_tables:
        snippets = get_turn_warehouse_tools(
            warehouse_tools_factory,
            execution_context,
            state["org"],
        ).get_schema_snippets(missing_tables)
        for table_name, snippet in snippets.items():
            cache[table_name.lower()] = snippet
        if snippets:
            persist_session_schema_cache(state, cache)
    if tables is None:
        return cache
    return {table_name: cache[table_name] for table_name in requested_tables if table_name in cache}


# ---------------------------------------------------------------------------
# Distinct value cache helpers
# ---------------------------------------------------------------------------


def normalize_distinct_value(value: Any) -> str:
    """Normalize one distinct value for exact cache lookups."""
    return str(value).strip().lower()


def has_validated_distinct_value(
    distinct_cache: set[tuple[Any, ...]],
    *,
    table_name: str,
    column_name: str,
    value: Any,
) -> bool:
    """Return whether this exact text filter value was already validated in-session."""
    normalized_value = normalize_distinct_value(value)
    normalized_column = column_name.lower()
    normalized_table = table_name.lower()
    return (
        (normalized_table, normalized_column, normalized_value) in distinct_cache
        or ("*", normalized_column, normalized_value) in distinct_cache
        or (normalized_table, normalized_column) in distinct_cache
        or ("*", normalized_column) in distinct_cache
    )


def is_text_type(data_type: str) -> bool:
    """Treat common string-like warehouse types as requiring distinct-value lookup."""
    return any(token in data_type for token in ["char", "text", "string", "varchar"])


def record_validated_distinct_values(
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
        normalized_value = normalize_distinct_value(value)
        distinct_cache.add((normalized_table, normalized_column, normalized_value))
        distinct_cache.add(("*", normalized_column, normalized_value))
    persist_session_distinct_cache(state, distinct_cache)


def record_validated_filters_from_sql(
    *,
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
    sql: str,
) -> None:
    """Seed exact validated filter values from a successful SQL statement."""
    table_refs = sql_table_references(sql)
    if not table_refs:
        return
    where_match = re.search(
        r"\bWHERE\s+(.+?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|$)",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not where_match:
        return

    query_tables = [
        reference["table_name"] for reference in table_refs if reference.get("table_name")
    ]
    schema_cache = dict(execution_context.get("schema_cache") or {})
    values_by_target: dict[tuple[str, str], list[str]] = {}
    for qualifier, column_name, value in extract_text_filter_values(where_match.group(1)):
        normalized_column = column_name.lower()
        resolved_table = resolve_identifier_table(
            qualifier=qualifier,
            column_name=normalized_column,
            table_refs=table_refs,
            schema_cache=schema_cache,
        )
        if resolved_table is None and qualifier is None:
            if schema_cache:
                matching = tables_with_column(normalized_column, query_tables, schema_cache)
                if len(matching) == 1:
                    resolved_table = matching[0]
            elif len(query_tables) == 1:
                resolved_table = query_tables[0]
        values_by_target.setdefault((resolved_table or "*", normalized_column), []).append(value)

    for (tbl, col), vals in values_by_target.items():
        record_validated_distinct_values(
            state=state,
            execution_context=execution_context,
            table_name=tbl,
            column_name=col,
            values=vals,
        )


def seed_distinct_cache_from_previous_sql(
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
) -> None:
    """Treat text filters from the previous successful SQL as already validated for follow-ups."""
    previous_sql = state["conversation_context"].last_sql_query
    if not previous_sql:
        return
    record_validated_filters_from_sql(
        state=state,
        execution_context=execution_context,
        sql=previous_sql,
    )


# ---------------------------------------------------------------------------
# dbt index helper
# ---------------------------------------------------------------------------


def dbt_resources_by_unique_id(state: DashboardChatRuntimeState) -> dict[str, dict[str, Any]]:
    """Return the allowlisted dbt index built at session start."""
    dbt_index = state.get("dbt_index") or {}
    return dict(dbt_index.get("resources_by_unique_id") or {})
