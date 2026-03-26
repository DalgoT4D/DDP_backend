"""Tool handlers and execution-context cache helpers for dashboard chat."""

from collections.abc import Sequence
import logging
import re
from typing import Any

from ddpui.core.dashboard_chat.warehouse.sql_guard import DashboardChatSqlGuard
from ddpui.core.dashboard_chat.vector.documents import DashboardChatSourceType
from ddpui.core.dashboard_chat.warehouse.tools import DashboardChatWarehouseTools
from ddpui.models.org import Org

from .retrieval import (
    retrieve_vector_documents,
    filter_allowlisted_dbt_results,
    dedupe_retrieved_documents,
    build_tool_document_payload,
    get_cached_query_embedding,
)
from .session_snapshot import persist_session_schema_cache, persist_session_distinct_cache
from .sql_parsing import (
    table_references as sql_table_references,
    resolve_identifier_table,
    tables_with_column,
    extract_text_filter_values,
    find_tables_with_column,
)
from .state import DashboardChatRuntimeState

logger = logging.getLogger(__name__)


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


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------


def handle_retrieve_docs_tool(
    vector_store,
    source_config,
    runtime_config,
    args: dict[str, Any],
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
) -> dict[str, Any]:
    """Retrieve current-dashboard, org, and dbt context using the prototype tool contract."""
    query = str(args.get("query") or state["user_query"]).strip()
    limit = max(1, min(int(args.get("limit", 8)), 20))
    requested_types = [
        str(doc_type)
        for doc_type in (args.get("types") or ["chart", "dataset", "context", "dbt_model"])
    ]
    retrieved_documents = []
    cached_embedding = get_cached_query_embedding(
        vector_store, query, execution_context["embedding_cache"]
    )

    if "chart" in requested_types:
        retrieved_documents.extend(
            retrieve_vector_documents(
                vector_store,
                runtime_config,
                org=state["org"],
                collection_name=state.get("vector_collection_name"),
                query_text=query,
                source_types=source_config.filter_enabled(
                    [DashboardChatSourceType.DASHBOARD_EXPORT]
                ),
                dashboard_id=state["dashboard_id"],
                query_embedding=cached_embedding,
            )
        )
    if "context" in requested_types:
        retrieved_documents.extend(
            retrieve_vector_documents(
                vector_store,
                runtime_config,
                org=state["org"],
                collection_name=state.get("vector_collection_name"),
                query_text=query,
                source_types=source_config.filter_enabled(
                    [DashboardChatSourceType.DASHBOARD_CONTEXT]
                ),
                dashboard_id=state["dashboard_id"],
                query_embedding=cached_embedding,
            )
        )
        retrieved_documents.extend(
            retrieve_vector_documents(
                vector_store,
                runtime_config,
                org=state["org"],
                collection_name=state.get("vector_collection_name"),
                query_text=query,
                source_types=source_config.filter_enabled([DashboardChatSourceType.ORG_CONTEXT]),
                query_embedding=cached_embedding,
            )
        )
    if "dataset" in requested_types or "dbt_model" in requested_types:
        dbt_results = retrieve_vector_documents(
            vector_store,
            runtime_config,
            org=state["org"],
            collection_name=state.get("vector_collection_name"),
            query_text=query,
            source_types=source_config.filter_enabled(
                [
                    DashboardChatSourceType.DBT_MANIFEST,
                    DashboardChatSourceType.DBT_CATALOG,
                ]
            ),
            query_embedding=cached_embedding,
        )
        retrieved_documents.extend(filter_allowlisted_dbt_results(dbt_results, state["allowlist"]))

    merged_results = dedupe_retrieved_documents(retrieved_documents)[:limit]
    for document in merged_results:
        if document.document_id in execution_context["retrieved_document_ids"]:
            continue
        execution_context["retrieved_document_ids"].add(document.document_id)
        execution_context["retrieved_documents"].append(document)

    docs = [
        build_tool_document_payload(document, state["allowlist"], state["dashboard_export"])
        for document in merged_results
    ]
    return {"docs": docs, "count": len(docs)}


def handle_get_schema_snippets_tool(
    warehouse_tools_factory,
    args: dict[str, Any],
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
) -> dict[str, Any]:
    """Return schema snippets for allowlisted tables only."""
    requested_tables = [str(table_name).lower() for table_name in args.get("tables") or []]
    allowed_tables = [
        table_name for table_name in requested_tables if state["allowlist"].is_allowed(table_name)
    ]
    filtered_tables = sorted(set(requested_tables) - set(allowed_tables))
    schema_cache = get_cached_schema_snippets(
        warehouse_tools_factory,
        state,
        execution_context,
        tables=allowed_tables,
    )
    tables_payload = [
        {"table": table_name, "columns": snippet.columns}
        for table_name, snippet in schema_cache.items()
        if table_name in allowed_tables
    ]
    response: dict[str, Any] = {"tables": tables_payload}
    if filtered_tables:
        response["filtered_tables"] = filtered_tables
        response[
            "filter_note"
        ] = f"{len(filtered_tables)} tables were filtered out because they are not used by the current dashboard."
    return response


def handle_search_dbt_models_tool(
    args: dict[str, Any],
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
) -> dict[str, Any]:
    """Search allowlisted dbt nodes by name, description, and column metadata."""
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
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
) -> dict[str, Any]:
    """Return one dbt model's description, columns, and lineage."""
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


def handle_get_distinct_values_tool(
    warehouse_tools_factory,
    args: dict[str, Any],
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
) -> dict[str, Any]:
    """Return distinct values and persist validated filter values for the session."""
    table_name = str(args.get("table") or "").lower()
    column_name = str(args.get("column") or "")
    limit = max(1, min(int(args.get("limit", 50)), 200))
    if not state["allowlist"].is_allowed(table_name):
        return {
            "error": "table_not_allowed",
            "table": table_name,
            "message": (f"Table {table_name} is not accessible in the current dashboard context."),
        }

    schema_cache = get_cached_schema_snippets(warehouse_tools_factory, state, execution_context)
    snippet = schema_cache.get(table_name)
    normalized_column_name = column_name.lower()
    if snippet is not None and normalized_column_name not in {
        str(column.get("name") or "").lower() for column in snippet.columns
    }:
        candidates = find_tables_with_column(normalized_column_name, schema_cache)
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
        execution_context,
        state["org"],
    ).get_distinct_values(
        table_name=table_name,
        column_name=column_name,
        limit=limit,
    )
    record_validated_distinct_values(
        state=state,
        execution_context=execution_context,
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
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
) -> dict[str, Any]:
    """Search allowlisted tables by table name or column name."""
    keyword = str(args.get("keyword") or "").strip().lower()
    limit = max(1, min(int(args.get("limit", 15)), 50))
    if not keyword:
        return {"tables": []}

    allowlist_tables_source = state["allowlist"].prioritized_tables() or sorted(
        state["allowlist"].allowed_tables
    )
    allowlisted_tables = list(
        dict.fromkeys(table_name.lower() for table_name in allowlist_tables_source)
    )
    direct_match_tables = [
        table_name
        for table_name in allowlisted_tables
        if keyword in table_name or keyword in table_name.rsplit(".", 1)[-1]
    ]

    schema_cache: dict[str, Any] = {}
    lookup_tables = direct_match_tables or allowlisted_tables
    if lookup_tables:
        try:
            schema_cache = get_cached_schema_snippets(
                warehouse_tools_factory,
                state,
                execution_context,
                tables=lookup_tables,
            )
        except Exception as error:
            logger.warning("Dashboard chat keyword table lookup fell back to names only: %s", error)
            execution_context["warnings"].append(str(error))

    matches: list[dict[str, Any]] = []
    seen_tables: set[str] = set()

    for table_name in direct_match_tables:
        column_names = [
            str(column.get("name") or "")
            for column in getattr(schema_cache.get(table_name), "columns", [])
        ]
        matches.append({"table": table_name, "columns": column_names[:40]})
        seen_tables.add(table_name)
        if len(matches) >= limit:
            break

    for table_name, snippet in schema_cache.items():
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
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
) -> dict[str, Any]:
    """Count rows in one allowlisted table."""
    table_name = str(args.get("table") or "").lower()
    if not state["allowlist"].is_allowed(table_name):
        return {
            "error": "table_not_allowed",
            "table": table_name,
            "message": (f"Table {table_name} is not accessible in the current dashboard context."),
        }

    sql = f"SELECT COUNT(*) AS row_count FROM {table_name} LIMIT 1"
    validation = DashboardChatSqlGuard(
        allowlist=state["allowlist"],
        max_rows=1,
    ).validate(sql)
    if not validation.is_valid or not validation.sanitized_sql:
        return {"error": "sql_validation_failed", "issues": validation.errors}

    rows = get_turn_warehouse_tools(
        warehouse_tools_factory,
        execution_context,
        state["org"],
    ).execute_sql(validation.sanitized_sql)
    row_count = 0
    if rows:
        row_count = int(rows[0].get("row_count") or 0)
    return {"table": table_name, "row_count": row_count, "has_data": row_count > 0}
