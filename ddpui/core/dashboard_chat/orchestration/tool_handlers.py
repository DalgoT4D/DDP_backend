"""Tool handlers and turn-scoped tool helpers for dashboard chat."""

from collections.abc import Sequence
import logging
from typing import Any

from ddpui.core.dashboard_chat.warehouse.sql_guard import DashboardChatSqlGuard
from ddpui.core.dashboard_chat.vector.documents import DashboardChatSourceType
from ddpui.core.dashboard_chat.warehouse.tools import DashboardChatWarehouseTools
from ddpui.models.org import Org

from .state import DashboardChatRuntimeState

logger = logging.getLogger(__name__)


def _handle_retrieve_docs_tool(
    self,
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

    if "chart" in requested_types:
        retrieved_documents.extend(
            self._retrieve_vector_documents(
                org=state["org"],
                collection_name=state.get("vector_collection_name"),
                query_text=query,
                source_types=self.source_config.filter_enabled(
                    [DashboardChatSourceType.DASHBOARD_EXPORT]
                ),
                dashboard_id=state["dashboard_id"],
                query_embedding=self._get_cached_query_embedding(
                    query,
                    execution_context["embedding_cache"],
                ),
            )
        )
    if "context" in requested_types:
        retrieved_documents.extend(
            self._retrieve_vector_documents(
                org=state["org"],
                collection_name=state.get("vector_collection_name"),
                query_text=query,
                source_types=self.source_config.filter_enabled(
                    [DashboardChatSourceType.DASHBOARD_CONTEXT]
                ),
                dashboard_id=state["dashboard_id"],
                query_embedding=self._get_cached_query_embedding(
                    query,
                    execution_context["embedding_cache"],
                ),
            )
        )
        retrieved_documents.extend(
            self._retrieve_vector_documents(
                org=state["org"],
                collection_name=state.get("vector_collection_name"),
                query_text=query,
                source_types=self.source_config.filter_enabled(
                    [DashboardChatSourceType.ORG_CONTEXT]
                ),
                query_embedding=self._get_cached_query_embedding(
                    query,
                    execution_context["embedding_cache"],
                ),
            )
        )
    if "dataset" in requested_types or "dbt_model" in requested_types:
        dbt_results = self._retrieve_vector_documents(
            org=state["org"],
            collection_name=state.get("vector_collection_name"),
            query_text=query,
            source_types=self.source_config.filter_enabled(
                [
                    DashboardChatSourceType.DBT_MANIFEST,
                    DashboardChatSourceType.DBT_CATALOG,
                ]
            ),
            query_embedding=self._get_cached_query_embedding(
                query,
                execution_context["embedding_cache"],
            ),
        )
        retrieved_documents.extend(
            self._filter_allowlisted_dbt_results(dbt_results, state["allowlist"])
        )

    merged_results = self._dedupe_retrieved_documents(retrieved_documents)[:limit]
    for document in merged_results:
        if document.document_id in execution_context["retrieved_document_ids"]:
            continue
        execution_context["retrieved_document_ids"].add(document.document_id)
        execution_context["retrieved_documents"].append(document)

    docs = [
        self._build_tool_document_payload(
            document,
            state["allowlist"],
            state["dashboard_export"],
        )
        for document in merged_results
    ]
    return {"docs": docs, "count": len(docs)}


def _handle_get_schema_snippets_tool(
    self,
    args: dict[str, Any],
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
) -> dict[str, Any]:
    """Return schema snippets for allowlisted tables only."""
    requested_tables = [str(table_name).lower() for table_name in args.get("tables") or []]
    allowed_tables = [
        table_name
        for table_name in requested_tables
        if state["allowlist"].is_allowed(table_name)
    ]
    filtered_tables = sorted(set(requested_tables) - set(allowed_tables))
    schema_cache = self._get_cached_schema_snippets(
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
        response["filter_note"] = (
            f"{len(filtered_tables)} tables were filtered out because they are not used by the current dashboard."
        )
    return response


def _handle_search_dbt_models_tool(
    self,
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
    for node in self._dbt_resources_by_unique_id(state).values():
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
                    str(column.get("name") or "")
                    for column in (node.get("columns") or [])
                ][:20],
                "table": table_name,
            }
        )
        if len(results) >= limit:
            break

    return {"models": results, "count": len(results)}


def _handle_get_dbt_model_info_tool(
    self,
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
    for unique_id, node in self._dbt_resources_by_unique_id(state).items():
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


def _handle_get_distinct_values_tool(
    self,
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
            "message": (
                f"Table {table_name} is not accessible in the current dashboard context."
            ),
        }

    schema_cache = self._get_cached_schema_snippets(state, execution_context)
    snippet = schema_cache.get(table_name)
    normalized_column_name = column_name.lower()
    if snippet is not None and normalized_column_name not in {
        str(column.get("name") or "").lower() for column in snippet.columns
    }:
        candidates = self._find_tables_with_column(normalized_column_name, schema_cache)
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

    values = self._get_turn_warehouse_tools(
        execution_context,
        state["org"],
    ).get_distinct_values(
        table_name=table_name,
        column_name=column_name,
        limit=limit,
    )
    self._record_validated_distinct_values(
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


def _handle_list_tables_by_keyword_tool(
    self,
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
            schema_cache = self._get_cached_schema_snippets(
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


def _handle_check_table_row_count_tool(
    self,
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
            "message": (
                f"Table {table_name} is not accessible in the current dashboard context."
            ),
        }

    sql = f"SELECT COUNT(*) AS row_count FROM {table_name} LIMIT 1"
    validation = DashboardChatSqlGuard(
        allowlist=state["allowlist"],
        max_rows=1,
    ).validate(sql)
    if not validation.is_valid or not validation.sanitized_sql:
        return {"error": "sql_validation_failed", "issues": validation.errors}

    rows = self._get_turn_warehouse_tools(
        execution_context,
        state["org"],
    ).execute_sql(
        validation.sanitized_sql
    )
    row_count = 0
    if rows:
        row_count = int(rows[0].get("row_count") or 0)
    return {"table": table_name, "row_count": row_count, "has_data": row_count > 0}


def _get_turn_warehouse_tools(
    self,
    execution_context: dict[str, Any],
    org: Org,
) -> DashboardChatWarehouseTools:
    """Build the warehouse tool helper lazily for the turn."""
    warehouse_tools = execution_context.get("warehouse_tools")
    if warehouse_tools is None:
        warehouse_tools = self.warehouse_tools_factory(org)
        execution_context["warehouse_tools"] = warehouse_tools
    return warehouse_tools


def _get_cached_schema_snippets(
    self,
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
        snippets = self._get_turn_warehouse_tools(
            execution_context,
            state["org"],
        ).get_schema_snippets(missing_tables)
        for table_name, snippet in snippets.items():
            cache[table_name.lower()] = snippet
        if snippets:
            self._persist_session_schema_cache(state, cache)
    if tables is None:
        return cache
    return {
        table_name: cache[table_name]
        for table_name in requested_tables
        if table_name in cache
    }


def _seed_distinct_cache_from_previous_sql(
    self,
    state: DashboardChatRuntimeState,
    execution_context: dict[str, Any],
) -> None:
    """Treat text filters from the previous successful SQL as already validated for follow-ups."""
    previous_sql = state["conversation_context"].last_sql_query
    if not previous_sql:
        return

    self._record_validated_filters_from_sql(
        state=state,
        execution_context=execution_context,
        sql=previous_sql,
    )


def _dbt_resources_by_unique_id(
    state: DashboardChatRuntimeState,
) -> dict[str, dict[str, Any]]:
    """Return the allowlisted dbt index built at session start."""
    dbt_index = state.get("dbt_index") or {}
    return dict(dbt_index.get("resources_by_unique_id") or {})


def _get_cached_query_embedding(
    self,
    query_text: str,
    embedding_cache: dict[str, list[float]],
) -> list[float]:
    """Cache embeddings per query string during one turn."""
    if query_text not in embedding_cache:
        embedding_cache[query_text] = self.vector_store.embed_query(query_text)
    return embedding_cache[query_text]
