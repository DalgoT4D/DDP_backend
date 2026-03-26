"""Tool handler implementations for dashboard chat."""

import json
import re
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder

from ddpui.core.dashboard_chat.context.allowlist import DashboardChatAllowlist
from ddpui.utils.custom_logger import CustomLogger
from ddpui.core.dashboard_chat.contracts import DashboardChatIntent
from ddpui.core.dashboard_chat.vector.documents import DashboardChatSourceType
from ddpui.core.dashboard_chat.warehouse.sql_guard import DashboardChatSqlGuard

from ..conversation import extract_requested_follow_up_dimension
from ..retrieval import (
    retrieve_vector_documents,
    filter_allowlisted_dbt_results,
    dedupe_retrieved_documents,
    build_tool_document_payload,
    get_cached_query_embedding,
)
from ..state import DashboardChatRuntimeState
from .cache import (
    get_turn_warehouse_tools,
    get_cached_schema_snippets,
    has_validated_distinct_value,
    is_text_type,
    record_validated_distinct_values,
    record_validated_filters_from_sql,
    dbt_resources_by_unique_id,
)
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

logger = CustomLogger("dashboard_chat")


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


def handle_run_sql_query_tool(
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

    allowlist_validation = _validate_sql_allowlist(sql, state["allowlist"])
    if not allowlist_validation["valid"]:
        return {
            "error": "table_not_allowed",
            "invalid_tables": allowlist_validation["invalid_tables"],
            "message": allowlist_validation["message"],
        }

    follow_up_dimension_validation = _validate_follow_up_dimension_usage(
        warehouse_tools_factory,
        sql=sql,
        state=state,
        execution_context=execution_context,
    )
    if follow_up_dimension_validation is not None:
        return follow_up_dimension_validation

    missing_distinct = _check_missing_distinct(
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

    missing_columns = _missing_columns_in_primary_table(
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
        structured_error = _structured_sql_execution_error(
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


# ---------------------------------------------------------------------------
# SQL execution (run_sql_query tool handler helpers)
# ---------------------------------------------------------------------------


def _validate_sql_allowlist(
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


def _missing_columns_in_primary_table(
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


def _structured_sql_execution_error(
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


def _validate_follow_up_dimension_usage(
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


def _check_missing_distinct(
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
