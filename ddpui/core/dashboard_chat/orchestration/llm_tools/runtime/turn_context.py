"""Turn-scoped state helpers for dashboard chat tool execution."""

from collections.abc import Sequence
from dataclasses import dataclass, field
import re
from typing import Any

from ddpui.core.dashboard_chat.contracts import DashboardChatRetrievedDocument
from ddpui.core.dashboard_chat.contracts.sql_contracts import DashboardChatSqlValidationResult
from ddpui.core.dashboard_chat.warehouse.warehouse_access_tools import DashboardChatWarehouseTools
from ddpui.utils.custom_logger import CustomLogger

from ddpui.core.dashboard_chat.contracts.retrieval_contracts import DashboardChatSchemaSnippet
from ddpui.core.dashboard_chat.orchestration.retrieval_support import get_or_embed_query
from ddpui.core.dashboard_chat.contracts import DashboardChatConversationContext
from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import DashboardChatAllowlist
from ddpui.models.org import Org
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_parsing import (
    extract_text_filter_values,
    resolve_identifier_table,
    table_references,
    tables_with_column,
)

logger = CustomLogger("dashboard_chat")


@dataclass
class DashboardChatTurnContext:
    """Ephemeral per-turn execution context kept outside checkpointed graph state."""

    validated_distinct_values: set[tuple[str, str, str]]
    query_embeddings: dict[str, list[float]]
    schema_snippets_by_table: dict[str, Any]
    retrieved_documents: list[DashboardChatRetrievedDocument] = field(default_factory=list)
    retrieved_document_ids: set[str] = field(default_factory=set)
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    warehouse_tools: DashboardChatWarehouseTools | None = None
    last_sql: str | None = None
    last_sql_results: list[dict[str, Any]] | None = None
    last_sql_validation: DashboardChatSqlValidationResult | None = None
    timing_breakdown: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_state(
        cls,
        state: DashboardChatGraphState,
        *,
        initial_query_embeddings: dict[str, list[float]] | None = None,
    ) -> "DashboardChatTurnContext":
        """Build a fresh turn context from checkpointed state plus per-run inputs."""
        return cls(
            validated_distinct_values=hydrate_validated_distinct_values(state),
            query_embeddings=dict(initial_query_embeddings or {}),
            schema_snippets_by_table=hydrate_schema_snippets_by_table(state),
            warnings=list(state.get("warnings", [])),
            timing_breakdown={
                "tool_calls_ms": list(
                    (state.get("timing_breakdown") or {}).get("tool_calls_ms") or []
                ),
            },
        )


def get_turn_warehouse_tools(
    warehouse_tools_factory,
    turn_context: DashboardChatTurnContext,
    state: DashboardChatGraphState,
) -> DashboardChatWarehouseTools:
    """Build the warehouse tool helper lazily for the turn."""
    warehouse_tools = turn_context.warehouse_tools
    if warehouse_tools is None:
        warehouse_tools = warehouse_tools_factory(
            Org.objects.select_related("dbt").get(id=int(state["org_id"]))
        )
        turn_context.warehouse_tools = warehouse_tools
    return warehouse_tools


def hydrate_schema_snippets_by_table(state: DashboardChatGraphState) -> dict[str, Any]:
    """Rebuild schema snippets from checkpoint payloads for one turn."""
    return {
        k: DashboardChatSchemaSnippet.model_validate(v)
        for k, v in (state.get("schema_snippet_payloads") or {}).items()
    }


def hydrate_validated_distinct_values(state: DashboardChatGraphState) -> set[tuple[str, str, str]]:
    """Rebuild validated distinct values from checkpoint payloads for one turn."""
    validated: set[tuple[str, str, str]] = set()
    for table_name, column_map in (state.get("validated_distinct_payloads") or {}).items():
        for column_name, values in (column_map or {}).items():
            for value in values or []:
                validated.add((str(table_name).lower(), str(column_name).lower(), str(value)))
    return validated


def current_schema_snippet_payloads(turn_context: DashboardChatTurnContext) -> dict[str, Any]:
    """Serialize the current turn's schema snippets back into checkpoint-safe payloads."""
    return {k: v.model_dump(mode="json") for k, v in turn_context.schema_snippets_by_table.items()}


def current_validated_distinct_payloads(turn_context: DashboardChatTurnContext) -> dict[str, Any]:
    """Serialize the current turn's validated distinct values back into checkpoint-safe payloads."""
    serialized: dict[str, dict[str, list[str]]] = {}
    for table_name, column_name, value in turn_context.validated_distinct_values:
        serialized.setdefault(table_name, {}).setdefault(column_name, []).append(value)
    return {
        table_name: {column_name: sorted(set(values)) for column_name, values in column_map.items()}
        for table_name, column_map in serialized.items()
    }


def get_or_load_schema_snippets(
    warehouse_tools_factory,
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
    tables: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Load and keep schema snippets in the current turn state."""
    allowlist = DashboardChatAllowlist.model_validate(state.get("allowlist_payload") or {})
    requested_tables = [
        table_name.lower()
        for table_name in (tables if tables is not None else allowlist.prioritized_tables())
        if allowlist.is_allowed(table_name)
    ]
    cache = turn_context.schema_snippets_by_table
    missing_tables = [table_name for table_name in requested_tables if table_name not in cache]
    if missing_tables:
        snippets = get_turn_warehouse_tools(
            warehouse_tools_factory,
            turn_context,
            state,
        ).get_schema_snippets(missing_tables)
        for table_name, snippet in snippets.items():
            cache[table_name.lower()] = snippet
    if tables is None:
        return cache
    return {table_name: cache[table_name] for table_name in requested_tables if table_name in cache}


def normalize_distinct_value(value: Any) -> str:
    """Normalize one distinct value for exact cache lookups."""
    return str(value).strip().lower()


def has_validated_distinct_value(
    validated_distinct_values: set[tuple[Any, ...]],
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
        (normalized_table, normalized_column, normalized_value) in validated_distinct_values
        or ("*", normalized_column, normalized_value) in validated_distinct_values
        or (normalized_table, normalized_column) in validated_distinct_values
        or ("*", normalized_column) in validated_distinct_values
    )


def is_text_type(data_type: str) -> bool:
    """Treat common string-like warehouse types as requiring distinct-value lookup."""
    return any(token in data_type for token in ["char", "text", "string", "varchar"])


def record_validated_distinct_values(
    *,
    turn_context: DashboardChatTurnContext,
    table_name: str,
    column_name: str,
    values: Sequence[Any],
) -> None:
    """Persist exact validated filter values into the current turn state."""
    normalized_table = table_name.lower()
    normalized_column = column_name.lower()
    validated_distinct_values = turn_context.validated_distinct_values
    for value in values:
        normalized_value = normalize_distinct_value(value)
        validated_distinct_values.add((normalized_table, normalized_column, normalized_value))
        validated_distinct_values.add(("*", normalized_column, normalized_value))


def record_validated_filters_from_sql(
    *,
    turn_context: DashboardChatTurnContext,
    sql: str,
) -> None:
    """Seed exact validated filter values from a successful SQL statement."""
    table_refs = table_references(sql)
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
    schema_snippets_by_table = dict(turn_context.schema_snippets_by_table)
    values_by_target: dict[tuple[str, str], list[str]] = {}
    for qualifier, column_name, value in extract_text_filter_values(where_match.group(1)):
        normalized_column = column_name.lower()
        resolved_table = resolve_identifier_table(
            qualifier=qualifier,
            column_name=normalized_column,
            table_refs=table_refs,
            schema_snippets_by_table=schema_snippets_by_table,
        )
        if resolved_table is None and qualifier is None:
            if schema_snippets_by_table:
                matching = tables_with_column(
                    normalized_column, query_tables, schema_snippets_by_table
                )
                if len(matching) == 1:
                    resolved_table = matching[0]
            elif len(query_tables) == 1:
                resolved_table = query_tables[0]
        values_by_target.setdefault((resolved_table or "*", normalized_column), []).append(value)

    for (tbl, col), vals in values_by_target.items():
        record_validated_distinct_values(
            turn_context=turn_context,
            table_name=tbl,
            column_name=col,
            values=vals,
        )


def seed_validated_distinct_values_from_previous_sql(
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
) -> None:
    """Treat text filters from the previous successful SQL as already validated for follow-ups."""
    previous_sql = DashboardChatConversationContext.model_validate(
        state.get("conversation_context") or {}
    ).last_sql_query
    if not previous_sql:
        return
    record_validated_filters_from_sql(
        turn_context=turn_context,
        sql=previous_sql,
    )


def dbt_resources_by_unique_id(state: DashboardChatGraphState) -> dict[str, dict[str, Any]]:
    """Return the allowlisted dbt index built at session start."""
    dbt_index = state.get("dbt_index") or {}
    return dict(dbt_index.get("resources_by_unique_id") or {})


__all__ = [
    "DashboardChatTurnContext",
    "current_validated_distinct_payloads",
    "current_schema_snippet_payloads",
    "dbt_resources_by_unique_id",
    "get_or_embed_query",
    "get_or_load_schema_snippets",
    "get_turn_warehouse_tools",
    "has_validated_distinct_value",
    "hydrate_validated_distinct_values",
    "hydrate_schema_snippets_by_table",
    "is_text_type",
    "record_validated_distinct_values",
    "record_validated_filters_from_sql",
    "seed_validated_distinct_values_from_previous_sql",
]
