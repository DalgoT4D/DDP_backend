"""Turn-scoped state helpers for dashboard chat tool execution."""

from collections.abc import Sequence
from dataclasses import dataclass, field
import re
from typing import Any

from ddpui.core.dashboard_chat.contracts.conversation_contracts import (
    DashboardChatConversationContext,
)
from ddpui.core.dashboard_chat.contracts.retrieval_contracts import (
    DashboardChatRetrievedDocument,
    DashboardChatSchemaSnippet,
)
from ddpui.core.dashboard_chat.contracts.sql_contracts import DashboardChatSqlValidationResult
from ddpui.core.dashboard_chat.warehouse.warehouse_access_tools import DashboardChatWarehouseTools
from ddpui.utils.custom_logger import CustomLogger

from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import (
    find_matching_dashboard_chat_table_name,
    normalize_dashboard_chat_table_name,
)
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
    schema_snippets_by_table: dict[str, Any]
    retrieved_documents: list[DashboardChatRetrievedDocument] = field(default_factory=list)
    retrieved_document_ids: set[str] = field(default_factory=set)
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    warehouse_tools: DashboardChatWarehouseTools | None = None
    last_sql: str | None = None
    last_attempted_sql: str | None = None
    last_attempted_answer_plan: dict[str, Any] | None = None
    last_sql_error: str | None = None
    last_sql_error_reason: str | None = None
    last_sql_results: list[dict[str, Any]] | None = None
    last_sql_validation: DashboardChatSqlValidationResult | None = None
    sql_query_plan: dict[str, Any] | None = None
    semantic_verifier_rejections: int = 0
    last_sql_rejection_reason: str | None = None
    last_sql_rejection_message: str | None = None
    repaired_reason_codes: set[str] = field(default_factory=set)
    distinct_validation_failures: dict[str, int] = field(default_factory=dict)
    forced_sql_continuations: int = 0
    timing_breakdown: dict[str, Any] = field(default_factory=dict)
    pii_value_map: dict[str, str] = field(default_factory=dict)
    pii_tokens_by_value: dict[str, str] = field(default_factory=dict)
    pii_token_counters: dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_state(
        cls,
        state: DashboardChatGraphState,
    ) -> "DashboardChatTurnContext":
        """Build a fresh turn context from checkpointed state plus per-run inputs."""
        return cls(
            validated_distinct_values=hydrate_validated_distinct_values(state),
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
    for validated_entry in turn_context.validated_distinct_values:
        if len(validated_entry) != 3:
            continue
        table_name, column_name, value = validated_entry
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
    requested_tables: list[str] = []
    for table_name in tables if tables is not None else allowlist.prioritized_tables():
        resolved_table_name = allowlist.resolve_allowed_table_name(table_name)
        if resolved_table_name is not None:
            requested_tables.append(resolved_table_name)
    requested_tables = list(dict.fromkeys(requested_tables))
    cache = turn_context.schema_snippets_by_table
    missing_tables = [
        table_name
        for table_name in requested_tables
        if find_matching_dashboard_chat_table_name(table_name, cache) is None
    ]
    if missing_tables:
        snippets = get_turn_warehouse_tools(
            warehouse_tools_factory,
            turn_context,
            state,
        ).get_schema_snippets(missing_tables)
        for table_name, snippet in snippets.items():
            cache[table_name] = snippet
    if tables is None:
        return cache
    requested_cache: dict[str, Any] = {}
    for table_name in requested_tables:
        matched_table_name = find_matching_dashboard_chat_table_name(table_name, cache)
        if matched_table_name is None:
            continue
        requested_cache[table_name] = cache[matched_table_name]
    return requested_cache


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


def metadata_column_is_pii(
    state: DashboardChatGraphState,
    *,
    table_name: str,
    column_name: str,
) -> bool:
    """Return whether effective metadata marks a column as PII."""
    payload = state.get("metadata_artifact_payload") or {}
    normalized_table_name = normalize_dashboard_chat_table_name(table_name)
    normalized_column_name = str(column_name or "").strip().lower()
    if not normalized_table_name or not normalized_column_name:
        return False

    for table in payload.get("tables") or []:
        if not isinstance(table, dict):
            continue
        candidate_table_name = normalize_dashboard_chat_table_name(
            str(table.get("table_name") or "")
        )
        if candidate_table_name != normalized_table_name:
            continue
        for column in table.get("columns") or []:
            if not isinstance(column, dict):
                continue
            candidate_column_name = str(
                column.get("column_name") or column.get("name") or ""
            ).strip().lower()
            if candidate_column_name == normalized_column_name:
                return bool(column.get("pii"))
    return False


def record_validated_distinct_values(
    *,
    turn_context: DashboardChatTurnContext,
    table_name: str,
    column_name: str,
    values: Sequence[Any],
    column_values_exhaustive: bool = False,
) -> None:
    """Persist exact validated filter values into the current turn state."""
    normalized_table = table_name.lower()
    normalized_column = column_name.lower()
    validated_distinct_values = turn_context.validated_distinct_values
    if column_values_exhaustive:
        validated_distinct_values.add((normalized_table, normalized_column))
        validated_distinct_values.add(("*", normalized_column))
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
