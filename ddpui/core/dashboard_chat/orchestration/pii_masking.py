"""Runtime PII masking for dashboard chat tool outputs."""

from __future__ import annotations

import re
from typing import Any

from ddpui.core.dashboard_chat.metadata.pii_overrides import (
    apply_pii_overrides_to_payload,
    load_pii_overrides_for_org,
)
from ddpui.core.dashboard_chat.metadata.schemas import DashboardChatMetadataArtifactPayload
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_parsing import (
    extract_identifier_refs_from_sql_segment,
    resolve_table_qualifier,
    table_references,
)
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.warehouse.sql_guard import DashboardChatSqlGuard
from ddpui.models.org_preferences import OrgPreferences

PII_TOKEN_PATTERN = re.compile(r"\[\[PII_[A-Z0-9_]+\]\]")
NUMERIC_AGGREGATE_PATTERN = re.compile(r"\b(?:COUNT|SUM|AVG)\s*\(", re.IGNORECASE)


def share_pii_with_llms_enabled(state: DashboardChatGraphState) -> bool:
    """Return whether raw PII values may be sent to LLMs for the current org."""
    org_id = int(state["org_id"])
    setting = (
        OrgPreferences.objects.filter(org_id=org_id)
        .values_list("dashboard_chat_share_pii_with_llms", flat=True)
        .first()
    )
    return True if setting is None else bool(setting)


def load_effective_metadata_payload(
    state: DashboardChatGraphState,
) -> DashboardChatMetadataArtifactPayload | None:
    """Load metadata payload with persisted PII overrides applied."""
    raw_payload = state.get("metadata_artifact_payload")
    if not raw_payload:
        return None
    try:
        payload = DashboardChatMetadataArtifactPayload.model_validate(raw_payload)
    except Exception:
        return None
    overrides = load_pii_overrides_for_org(int(state["org_id"]))
    return apply_pii_overrides_to_payload(payload, overrides)


def is_metadata_pii_column(
    *,
    state: DashboardChatGraphState,
    table_name: str,
    column_name: str,
) -> bool:
    """Return whether the effective metadata marks a specific column as PII."""
    payload = load_effective_metadata_payload(state)
    if payload is None:
        return False
    normalized_table_names = _candidate_table_names(table_name)
    normalized_column_name = column_name.lower()
    for table in payload.tables:
        if table.table_name.lower() not in normalized_table_names:
            continue
        for column in table.columns:
            if column.column_name.lower() == normalized_column_name:
                return bool(column.pii)
    return False


def mask_distinct_values_for_llm(
    *,
    state: DashboardChatGraphState,
    turn_context,
    table_name: str,
    column_name: str,
    values: list[Any],
) -> list[Any]:
    """Mask distinct values for one PII column before returning them to the tool loop."""
    if share_pii_with_llms_enabled(state):
        return values
    if not is_metadata_pii_column(
        state=state,
        table_name=table_name,
        column_name=column_name,
    ):
        return values
    return [
        mask_pii_value(
            turn_context=turn_context,
            source_column=column_name,
            value=value,
        )
        for value in values
    ]


def mask_sql_rows_for_llm(
    *,
    state: DashboardChatGraphState,
    turn_context,
    sql: str,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Mask PII result cells before SQL rows are sent to an LLM."""
    if share_pii_with_llms_enabled(state) or not rows:
        return rows
    pii_result_columns = result_columns_that_contain_pii(state=state, sql=sql, rows=rows)
    if not pii_result_columns:
        return rows
    masked_rows: list[dict[str, Any]] = []
    for row in rows:
        masked_row = dict(row)
        for column_name in pii_result_columns:
            if column_name not in masked_row:
                continue
            masked_row[column_name] = mask_pii_value(
                turn_context=turn_context,
                source_column=column_name,
                value=masked_row[column_name],
            )
        masked_rows.append(masked_row)
    return masked_rows


def mask_metadata_artifact_for_llm(
    *,
    state: DashboardChatGraphState,
    turn_context,
    artifact: DashboardChatMetadataArtifactPayload,
) -> DashboardChatMetadataArtifactPayload:
    """Mask PII sample values inside metadata tool payloads before LLM exposure."""
    if share_pii_with_llms_enabled(state):
        return artifact
    artifact_copy = artifact.model_copy(deep=True)
    for table in artifact_copy.tables:
        for column in table.columns:
            if not column.pii or not column.statistics.sample_values:
                continue
            column.statistics.sample_values = [
                mask_pii_value(
                    turn_context=turn_context,
                    source_column=column.column_name,
                    value=value,
                )
                for value in column.statistics.sample_values
            ]
    return artifact_copy


def result_columns_that_contain_pii(
    *,
    state: DashboardChatGraphState,
    sql: str,
    rows: list[dict[str, Any]],
) -> set[str]:
    """Infer which output columns correspond to effective metadata PII columns."""
    payload = load_effective_metadata_payload(state)
    if payload is None or not rows:
        return set()
    result_columns = set(rows[0].keys())
    referenced_tables = table_references(sql)
    referenced_table_names = [
        str(reference["table_name"])
        for reference in referenced_tables
        if reference.get("table_name")
    ]
    pii_columns_by_table = _pii_columns_by_table(payload)
    pii_columns_by_name = {
        column_name
        for table_name, column_names in pii_columns_by_table.items()
        if not referenced_table_names
        or table_name in _expanded_referenced_table_names(referenced_table_names)
        for column_name in column_names
    }

    pii_result_columns = {
        column_name
        for column_name in result_columns
        if _normalized_column_alias(column_name) in pii_columns_by_name
    }

    select_clause = DashboardChatSqlGuard._extract_outer_select_clause(sql)
    if not select_clause:
        return pii_result_columns

    for expression in DashboardChatSqlGuard._split_select_expressions(select_clause):
        output_name = _select_output_name(expression)
        if output_name not in result_columns:
            continue
        if _expression_projects_pii_value(
            expression=expression,
            referenced_tables=referenced_tables,
            pii_columns_by_table=pii_columns_by_table,
        ):
            pii_result_columns.add(output_name)
    return pii_result_columns


def mask_pii_value(*, turn_context, source_column: str, value: Any) -> Any:
    """Replace one raw PII value with a stable per-turn placeholder."""
    if value is None:
        return None
    raw_value = str(value)
    if not raw_value:
        return raw_value
    cache_key = f"{source_column}\u241f{raw_value}"
    existing_token = turn_context.pii_tokens_by_value.get(cache_key)
    if existing_token is not None:
        return existing_token

    prefix = _pii_token_prefix(source_column)
    counter = turn_context.pii_token_counters.get(prefix, 0) + 1
    turn_context.pii_token_counters[prefix] = counter
    token = f"[[PII_{prefix}_{counter}]]"
    turn_context.pii_tokens_by_value[cache_key] = token
    turn_context.pii_value_map[token] = raw_value
    return token


def unmask_pii_text(text: str, pii_value_map: dict[str, str] | None) -> str:
    """Replace PII placeholders in final user-facing text after LLM composition."""
    if not text or not pii_value_map:
        return text
    unmasked_text = text
    for token, raw_value in pii_value_map.items():
        unmasked_text = unmasked_text.replace(token, raw_value)
    return unmasked_text


def mask_text_with_pii_map(text: str, pii_value_map: dict[str, str] | None) -> str:
    """Replace raw PII values with placeholders before storing LLM-reusable history."""
    if not text or not pii_value_map:
        return text
    masked_text = text
    replacements = sorted(
        ((raw_value, token) for token, raw_value in pii_value_map.items() if raw_value),
        key=lambda item: len(item[0]),
        reverse=True,
    )
    for raw_value, token in replacements:
        masked_text = masked_text.replace(raw_value, token)
    return masked_text


def unmask_pii_rows(
    rows: list[dict[str, Any]] | None,
    pii_value_map: dict[str, str] | None,
) -> list[dict[str, Any]] | None:
    """Replace placeholders in display rows after the LLM boundary."""
    if rows is None or not pii_value_map:
        return rows
    return [
        {column_name: _unmask_cell(value, pii_value_map) for column_name, value in row.items()}
        for row in rows
    ]


def _unmask_cell(value: Any, pii_value_map: dict[str, str]) -> Any:
    if isinstance(value, str):
        return unmask_pii_text(value, pii_value_map)
    return value


def _pii_columns_by_table(payload: DashboardChatMetadataArtifactPayload) -> dict[str, set[str]]:
    pii_columns_by_table: dict[str, set[str]] = {}
    for table in payload.tables:
        table_names = _candidate_table_names(table.table_name)
        pii_columns = {
            _normalized_column_alias(column.column_name) for column in table.columns if column.pii
        }
        if not pii_columns:
            continue
        for table_name in table_names:
            pii_columns_by_table.setdefault(table_name, set()).update(pii_columns)
    return pii_columns_by_table


def _expanded_referenced_table_names(table_names: list[str]) -> set[str]:
    expanded: set[str] = set()
    for table_name in table_names:
        expanded.update(_candidate_table_names(table_name))
    return expanded


def _candidate_table_names(table_name: str) -> set[str]:
    normalized_table = str(table_name or "").strip().strip('"`').lower()
    table_names = {normalized_table} if normalized_table else set()
    if "." in normalized_table:
        table_names.add(normalized_table.split(".")[-1])
    return table_names


def _select_output_name(expression: str) -> str:
    alias_match = re.search(
        r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)\s*$",
        expression,
        flags=re.IGNORECASE,
    )
    if alias_match:
        return alias_match.group(1)
    bare_identifier_match = re.search(
        r"(?:[A-Za-z_][A-Za-z0-9_]*\.)?([A-Za-z_][A-Za-z0-9_]*)\s*$",
        expression.strip(),
    )
    return bare_identifier_match.group(1) if bare_identifier_match else ""


def _expression_projects_pii_value(
    *,
    expression: str,
    referenced_tables: list[dict[str, str | None]],
    pii_columns_by_table: dict[str, set[str]],
) -> bool:
    if NUMERIC_AGGREGATE_PATTERN.search(expression):
        return False
    identifier_refs = extract_identifier_refs_from_sql_segment(
        expression,
        table_aliases={str(reference.get("alias") or "") for reference in referenced_tables},
    )
    for qualifier, column_name in identifier_refs:
        normalized_column = _normalized_column_alias(column_name)
        if qualifier:
            table_name = resolve_table_qualifier(qualifier, referenced_tables)
            if not table_name:
                continue
            if normalized_column in pii_columns_by_table.get(table_name.lower(), set()):
                return True
            if normalized_column in pii_columns_by_table.get(
                table_name.split(".")[-1].lower(), set()
            ):
                return True
            continue
        matching_tables = [
            table_name
            for table_name, pii_columns in pii_columns_by_table.items()
            if normalized_column in pii_columns
        ]
        if matching_tables:
            return True
    return False


def _normalized_column_alias(column_name: str) -> str:
    normalized = str(column_name or "").strip().strip('"`').lower()
    normalized = re.sub(r"[^a-z0-9_]+", "_", normalized)
    return normalized.strip("_")


def _pii_token_prefix(column_name: str) -> str:
    prefix = _normalized_column_alias(column_name).upper()
    prefix = re.sub(r"_+", "_", prefix).strip("_")
    return prefix or "VALUE"
