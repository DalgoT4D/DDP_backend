"""Semantic SQL verification helpers for dashboard chat."""

from __future__ import annotations

import re
from typing import Any

from ddpui.core.dashboard_chat.contracts.intent_contracts import DashboardChatIntentDecision
from ddpui.core.dashboard_chat.contracts.sql_contracts import DashboardChatSqlVerificationResult
from ddpui.core.dashboard_chat.metadata.schemas import DashboardChatMetadataArtifactPayload
from ddpui.core.dashboard_chat.metadata.search import table_lookup
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_parsing import (
    structural_dimensions_from_sql,
)
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.warehouse.sql_guard import DashboardChatSqlGuard

LATEST_ROW_PATTERN = re.compile(
    r"row_number\s*\(\s*\)\s*over\s*\([^)]+order\s+by\s+[^)]*(?:date|time|timestamp)[^)]*desc",
    re.IGNORECASE | re.DOTALL,
)
MAX_DATE_PATTERN = re.compile(
    r"max\s*\(\s*[^)]*(?:date|time|timestamp)[^)]*\)",
    re.IGNORECASE,
)
NAME_AGGREGATION_PATTERN = re.compile(
    r"\b(?:array_agg|string_agg|listagg|group_concat)\b",
    re.IGNORECASE,
)
LATEST_REQUEST_PATTERN = re.compile(
    r"\b(latest|most recent|recent|as of|as-of|latest available)\b"
    r"|\bcurrent\s+(?:dashboard\s+)?snapshot\b",
    re.IGNORECASE,
)
RELATIVE_TIME_REQUEST_PATTERN = re.compile(
    r"\b(?:this|current)\s+(?:year|quarter|month)\b"
    r"|\b(?:fiscal|financial)\s+year\b"
    r"|\bfy\s*\d{2,4}\b"
    r"|\bq[1-4]\b"
    r"|\b\d{4}\s*-\s*\d{2}\b",
    re.IGNORECASE,
)
DIRECT_TIME_FILTER_PATTERN = re.compile(
    r"(?:date|time|timestamp)[\w.\"`]*\s*(?:>=|>|<=|<|=|between)\s*(?:date\s*)?['\"]?\d{4}-\d{2}-\d{2}"
    r"|extract\s*\(\s*(?:year|quarter|month)\s+from\s+[^)]+\)\s*=\s*\d{1,4}",
    re.IGNORECASE,
)
NAME_LIST_PATTERN = re.compile(
    r"\b(give me the names?|show me the names?|list(?:\s+the)?\s+names?|names?\s+of|who are)\b",
    re.IGNORECASE,
)


def verify_sql_against_question(
    llm_client,
    *,
    sql: str,
    state: DashboardChatGraphState,
) -> DashboardChatSqlVerificationResult:
    """Use an LLM verifier to judge whether SQL answers the user's question faithfully."""
    if not hasattr(llm_client, "verify_sql_against_question"):
        return DashboardChatSqlVerificationResult(is_valid=True)

    artifact_payload = state.get("metadata_artifact_payload") or {}
    try:
        artifact = DashboardChatMetadataArtifactPayload.model_validate(artifact_payload)
        referenced_tables = _referenced_table_metadata(sql, artifact)
    except Exception:
        artifact = None
        referenced_tables = []
    risk_flags = build_sql_risk_flags(
        sql=sql, user_query=state["user_query"], tables=referenced_tables
    )
    deterministic_result = _deterministic_temporal_verification(
        sql=sql,
        user_query=state["user_query"],
        risk_flags=risk_flags,
    )
    if deterministic_result is not None:
        return deterministic_result

    intent = DashboardChatIntentDecision.model_validate(state.get("intent_decision") or {}).intent
    return llm_client.verify_sql_against_question(
        user_query=state["user_query"],
        intent=intent,
        sql=sql,
        risk_flags=risk_flags,
        referenced_tables=referenced_tables,
        structural_dimensions=sorted(structural_dimensions_from_sql(sql)),
    )


def build_sql_risk_flags(
    *,
    sql: str,
    user_query: str,
    tables: list[dict[str, Any]],
) -> list[str]:
    """Return deterministic risk signals for the verifier to reason over."""
    flags: list[str] = []
    if not LATEST_REQUEST_PATTERN.search(user_query):
        if MAX_DATE_PATTERN.search(sql):
            flags.append("uses_max_date_without_explicit_latest_request")
        if LATEST_ROW_PATTERN.search(sql):
            flags.append("uses_latest_row_logic_without_explicit_latest_request")
    if NAME_LIST_PATTERN.search(user_query) and NAME_AGGREGATION_PATTERN.search(sql):
        flags.append("aggregates_names_instead_of_returning_one_row_per_name")
    if tables and all((table.get("table_type") or "") == "aggregate" for table in tables if table):
        flags.append("uses_only_aggregate_tables")
    if any(table.get("table_type") == "aggregate" for table in tables):
        flags.append("includes_aggregate_table")
    return flags


def _deterministic_temporal_verification(
    *,
    sql: str,
    user_query: str,
    risk_flags: list[str],
) -> DashboardChatSqlVerificationResult | None:
    """Hard-block temporal semantics that must not be left to LLM judgment."""
    uses_latest_logic = (
        "uses_max_date_without_explicit_latest_request" in risk_flags
        or "uses_latest_row_logic_without_explicit_latest_request" in risk_flags
    )
    if uses_latest_logic:
        return DashboardChatSqlVerificationResult(
            is_valid=False,
            severity="hard_block",
            reason_code="latest_logic_without_user_request",
            reasoning=(
                "SQL uses latest-row, latest-date, MAX(date), or ROW_NUMBER-over-date "
                "logic, but the user did not explicitly ask for latest, most recent, "
                "as-of, or current-snapshot semantics."
            ),
            issues=[
                "Unrequested latest/as-of logic changes the user's requested time basis.",
            ],
            repair_instructions=[
                "Remove latest-row/latest-date logic.",
                "Use direct filters and aggregate over the requested rows or period.",
                "Only use latest/as-of logic when the user explicitly asks for it.",
            ],
            risk_flags=risk_flags,
        )

    if (
        RELATIVE_TIME_REQUEST_PATTERN.search(user_query)
        and LATEST_REQUEST_PATTERN.search(user_query)
        and (MAX_DATE_PATTERN.search(sql) or LATEST_ROW_PATTERN.search(sql))
        and not DIRECT_TIME_FILTER_PATTERN.search(sql)
    ):
        return DashboardChatSqlVerificationResult(
            is_valid=False,
            severity="hard_block",
            reason_code="latest_logic_without_relative_time_filter",
            reasoning=(
                "The user asked for a relative period and latest/as-of semantics, but "
                "the SQL uses latest-row/date logic without applying a concrete date "
                "window for that period."
            ),
            issues=[
                "Relative time questions require a resolved date window before latest/as-of logic can be used.",
            ],
            repair_instructions=[
                "Resolve the relative period to concrete start and end dates.",
                "Add direct date filters for that period.",
                "Then apply latest/as-of logic only within the filtered period if it is still explicitly requested.",
            ],
            risk_flags=risk_flags,
        )

    return None


def _referenced_table_metadata(
    sql: str,
    artifact: DashboardChatMetadataArtifactPayload,
) -> list[dict[str, Any]]:
    """Return compact metadata for tables referenced by the SQL."""
    referenced_table_names = list(dict.fromkeys(_extract_referenced_tables(sql)))
    tables_by_name = table_lookup(artifact)
    tables_by_normalized_name = {
        table_name.lower(): table for table_name, table in tables_by_name.items()
    }
    results: list[dict[str, Any]] = []
    for table_name in referenced_table_names:
        table = tables_by_name.get(table_name) or tables_by_normalized_name.get(table_name.lower())
        if table is None:
            continue
        results.append(
            {
                "table_name": table.table_name,
                "table_type": table.table_type,
                "row_grain": table.row_grain,
                "description": table.description,
                "primary_entities": table.primary_entities,
                "primary_filter_time_column": table.temporal.primary_filter_time_column,
                "time_column_meanings": table.temporal.time_column_meanings,
                "entity_counting_guidance": table.counting.entity_counting_guidance,
                "answerability": table.answerability.model_dump(mode="json"),
                "columns": [
                    {
                        "name": column.name,
                        "description": column.description,
                        "semantic_role": column.semantic_role,
                        "value_semantics": column.value_semantics,
                        "pii": column.pii,
                    }
                    for column in table.columns[:80]
                ],
            }
        )
    return results


def _extract_referenced_tables(sql: str) -> list[str]:
    """Extract referenced physical tables from FROM/JOIN clauses."""
    return DashboardChatSqlGuard._extract_table_names(sql)
