"""Structured metadata exploration tools for dashboard chat."""

from __future__ import annotations

import re
from datetime import date
from typing import Any

from django.utils import timezone

from ddpui.core.dashboard_chat.metadata.schemas import (
    DashboardChatChartRegistryEntry,
    DashboardChatMetadataArtifactPayload,
)
from ddpui.core.dashboard_chat.metadata.search import (
    get_related_tables,
    join_paths_for_tables,
    search_columns_by_name,
    search_metadata_tables,
    table_lookup,
)
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState

QUARTER_PATTERN = re.compile(r"\bq([1-4])(?:\s*[-–]\s*q([1-4]))?\b", re.IGNORECASE)
FISCAL_YEAR_PATTERN = re.compile(
    r"\b(?:fy\s*)?(20\d{2}|\d{2})\s*[-/]\s*(20\d{2}|\d{2})\b",
    re.IGNORECASE,
)


def _load_metadata_artifact(
    state: DashboardChatGraphState,
) -> DashboardChatMetadataArtifactPayload | None:
    payload = state.get("metadata_artifact_payload")
    if not payload:
        return None
    return DashboardChatMetadataArtifactPayload.model_validate(payload)


def _load_chart_registry(
    state: DashboardChatGraphState,
) -> list[DashboardChatChartRegistryEntry]:
    return [
        DashboardChatChartRegistryEntry.model_validate(entry)
        for entry in (state.get("chart_registry_payload") or [])
    ]


def _metadata_unavailable_result(state: DashboardChatGraphState) -> dict[str, Any]:
    return {
        "error": (
            "Dashboard metadata artifact is not available. "
            f"Current status: {state.get('metadata_artifact_status') or 'missing'}."
        )
    }


def handle_get_chart_table_metadata_tool(
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context,
) -> dict[str, Any]:
    """Return chart entries plus metadata for the chart tables they use."""
    artifact = _load_metadata_artifact(state)
    if artifact is None:
        return _metadata_unavailable_result(state)

    chart_ids = {int(value) for value in (args.get("chart_ids") or []) if value is not None}
    chart_titles = {str(value).strip().lower() for value in (args.get("chart_titles") or []) if value}
    table_names = {str(value).strip() for value in (args.get("table_names") or []) if value}

    registry = _load_chart_registry(state)
    tables_by_name = table_lookup(artifact)
    matched_charts: list[DashboardChatChartRegistryEntry] = []
    for chart in registry:
        if chart_ids and chart.chart_id in chart_ids:
            matched_charts.append(chart)
            continue
        if chart_titles and chart.title.strip().lower() in chart_titles:
            matched_charts.append(chart)
            continue
        if table_names and chart.preferred_table in table_names:
            matched_charts.append(chart)
            continue
    if not chart_ids and not chart_titles and not table_names:
        matched_charts = registry

    deduped_tables: list[str] = []
    for chart in matched_charts:
        if chart.preferred_table and chart.preferred_table not in deduped_tables:
            deduped_tables.append(chart.preferred_table)

    return {
        "count": len(matched_charts),
        "charts": [chart.model_dump(mode="json") for chart in matched_charts],
        "tables": [
            tables_by_name[table_name].model_dump(mode="json")
            for table_name in deduped_tables
            if table_name in tables_by_name
        ],
    }


def handle_search_metadata_tool(
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context,
) -> dict[str, Any]:
    """Search the enriched metadata artifact using structured query-brief fields."""
    artifact = _load_metadata_artifact(state)
    if artifact is None:
        return _metadata_unavailable_result(state)

    matches = search_metadata_tables(
        artifact,
        question_terms=args.get("question_terms") or [],
        entity_terms=args.get("entity_terms") or [],
        measure_terms=args.get("measure_terms") or [],
        grain_terms=args.get("grain_terms") or [],
        time_terms=args.get("time_terms") or [],
        question_type=args.get("question_type"),
        required_output_shape=args.get("required_output_shape"),
        limit=int(args.get("limit") or 8),
    )
    return {"count": len(matches), "tables": matches}


def handle_get_table_metadata_tool(
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context,
) -> dict[str, Any]:
    """Return full metadata entries for the requested tables."""
    artifact = _load_metadata_artifact(state)
    if artifact is None:
        return _metadata_unavailable_result(state)
    requested_tables = [str(value) for value in (args.get("tables") or []) if value]
    by_name = table_lookup(artifact)
    tables = [by_name[table_name].model_dump(mode="json") for table_name in requested_tables if table_name in by_name]
    return {"count": len(tables), "tables": tables}


def handle_get_column_metadata_tool(
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context,
) -> dict[str, Any]:
    """Return matching columns from the requested tables."""
    artifact = _load_metadata_artifact(state)
    if artifact is None:
        return _metadata_unavailable_result(state)
    requested_tables = {str(value) for value in (args.get("tables") or []) if value}
    column_terms = [str(value).strip().lower() for value in (args.get("column_terms") or []) if value]
    matches: list[dict[str, Any]] = []
    for table in artifact.tables:
        if requested_tables and table.table_name not in requested_tables:
            continue
        for column in table.columns:
            haystack = " ".join(
                [
                    column.name.lower(),
                    column.description.lower(),
                    " ".join(column.entity_tags).lower(),
                    " ".join(column.measure_tags).lower(),
                    column.semantic_role.lower(),
                ]
            )
            if column_terms and not all(term in haystack for term in column_terms):
                continue
            matches.append(
                {
                    "table_name": table.table_name,
                    "column": column.model_dump(mode="json"),
                    "table_row_grain": table.row_grain,
                    "table_type": table.table_type,
                }
            )
    return {"count": len(matches), "columns": matches}


def handle_search_columns_by_name_tool(
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context,
) -> dict[str, Any]:
    """Find all allowlisted tables containing a column-name match."""
    artifact = _load_metadata_artifact(state)
    if artifact is None:
        return _metadata_unavailable_result(state)
    column_name = str(args.get("column_name") or "").strip()
    matches = search_columns_by_name(
        artifact,
        column_name=column_name,
        limit=int(args.get("limit") or 20),
    )
    return {"count": len(matches), "columns": matches}


def handle_get_join_paths_tool(
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context,
) -> dict[str, Any]:
    """Return join paths touching the requested tables."""
    artifact = _load_metadata_artifact(state)
    if artifact is None:
        return _metadata_unavailable_result(state)
    table_names = [str(value) for value in (args.get("tables") or []) if value]
    joins = join_paths_for_tables(artifact, table_names=table_names)
    return {
        "count": len(joins),
        "joins": [join_path.model_dump(mode="json") for join_path in joins],
        "missing_fields": list(args.get("missing_fields") or []),
    }


def handle_get_table_statistics_tool(
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context,
) -> dict[str, Any]:
    """Return precomputed statistics for the requested tables."""
    artifact = _load_metadata_artifact(state)
    if artifact is None:
        return _metadata_unavailable_result(state)
    requested_tables = [str(value) for value in (args.get("tables") or []) if value]
    by_name = table_lookup(artifact)
    stats = []
    for table_name in requested_tables:
        table = by_name.get(table_name)
        if table is None:
            continue
        stats.append(
            {
                "table_name": table.table_name,
                "row_grain": table.row_grain,
                "table_type": table.table_type,
                "total_row_count": table.total_row_count,
                "time_coverage": table.time_coverage,
                "statistics": table.statistics,
            }
        )
    return {"count": len(stats), "tables": stats}


def handle_get_related_tables_tool(
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context,
) -> dict[str, Any]:
    """Return related tables connected through the metadata join graph."""
    artifact = _load_metadata_artifact(state)
    if artifact is None:
        return _metadata_unavailable_result(state)
    related = get_related_tables(
        artifact,
        table_names=[str(value) for value in (args.get("tables") or []) if value],
        entity_terms=args.get("entity_terms") or [],
        measure_terms=args.get("measure_terms") or [],
    )
    return {"count": len(related), "tables": related}


def handle_read_full_metadata_tool(
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context,
) -> dict[str, Any]:
    """Return the full enriched dashboard metadata artifact. Extreme last resort only."""
    artifact = _load_metadata_artifact(state)
    if artifact is None:
        return _metadata_unavailable_result(state)
    return artifact.model_dump(mode="json")


def handle_resolve_time_scope_tool(
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context,
) -> dict[str, Any]:
    """Resolve relative or quarter-based time language into explicit ranges."""
    question_text = str(args.get("question_text") or state.get("user_query") or "").strip()
    current_date = timezone.localdate()
    resolved_ranges = _resolve_time_scope(question_text, current_date)
    return {
        "question_text": question_text,
        "current_date": current_date.isoformat(),
        "resolved_ranges": resolved_ranges,
    }


def _resolve_time_scope(question_text: str, current_date: date) -> list[dict[str, Any]]:
    """Resolve common relative and quarter-based time scopes."""
    lowered = question_text.lower()
    ranges: list[dict[str, Any]] = []

    fiscal_year_match = FISCAL_YEAR_PATTERN.search(lowered)
    quarter_match = QUARTER_PATTERN.search(lowered)
    if quarter_match:
        start_quarter = int(quarter_match.group(1))
        end_quarter = int(quarter_match.group(2) or quarter_match.group(1))
        fiscal_year_start = _infer_fiscal_year_start(fiscal_year_match, current_date)
        for quarter_number in range(start_quarter, end_quarter + 1):
            start_date, end_date = _fiscal_quarter_dates(fiscal_year_start, quarter_number)
            ranges.append(
                {
                    "label": f"Q{quarter_number}",
                    "start_date": start_date.isoformat(),
                    "end_date_exclusive": end_date.isoformat(),
                    "calendar_basis": f"fiscal year starting {fiscal_year_start}-04-01",
                }
            )
        return ranges

    if "this year" in lowered or "current year" in lowered:
        start_date = date(current_date.year, 1, 1)
        end_date = date(current_date.year + 1, 1, 1)
        ranges.append(
            {
                "label": "current_calendar_year",
                "start_date": start_date.isoformat(),
                "end_date_exclusive": end_date.isoformat(),
            }
        )
    if "this month" in lowered or "current month" in lowered:
        start_date = date(current_date.year, current_date.month, 1)
        if current_date.month == 12:
            end_date = date(current_date.year + 1, 1, 1)
        else:
            end_date = date(current_date.year, current_date.month + 1, 1)
        ranges.append(
            {
                "label": "current_calendar_month",
                "start_date": start_date.isoformat(),
                "end_date_exclusive": end_date.isoformat(),
            }
        )
    if "this quarter" in lowered or "current quarter" in lowered:
        quarter_start_month = ((current_date.month - 1) // 3) * 3 + 1
        start_date = date(current_date.year, quarter_start_month, 1)
        if quarter_start_month == 10:
            end_date = date(current_date.year + 1, 1, 1)
        else:
            end_date = date(current_date.year, quarter_start_month + 3, 1)
        ranges.append(
            {
                "label": "current_calendar_quarter",
                "start_date": start_date.isoformat(),
                "end_date_exclusive": end_date.isoformat(),
            }
        )
    return ranges


def _infer_fiscal_year_start(
    fiscal_year_match: re.Match[str] | None,
    current_date: date,
) -> int:
    """Infer the April-start fiscal year base year."""
    if fiscal_year_match is None:
        return current_date.year if current_date.month >= 4 else current_date.year - 1
    start_token = fiscal_year_match.group(1)
    start_year = int(start_token)
    if start_year < 100:
        start_year += 2000
    return start_year


def _fiscal_quarter_dates(fiscal_year_start: int, quarter_number: int) -> tuple[date, date]:
    """Return inclusive start and exclusive end dates for an April-based fiscal quarter."""
    month_lookup = {
        1: (4, 7),
        2: (7, 10),
        3: (10, 13),
        4: (1, 4),
    }
    start_month, end_month = month_lookup[quarter_number]
    if quarter_number == 4:
        start_date = date(fiscal_year_start + 1, start_month, 1)
        end_date = date(fiscal_year_start + 1, end_month, 1)
    else:
        start_date = date(fiscal_year_start, start_month, 1)
        if end_month == 13:
            end_date = date(fiscal_year_start + 1, 1, 1)
        else:
            end_date = date(fiscal_year_start, end_month, 1)
    return start_date, end_date
