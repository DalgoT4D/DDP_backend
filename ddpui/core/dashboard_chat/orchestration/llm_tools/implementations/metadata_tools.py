"""Structured metadata exploration tools for dashboard chat."""

from __future__ import annotations

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
from ddpui.core.dashboard_chat.orchestration.pii_masking import mask_metadata_artifact_for_llm
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.time_scope import (
    resolve_time_scope,
)


def _load_metadata_artifact(
    state: DashboardChatGraphState,
) -> DashboardChatMetadataArtifactPayload | None:
    payload = state.get("metadata_artifact_payload")
    if not payload:
        return None
    return DashboardChatMetadataArtifactPayload.model_validate(payload)


def _load_metadata_artifact_for_tool(
    state: DashboardChatGraphState,
    turn_context,
) -> DashboardChatMetadataArtifactPayload | None:
    artifact = _load_metadata_artifact(state)
    if artifact is None:
        return None
    return mask_metadata_artifact_for_llm(
        state=state,
        turn_context=turn_context,
        artifact=artifact,
    )


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
    artifact = _load_metadata_artifact_for_tool(state, turn_context)
    if artifact is None:
        return _metadata_unavailable_result(state)

    chart_ids = {int(value) for value in (args.get("chart_ids") or []) if value is not None}
    chart_titles = {
        str(value).strip().lower() for value in (args.get("chart_titles") or []) if value
    }
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
    artifact = _load_metadata_artifact_for_tool(state, turn_context)
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
    artifact = _load_metadata_artifact_for_tool(state, turn_context)
    if artifact is None:
        return _metadata_unavailable_result(state)
    requested_tables = [str(value) for value in (args.get("tables") or []) if value]
    by_name = table_lookup(artifact)
    tables = [
        by_name[table_name].model_dump(mode="json")
        for table_name in requested_tables
        if table_name in by_name
    ]
    return {"count": len(tables), "tables": tables}


def handle_get_column_metadata_tool(
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context,
) -> dict[str, Any]:
    """Return matching columns from the requested tables."""
    artifact = _load_metadata_artifact_for_tool(state, turn_context)
    if artifact is None:
        return _metadata_unavailable_result(state)
    requested_tables = {str(value) for value in (args.get("tables") or []) if value}
    column_terms = [
        str(value).strip().lower() for value in (args.get("column_terms") or []) if value
    ]
    matches: list[dict[str, Any]] = []
    for table in artifact.tables:
        if requested_tables and table.table_name not in requested_tables:
            continue
        for column in table.columns:
            haystack = " ".join(
                [
                    column.name.lower(),
                    column.description.lower(),
                    column.semantic_role.lower(),
                    column.value_semantics.lower(),
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
    artifact = _load_metadata_artifact_for_tool(state, turn_context)
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
    artifact = _load_metadata_artifact_for_tool(state, turn_context)
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
    artifact = _load_metadata_artifact_for_tool(state, turn_context)
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
                "temporal": table.temporal.model_dump(mode="json"),
                "statistics": table.statistics.model_dump(mode="json"),
            }
        )
    return {"count": len(stats), "tables": stats}


def handle_get_related_tables_tool(
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context,
) -> dict[str, Any]:
    """Return related tables connected through the metadata join graph."""
    artifact = _load_metadata_artifact_for_tool(state, turn_context)
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
    artifact = _load_metadata_artifact_for_tool(state, turn_context)
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
    resolved_ranges = resolve_time_scope(question_text, current_date)
    return {
        "question_text": question_text,
        "current_date": current_date.isoformat(),
        "resolved_ranges": resolved_ranges,
    }
