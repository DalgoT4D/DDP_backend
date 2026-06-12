"""Retrieval and citation helpers for dashboard chat graph execution."""

from collections.abc import Sequence
from typing import Any
from urllib.parse import urlencode

from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import DashboardChatAllowlist, build_dashboard_chat_table_name
from ddpui.core.dashboard_chat.contracts.response_contracts import DashboardChatCitation
from ddpui.core.dashboard_chat.contracts.retrieval_contracts import DashboardChatRetrievedDocument
from ddpui.core.dashboard_chat.vector.vector_documents import DashboardChatSourceType

from ddpui.core.dashboard_chat.orchestration.source_identifier_parsing import (
    chart_id_from_source_identifier,
    unique_id_from_source_identifier,
)


def retrieve_vector_documents(
    vector_store,
    runtime_config,
    *,
    org,
    collection_name: str | None,
    query_text: str,
    source_types,
    dashboard_id: int | None = None,
    query_embedding: list[float] | None = None,
) -> list[DashboardChatRetrievedDocument]:
    """Query chroma and normalize the results."""
    if not source_types:
        return []

    results = vector_store.query(
        org.id,
        query_text=query_text,
        n_results=runtime_config.retrieval_limit,
        source_types=source_types,
        dashboard_id=dashboard_id,
        query_embedding=query_embedding,
        collection_name=collection_name,
    )
    return [
        DashboardChatRetrievedDocument(
            document_id=result.document_id,
            source_type=str(result.metadata.get("source_type") or ""),
            source_identifier=str(result.metadata.get("source_identifier") or ""),
            content=result.content,
            dashboard_id=result.metadata.get("dashboard_id"),
            distance=result.distance,
        )
        for result in results
    ]


def filter_allowlisted_dbt_results(
    results: Sequence[DashboardChatRetrievedDocument],
    allowlist: DashboardChatAllowlist,
) -> list[DashboardChatRetrievedDocument]:
    """Keep only dbt docs that belong to the dashboard lineage."""
    filtered_results: list[DashboardChatRetrievedDocument] = []
    for result in results:
        unique_id = unique_id_from_source_identifier(result.source_identifier)
        if allowlist.is_unique_id_allowed(unique_id):
            filtered_results.append(result)
    return filtered_results


def dedupe_retrieved_documents(
    results: Sequence[DashboardChatRetrievedDocument],
) -> list[DashboardChatRetrievedDocument]:
    """Deduplicate retrieved documents while preserving better-ranked items."""
    scored_results = [
        (result.distance if result.distance is not None else 999.0, result) for result in results
    ]
    merged_results: list[DashboardChatRetrievedDocument] = []
    seen_document_ids: set[str] = set()
    for _, result in sorted(scored_results, key=lambda item: item[0]):
        if result.document_id in seen_document_ids:
            continue
        merged_results.append(result)
        seen_document_ids.add(result.document_id)
    return merged_results


def build_citations(
    *,
    retrieved_documents: Sequence[DashboardChatRetrievedDocument],
    dashboard_export: dict[str, Any],
    allowlist: DashboardChatAllowlist,
) -> list[DashboardChatCitation]:
    """Build citations from the retrieved tool-loop documents."""
    dashboard_title = dashboard_export["dashboard"].get("title") or "Current dashboard"
    chart_lookup = {
        chart.get("id"): chart.get("title") or f"Chart {chart.get('id')}"
        for chart in dashboard_export.get("charts") or []
    }
    dashboard_id = dashboard_export.get("dashboard", {}).get("id")
    citations: list[DashboardChatCitation] = []
    seen_sources: set[tuple[str, str]] = set()
    for document in retrieved_documents[:6]:
        if document.source_type in {
            DashboardChatSourceType.DBT_MANIFEST.value,
            DashboardChatSourceType.DBT_CATALOG.value,
        }:
            continue

        if (
            document.source_type == DashboardChatSourceType.DASHBOARD_EXPORT.value
            and chart_id_from_source_identifier(document.source_identifier) is None
        ):
            continue

        table_name = None
        source_type = document.source_type
        source_identifier = document.source_identifier
        title = citation_title(
            document=document,
            dashboard_title=dashboard_title,
            chart_lookup=chart_lookup,
            table_name=table_name,
        )
        snippet = citation_snippet(
            document=document,
            dashboard_title=dashboard_title,
            chart_lookup=chart_lookup,
        )
        url = citation_url(
            document=document,
            dashboard_id=document.dashboard_id or dashboard_id,
            table_name=table_name,
        )

        source_key = (source_type, source_identifier)
        if source_key in seen_sources:
            continue

        citations.append(
            DashboardChatCitation(
                source_type=source_type,
                source_identifier=source_identifier,
                title=title,
                snippet=snippet,
                url=url,
                dashboard_id=document.dashboard_id,
                table_name=table_name,
            )
        )
        seen_sources.add(source_key)
    return citations


def citation_title(
    *,
    document: DashboardChatRetrievedDocument,
    dashboard_title: str,
    chart_lookup: dict[int, str],
    table_name: str | None,
) -> str:
    """Map a retrieved document into a human-readable citation title."""
    if document.source_type == DashboardChatSourceType.ORG_CONTEXT.value:
        return "Organization context file"
    if document.source_type == DashboardChatSourceType.DASHBOARD_CONTEXT.value:
        return f"Dashboard context file: {dashboard_title}"
    if document.source_type == DashboardChatSourceType.DASHBOARD_EXPORT.value:
        chart_id = chart_id_from_source_identifier(document.source_identifier)
        if chart_id is not None and chart_id in chart_lookup:
            return f"Chart: {chart_lookup[chart_id]}"
        return f"Dashboard export: {dashboard_title}"
    if document.source_type == DashboardChatSourceType.DBT_MANIFEST.value:
        return f"dbt manifest: {table_name or document.source_identifier}"
    if document.source_type == DashboardChatSourceType.DBT_CATALOG.value:
        return f"dbt catalog: {table_name or document.source_identifier}"
    return document.source_identifier


def compact_snippet(content: str, max_length: int = 220) -> str:
    """Collapse whitespace and trim long snippets for citations."""
    normalized = " ".join(content.split())
    if len(normalized) <= max_length:
        return normalized
    return normalized[: max_length - 3].rstrip() + "..."


def citation_snippet(
    *,
    document: DashboardChatRetrievedDocument,
    dashboard_title: str,
    chart_lookup: dict[int, str],
) -> str:
    """Return the user-facing source subtitle for supported citation types."""
    if document.source_type == DashboardChatSourceType.ORG_CONTEXT.value:
        return "Studied context about the organization from the organization context file."

    if document.source_type == DashboardChatSourceType.DASHBOARD_CONTEXT.value:
        return f'Studied context about "{dashboard_title}" from the dashboard context file.'

    if document.source_type == DashboardChatSourceType.DASHBOARD_EXPORT.value:
        chart_id = chart_id_from_source_identifier(document.source_identifier)
        chart_title = chart_lookup.get(chart_id)
        if chart_title:
            return f'Reviewed chart configuration and metadata for "{chart_title}".'

    return compact_snippet(document.content)


def citation_url(
    *,
    document: DashboardChatRetrievedDocument,
    dashboard_id: int | None,
    table_name: str | None,
) -> str | None:
    """Build the most useful frontend destination for one citation."""
    if document.source_type == DashboardChatSourceType.ORG_CONTEXT.value:
        return "/settings/organization"

    if document.source_type == DashboardChatSourceType.DASHBOARD_CONTEXT.value:
        if dashboard_id is None:
            return "/settings/organization"
        return f"/settings/organization?dashboard_id={dashboard_id}"

    if document.source_type == DashboardChatSourceType.DASHBOARD_EXPORT.value:
        chart_id = chart_id_from_source_identifier(document.source_identifier)
        if chart_id is not None:
            return f"/charts/{chart_id}"
        if dashboard_id is None:
            return None
        return f"/dashboards/{dashboard_id}"

    if document.source_type in {
        DashboardChatSourceType.DBT_MANIFEST.value,
        DashboardChatSourceType.DBT_CATALOG.value,
    }:
        if table_name is None:
            return None
        return explore_table_url(table_name)

    return None


def explore_table_url(table_name: str) -> str | None:
    """Build one Explore deep link from a schema-qualified table name."""
    schema_name, _, raw_table_name = table_name.partition(".")
    if not schema_name or not raw_table_name:
        return None
    return f"/explore?{urlencode({'schema_name': schema_name, 'table_name': raw_table_name})}"


def build_tool_document_payload(
    document: DashboardChatRetrievedDocument,
    allowlist: DashboardChatAllowlist,
    dashboard_export: dict[str, Any],
) -> dict[str, Any]:
    """Convert a runtime retrieval result into the tool payload shape."""
    metadata: dict[str, Any] = {
        "type": prototype_doc_type(document.source_type),
        "source_type": document.source_type,
        "source_identifier": document.source_identifier,
    }
    chart_id = chart_id_from_source_identifier(document.source_identifier)
    if chart_id is not None:
        metadata["chart_id"] = chart_id
        metadata["dashboard_id"] = document.dashboard_id
        chart_meta = build_chart_tool_metadata(chart_id, dashboard_export)
        if chart_meta:
            metadata.update(chart_meta)
    unique_id = unique_id_from_source_identifier(document.source_identifier)
    if unique_id:
        metadata["dbt_unique_id"] = unique_id
        metadata["table_name"] = allowlist.unique_id_to_table.get(unique_id)
    return {
        "doc_id": document.document_id,
        "content": document.content,
        "metadata": metadata,
        "similarity_score": document.distance,
    }


def build_chart_tool_metadata(
    chart_id: int,
    dashboard_export: dict[str, Any],
) -> dict[str, Any]:
    """Return structured chart metadata that nudges the tool loop toward exact chart fields."""
    chart = next(
        (c for c in (dashboard_export.get("charts") or []) if c.get("id") == chart_id),
        None,
    )
    if chart is None:
        return {}

    preferred_table = build_dashboard_chat_table_name(
        chart.get("schema_name"),
        chart.get("table_name"),
    )
    metric_cols = chart_metric_columns(chart)
    dimension_cols = chart_dimension_columns(chart)
    time_col = chart_time_column(chart, dimension_cols)
    payload: dict[str, Any] = {
        "chart_title": str(chart.get("title") or ""),
        "chart_type": str(chart.get("chart_type") or ""),
    }
    if preferred_table:
        payload["preferred_table"] = preferred_table
    if metric_cols:
        payload["metric_columns"] = metric_cols
    if dimension_cols:
        payload["dimension_columns"] = dimension_cols
    if time_col:
        payload["time_column"] = time_col
    return payload


def prototype_doc_type(source_type: str) -> str:
    """Map Dalgo source types into the prototype doc-type vocabulary."""
    if source_type == DashboardChatSourceType.DASHBOARD_EXPORT.value:
        return "chart"
    if source_type in {
        DashboardChatSourceType.DBT_MANIFEST.value,
        DashboardChatSourceType.DBT_CATALOG.value,
    }:
        return "dbt_model"
    return "context"


def chart_metric_columns(chart: dict[str, Any]) -> list[str]:
    """Extract the most likely metric columns from one chart export payload."""
    extra_config = chart.get("extra_config") or {}
    metrics: list[str] = []
    for metric in extra_config.get("metrics") or []:
        if isinstance(metric, str) and metric.strip():
            metrics.append(metric.strip())
            continue
        if isinstance(metric, dict):
            for key in ["column", "name", "field", "metric", "metric_column"]:
                value = metric.get(key)
                if isinstance(value, str) and value.strip():
                    metrics.append(value.strip())
                    break
    for key in [
        "metric_col",
        "metric_column",
        "measure_col",
        "measure_column",
        "value_column",
        "y_axis_column",
    ]:
        value = extra_config.get(key)
        if isinstance(value, str) and value.strip():
            metrics.append(value.strip())
    return list(dict.fromkeys(metrics))


def chart_dimension_columns(chart: dict[str, Any]) -> list[str]:
    """Extract dimension-like fields from one chart export payload."""
    extra_config = chart.get("extra_config") or {}
    dimensions: list[str] = []
    for key in ["dimension_col", "extra_dimension", "group_by", "category_column", "x_axis_column"]:
        value = extra_config.get(key)
        if isinstance(value, str) and value.strip():
            dimensions.append(value.strip())
    for value in extra_config.get("dimensions") or []:
        if isinstance(value, str) and value.strip():
            dimensions.append(value.strip())
    return list(dict.fromkeys(dimensions))


def chart_time_column(
    chart: dict[str, Any],
    dimension_columns: Sequence[str],
) -> str | None:
    """Extract or infer the chart's time dimension when one is present."""
    extra_config = chart.get("extra_config") or {}
    for key in ["time_column", "time_dimension", "date_column"]:
        value = extra_config.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    for dimension in dimension_columns:
        if looks_like_time_dimension(dimension):
            return dimension
    return None


def looks_like_time_dimension(column_name: str) -> bool:
    """Return whether a dimension name probably represents time bucketing."""
    normalized_column = column_name.lower()
    return any(
        token in normalized_column
        for token in ["date", "day", "week", "month", "quarter", "year", "time"]
    )


def get_or_embed_query(
    vector_store,
    query_text: str,
    query_embeddings: dict[str, list[float]],
) -> list[float]:
    """Cache embeddings per query string during one turn."""
    if query_text not in query_embeddings:
        query_embeddings[query_text] = vector_store.embed_query(query_text)
    return query_embeddings[query_text]
