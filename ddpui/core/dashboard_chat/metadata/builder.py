"""Build dashboard-scoped metadata artifacts for chat."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import hashlib
from typing import Any

from django.utils import timezone

from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import (
    DashboardChatAllowlist,
    DashboardChatAllowlistBuilder,
)
from ddpui.core.dashboard_chat.metadata.schemas import (
    DashboardChatChartRegistryEntry,
    DashboardChatMetadataArtifactPayload,
    DashboardChatChartFilterSpec,
    DashboardChatChartMetricSpec,
    DashboardChatColumnRangeProfile,
    DashboardChatMetadataChartUsage,
    DashboardChatMetadataColumn,
    DashboardChatMetadataJoinPath,
    DashboardChatMetadataTable,
    DashboardChatTableStatistics,
)
from ddpui.core.dashboard_chat.orchestration.retrieval_support import (
    chart_dimension_columns,
    chart_metric_columns,
    chart_time_column,
)
from ddpui.core.dashboard_chat.warehouse.warehouse_access_tools import DashboardChatWarehouseTools
from ddpui.models.dashboard import Dashboard
from ddpui.models.org import Org
from ddpui.services.dashboard_service import DashboardService


def _iso_now() -> str:
    return timezone.now().isoformat()


def _is_numeric_type(data_type: str) -> bool:
    lowered_type = data_type.lower()
    return any(token in lowered_type for token in ["int", "float", "double", "decimal", "numeric", "real"])


def _is_text_like_type(data_type: str) -> bool:
    lowered_type = data_type.lower()
    return any(token in lowered_type for token in ["char", "text", "string", "varchar"])


def _is_time_type(data_type: str) -> bool:
    lowered_type = data_type.lower()
    return any(token in lowered_type for token in ["date", "time", "timestamp"])


def _layer_for_schema(schema_name: str) -> str:
    lowered = schema_name.lower()
    if "staging" in lowered:
        return "staging"
    if "intermediate" in lowered:
        return "intermediate"
    if "mart" in lowered or "marts" in lowered:
        return "marts"
    return "prod"


def build_chart_registry_from_dashboard_export(
    dashboard_export: dict[str, Any],
) -> list[dict[str, Any]]:
    """Build the compact always-on chart registry payload from a dashboard export."""
    builder = DashboardChatMetadataArtifactBuilder()
    return [
        builder._chart_registry_entry(chart).model_dump(mode="json")
        for chart in list(dashboard_export.get("charts") or [])
    ]


@dataclass
class DashboardChatMetadataBuildInputs:
    """Structured build inputs for one dashboard artifact."""

    org: Org
    dashboard: Dashboard
    dashboard_export: dict[str, Any]
    allowlist: DashboardChatAllowlist
    dbt_index: dict[str, Any]
    warehouse_tools: DashboardChatWarehouseTools


class DashboardChatMetadataArtifactBuilder:
    """Build dashboard-scoped metadata from chart export, manifest lineage, and warehouse schema.

    The dbt manifest is an explicit build-time dependency. Runtime no longer uses raw dbt
    artifacts, but metadata builds still require `target/manifest.json` for lineage and
    YAML-authored model/column documentation.
    """

    def build_for_dashboard(
        self,
        *,
        org: Org,
        dashboard: Dashboard,
        warehouse_tools: DashboardChatWarehouseTools,
    ) -> DashboardChatMetadataArtifactPayload:
        dashboard_export = DashboardService.export_dashboard_context_for_dashboard(dashboard, org)
        manifest_json = DashboardChatAllowlistBuilder.load_required_manifest_json(org.dbt)
        allowlist = DashboardChatAllowlistBuilder.build(
            dashboard_export,
            manifest_json=manifest_json,
        )
        dbt_index = DashboardChatAllowlistBuilder.build_dbt_index(manifest_json, allowlist)
        inputs = DashboardChatMetadataBuildInputs(
            org=org,
            dashboard=dashboard,
            dashboard_export=dashboard_export,
            allowlist=allowlist,
            dbt_index=dbt_index,
            warehouse_tools=warehouse_tools,
        )
        return self._build_payload(inputs)

    def _build_payload(
        self,
        inputs: DashboardChatMetadataBuildInputs,
    ) -> DashboardChatMetadataArtifactPayload:
        charts = list(inputs.dashboard_export.get("charts") or [])
        chart_registry = [self._chart_registry_entry(chart) for chart in charts]
        chart_table_map = self._chart_table_map(chart_registry)
        resources = inputs.dbt_index.get("resources_by_unique_id") or {}
        schema_snippets = inputs.warehouse_tools.get_schema_snippets(
            inputs.allowlist.prioritized_tables()
        )
        tables = [
            self._build_table_metadata(
                table_name=table_name,
                resources=resources,
                schema_snippets=schema_snippets,
                chart_registry=chart_registry,
                warehouse_tools=inputs.warehouse_tools,
            )
            for table_name in inputs.allowlist.prioritized_tables()
        ]
        source_fingerprint = self._fingerprint(
            dashboard_id=inputs.dashboard.id,
            chart_registry=chart_registry,
            tables=tables,
        )
        return DashboardChatMetadataArtifactPayload(
            dashboard_id=inputs.dashboard.id,
            org_id=inputs.org.id,
            dashboard_title=str(inputs.dashboard.title or ""),
            dashboard_description=str(inputs.dashboard.description or ""),
            built_at=_iso_now(),
            source_fingerprint=source_fingerprint,
            allowlisted_tables=inputs.allowlist.prioritized_tables(),
            chart_table_map=chart_table_map,
            tables=tables,
            join_paths=[],
        )

    def _chart_registry_entry(self, chart: dict[str, Any]) -> DashboardChatChartRegistryEntry:
        dimensions = chart_dimension_columns(chart)
        preferred_table = chart.get("schema_name") and chart.get("table_name")
        metrics = []
        for metric in (chart.get("extra_config") or {}).get("metrics") or []:
            if not isinstance(metric, dict):
                continue
            metrics.append(
                DashboardChatChartMetricSpec(
                    column=str(metric.get("column") or ""),
                    aggregation=str(metric.get("aggregation") or ""),
                    alias=str(metric.get("alias") or ""),
                )
            )
        filters = []
        for chart_filter in (chart.get("extra_config") or {}).get("filters") or []:
            if not isinstance(chart_filter, dict):
                continue
            filters.append(
                DashboardChatChartFilterSpec(
                    column=str(chart_filter.get("column") or ""),
                    operator=str(chart_filter.get("operator") or ""),
                    value=str(chart_filter.get("value") or ""),
                )
            )
        return DashboardChatChartRegistryEntry(
            chart_id=int(chart.get("id")),
            title=str(chart.get("title") or ""),
            section=str((chart.get("extra_config") or {}).get("section_name") or ""),
            chart_type=str(chart.get("chart_type") or ""),
            description=str(chart.get("description") or ""),
            preferred_table=(
                f"{chart.get('schema_name')}.{chart.get('table_name')}" if preferred_table else ""
            ),
            metrics=metrics,
            metric_columns=chart_metric_columns(chart),
            dimension_columns=dimensions,
            filters=filters,
            time_column=chart_time_column(chart, dimensions),
        )

    def _chart_table_map(
        self,
        chart_registry: list[DashboardChatChartRegistryEntry],
    ) -> dict[str, list[str]]:
        mapping: dict[str, list[str]] = defaultdict(list)
        for chart in chart_registry:
            if chart.preferred_table:
                mapping[chart.preferred_table].append(str(chart.chart_id))
        return dict(mapping)

    def _build_table_metadata(
        self,
        *,
        table_name: str,
        resources: dict[str, dict[str, Any]],
        schema_snippets: dict[str, Any],
        chart_registry: list[DashboardChatChartRegistryEntry],
        warehouse_tools: DashboardChatWarehouseTools,
    ) -> DashboardChatMetadataTable:
        matched_resources = [
            resource for resource in resources.values() if resource.get("table") == table_name
        ]
        unique_ids = sorted(
            {str(resource.get("unique_id") or "") for resource in matched_resources if resource.get("unique_id")}
        )
        resource = matched_resources[0] if matched_resources else {}
        snippet = schema_snippets.get(table_name)
        resource_columns = {
            str(column.get("name") or "").lower(): column for column in (resource.get("columns") or [])
        }
        columns = []
        for column in (snippet.columns if snippet is not None else []):
            column_name = str(column.get("name") or "")
            resource_column = resource_columns.get(column_name.lower()) or {}
            data_type = str(
                column.get("data_type")
                or column.get("type")
                or resource_column.get("data_type")
                or resource_column.get("type")
                or ""
            )
            description = str(resource_column.get("description") or "")
            columns.append(
                DashboardChatMetadataColumn(
                    column_name=column_name,
                    data_type=data_type,
                    description=description,
                )
            )

        schema_name, _, raw_table_name = table_name.partition(".")
        columns, statistics = self._safe_table_statistics(warehouse_tools, table_name, columns)
        chart_usage = [
            DashboardChatMetadataChartUsage(
                chart_id=chart.chart_id,
                chart_title=chart.title,
                relation="direct",
                chart_type=chart.chart_type,
                metrics=self._chart_metric_specs(chart),
                dimensions=chart.dimension_columns,
                filters=self._chart_filter_specs(chart),
                time_column=chart.time_column,
            )
            for chart in chart_registry
            if chart.preferred_table == table_name
        ]
        table_description = str(resource.get("description") or "")
        total_row_count = statistics.get("total_row_count")
        return DashboardChatMetadataTable(
            table_name=table_name,
            dbt_unique_id=unique_ids[0] if unique_ids else "",
            schema_name=schema_name,
            model_name=str(resource.get("name") or raw_table_name),
            layer=_layer_for_schema(schema_name),
            description=table_description,
            upstream_models=list(resource.get("upstream") or []),
            chart_usage=chart_usage,
            statistics=DashboardChatTableStatistics(
                row_count=total_row_count,
                column_count=len(columns),
                distinct_counts=dict(statistics.get("distinct_counts") or {}),
            ),
            columns=columns,
        )

    def _safe_table_statistics(
        self,
        warehouse_tools: DashboardChatWarehouseTools,
        table_name: str,
        columns: list[DashboardChatMetadataColumn],
    ) -> tuple[list[DashboardChatMetadataColumn], dict[str, Any]]:
        stats: dict[str, Any] = {}
        quoted_table_name = self._quote_table_name(warehouse_tools, table_name)
        total_row_count: int | None = None
        try:
            rows = warehouse_tools.execute_sql(
                f"SELECT COUNT(*) AS total_rows FROM {quoted_table_name}"
            )
            if rows:
                total_row_count = int(rows[0].get("total_rows") or 0)
                stats["total_row_count"] = total_row_count
        except Exception:
            stats["total_row_count"] = None
        if not columns:
            return columns, stats

        profiled_columns = [column.model_copy(deep=True) for column in columns]
        profiled_columns = self._apply_range_and_null_stats(
            warehouse_tools,
            quoted_table_name,
            profiled_columns,
            total_row_count,
        )
        profiled_columns, distinct_counts, low_cardinality_samples = self._apply_distinct_and_sample_stats(
            warehouse_tools,
            quoted_table_name,
            profiled_columns,
        )
        if distinct_counts:
            stats["distinct_counts"] = distinct_counts
        if low_cardinality_samples:
            stats["low_cardinality_samples"] = low_cardinality_samples
        return profiled_columns, stats

    def _apply_range_and_null_stats(
        self,
        warehouse_tools: DashboardChatWarehouseTools,
        quoted_table_name: str,
        columns: list[DashboardChatMetadataColumn],
        total_row_count: int | None,
    ) -> list[DashboardChatMetadataColumn]:
        if not columns:
            return columns

        expressions: list[str] = []
        for index, column in enumerate(columns):
            quoted_column_name = self._quote_column_name(warehouse_tools, column.name)
            expressions.append(
                f"SUM(CASE WHEN {quoted_column_name} IS NULL THEN 1 ELSE 0 END) AS c{index}_null_count"
            )
            if _is_numeric_type(column.data_type):
                expressions.append(f"MIN({quoted_column_name}) AS c{index}_numeric_min")
                expressions.append(f"MAX({quoted_column_name}) AS c{index}_numeric_max")
            if _is_time_type(column.data_type):
                expressions.append(f"MIN({quoted_column_name}) AS c{index}_time_min")
                expressions.append(f"MAX({quoted_column_name}) AS c{index}_time_max")

        if not expressions:
            return columns
        try:
            rows = warehouse_tools.execute_sql(
                "SELECT " + ", ".join(expressions) + f" FROM {quoted_table_name}"
            )
        except Exception:
            return columns
        if not rows:
            return columns
        row = rows[0]
        for index, column in enumerate(columns):
            null_count = row.get(f"c{index}_null_count")
            if null_count is not None and total_row_count not in {None, 0}:
                column.statistics.null_percentage = round(
                    (float(null_count) / float(total_row_count)) * 100,
                    2,
                )
                column.statistics.nullable = bool(null_count)
            if _is_numeric_type(column.data_type):
                numeric_min = row.get(f"c{index}_numeric_min")
                numeric_max = row.get(f"c{index}_numeric_max")
                if column.statistics.range_profile is None:
                    column.statistics.range_profile = DashboardChatColumnRangeProfile()
                column.statistics.range_profile.numeric_min = (
                    float(numeric_min) if numeric_min is not None else None
                )
                column.statistics.range_profile.numeric_max = (
                    float(numeric_max) if numeric_max is not None else None
                )
            if _is_time_type(column.data_type):
                time_min = row.get(f"c{index}_time_min")
                time_max = row.get(f"c{index}_time_max")
                if column.statistics.range_profile is None:
                    column.statistics.range_profile = DashboardChatColumnRangeProfile()
                column.statistics.range_profile.time_min = (
                    str(time_min) if time_min is not None else None
                )
                column.statistics.range_profile.time_max = (
                    str(time_max) if time_max is not None else None
                )
        return columns

    def _apply_distinct_and_sample_stats(
        self,
        warehouse_tools: DashboardChatWarehouseTools,
        quoted_table_name: str,
        columns: list[DashboardChatMetadataColumn],
    ) -> tuple[list[DashboardChatMetadataColumn], dict[str, int | None], dict[str, list[str]]]:
        profiled_candidates = list(enumerate(columns[:25]))
        if not profiled_candidates:
            return columns, {}, {}

        distinct_expressions = []
        for index, column in profiled_candidates:
            quoted_column_name = self._quote_column_name(warehouse_tools, column.name)
            distinct_expressions.append(
                f"COUNT(DISTINCT {quoted_column_name}) AS c{index}_distinct_count"
            )

        distinct_counts: dict[str, int | None] = {}
        low_cardinality_samples: dict[str, list[str]] = {}
        try:
            rows = warehouse_tools.execute_sql(
                "SELECT " + ", ".join(distinct_expressions) + f" FROM {quoted_table_name}"
            )
        except Exception:
            return columns, distinct_counts, low_cardinality_samples
        if not rows:
            return columns, distinct_counts, low_cardinality_samples
        row = rows[0]
        for index, column in profiled_candidates:
            distinct_count = row.get(f"c{index}_distinct_count")
            parsed_count = int(distinct_count) if distinct_count is not None else None
            distinct_counts[column.name] = parsed_count
            if (
                parsed_count is not None
                and parsed_count <= 20
                and not column.pii
                and _is_text_like_type(column.data_type)
            ):
                try:
                    quoted_column_name = self._quote_column_name(warehouse_tools, column.name)
                    sample_rows = warehouse_tools.execute_sql(
                        "SELECT DISTINCT "
                        f"{quoted_column_name} AS value "
                        f"FROM {quoted_table_name} "
                        f"WHERE {quoted_column_name} IS NOT NULL "
                        "ORDER BY 1 "
                        "LIMIT 10"
                    )
                    samples = [
                        str(sample_row.get("value"))
                        for sample_row in sample_rows
                        if sample_row.get("value") is not None
                    ]
                    column.statistics.sample_values = samples
                    low_cardinality_samples[column.name] = samples
                except Exception:
                    continue
        return columns, distinct_counts, low_cardinality_samples

    def _quote_table_name(
        self,
        warehouse_tools: DashboardChatWarehouseTools,
        table_name: str,
    ) -> str:
        """Quote a physical table name safely for build-time profiling queries."""
        parsed = warehouse_tools._parse_table_name(table_name)
        if parsed is None:
            return table_name
        schema_name, bare_table_name = parsed
        if getattr(warehouse_tools.org_warehouse, "wtype", None) == "bigquery":
            return warehouse_tools._quote_bigquery_table_ref(schema_name, bare_table_name)
        return (
            f'{warehouse_tools._quote_postgres_identifier(schema_name)}.'
            f'{warehouse_tools._quote_postgres_identifier(bare_table_name)}'
        )

    def _quote_column_name(
        self,
        warehouse_tools: DashboardChatWarehouseTools,
        column_name: str,
    ) -> str:
        """Quote a physical column name safely for build-time profiling queries."""
        normalized = warehouse_tools._normalize_identifier_component(column_name, "column name")
        if getattr(warehouse_tools.org_warehouse, "wtype", None) == "bigquery":
            return f"`{normalized}`"
        return warehouse_tools._quote_postgres_identifier(normalized)

    def _build_join_paths(
        self,
        tables: list[DashboardChatMetadataTable],
    ) -> list[DashboardChatMetadataJoinPath]:
        joins: list[DashboardChatMetadataJoinPath] = []
        for source in tables:
            source_ids = set(source.candidate_unique_id_columns) | set(source.natural_keys)
            if not source_ids:
                continue
            for target in tables:
                if source.table_name == target.table_name:
                    continue
                target_ids = set(target.candidate_unique_id_columns) | set(target.natural_keys)
                shared_ids = sorted(source_ids & target_ids)
                if not shared_ids:
                    continue
                cardinality = self._infer_join_cardinality(source, target, shared_ids[0])
                joins.append(
                    DashboardChatMetadataJoinPath(
                        source_table=source.table_name,
                        target_table=target.table_name,
                        via_columns=shared_ids[:3],
                        cardinality=cardinality,
                        preferred=cardinality in {"many_to_one", "one_to_one"},
                        dashboard_relevant=True,
                        required_for_entity_names=any("name" in column.name.lower() for column in target.columns),
                        required_for_metrics=any(column.semantic_role == "metric" for column in target.columns),
                    )
                )
        return joins

    def _infer_join_cardinality(
        self,
        source: DashboardChatMetadataTable,
        target: DashboardChatMetadataTable,
        shared_column: str,
    ) -> str:
        source_rows = source.total_row_count
        target_rows = target.total_row_count
        source_distinct = source.statistics.distinct_counts.get(shared_column)
        target_distinct = target.statistics.distinct_counts.get(shared_column)
        if None in {source_rows, target_rows, source_distinct, target_distinct}:
            return "unknown"
        if source_distinct == source_rows and target_distinct == target_rows:
            return "one_to_one"
        if source_distinct < source_rows and target_distinct == target_rows:
            return "many_to_one"
        if source_distinct == source_rows and target_distinct < target_rows:
            return "one_to_many"
        if source_distinct < source_rows and target_distinct < target_rows:
            return "many_to_many"
        return "unknown"

    def rebuild_derived_indexes(
        self,
        payload: DashboardChatMetadataArtifactPayload,
    ) -> DashboardChatMetadataArtifactPayload:
        """Recompute join graph and search indexes after enrichment overrides."""
        payload.join_paths = self._build_join_paths(payload.tables)
        return payload

    def _chart_metric_specs(
        self,
        chart: DashboardChatChartRegistryEntry,
    ) -> list[DashboardChatChartMetricSpec]:
        return [metric.model_copy(deep=True) for metric in chart.metrics]

    def _chart_filter_specs(
        self,
        chart: DashboardChatChartRegistryEntry,
    ) -> list[DashboardChatChartFilterSpec]:
        return [chart_filter.model_copy(deep=True) for chart_filter in chart.filters]

    def _fingerprint(
        self,
        *,
        dashboard_id: int,
        chart_registry: list[DashboardChatChartRegistryEntry],
        tables: list[DashboardChatMetadataTable],
    ) -> str:
        fingerprint_input = "|".join(
            [
                str(dashboard_id),
                *(f"{chart.chart_id}:{chart.preferred_table}" for chart in chart_registry),
                *(f"{table.table_name}:{table.total_row_count}" for table in tables),
            ]
        )
        return hashlib.sha256(fingerprint_input.encode("utf-8")).hexdigest()[:24]
