"""LLM enrichment for dashboard chat metadata artifacts."""

from __future__ import annotations

from datetime import date, datetime
import json
import os
from decimal import Decimal
from typing import Any

from openai import OpenAI

from ddpui.core.dashboard_chat.metadata.schemas import (
    DashboardChatInferredColumn,
    DashboardChatInferredTable,
    DashboardChatMetadataArtifactPayload,
    DashboardChatMetadataColumn,
    DashboardChatMetadataTable,
)
from ddpui.core.dashboard_chat.warehouse.warehouse_access_tools import DashboardChatWarehouseTools
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.openai_client import get_shared_openai_client

logger = CustomLogger("dashboard_chat")

MAX_PROFILING_REQUESTS = 5

METADATA_PROFILING_PLAN_SYSTEM_PROMPT = """You are planning extra warehouse profiling for ONE table before semantic metadata inference.

Your job is to decide whether more evidence is needed to infer:
- row grain
- natural keys
- candidate unique id columns
- primary time columns and their meaning

RULES
1. Use the observed facts first. Request extra profiling only if the current evidence is insufficient.
2. Never request profiling for columns that do not exist.
3. Keep requests minimal. Use at most 5 requests total.
4. Prefer these request kinds only:
   - "uniqueness_check": to test whether one or more columns uniquely identify rows
   - "sample_rows": to inspect a few rows for key/time semantics
5. Do not request joins or cross-table profiling here.
6. Output valid JSON only.

Return JSON in this exact shape:
{
  "profiling_requests": [
    {
      "kind": "uniqueness_check",
      "columns": ["column_a", "column_b"]
    },
    {
      "kind": "sample_rows",
      "columns": ["column_a", "column_b", "column_c"],
      "limit": 5
    }
  ]
}
"""


METADATA_ENRICHMENT_SYSTEM_PROMPT = """You are enriching structured table metadata for a dashboard chat SQL agent.

Your job is to infer semantic metadata for ONE table from observed physical facts and optional profiling results.

RULES
1. Never invent tables, columns, joins, metrics, or time coverage that are not supported by the input.
2. The observed facts are the source of truth for physical data. Infer semantics from those facts; do not contradict them.
3. Infer, do not guess blindly. If the evidence is weak, leave fields empty or express uncertainty through lower confidence and ambiguity notes.
4. Never assume snapshot semantics, cumulative semantics, latest-row semantics, as-of semantics, or one-row-per-entity-per-time semantics unless the observed facts or profiling results explicitly support them.
5. Be generic and reusable. Do not mention a specific NGO unless the table facts make that necessary.
6. For row-grain tables, be explicit about the inferred entity/event grain.
7. For aggregate tables, be explicit that they are not suitable for row-level name lists unless joined to a row-grain table.
8. For each column override, only set fields you are confident about.
9. Output valid JSON only.

Return JSON in this exact shape:
{
  "table_purpose": "string",
  "table_type": "fact | dim | aggregate | bridge | row_grain",
  "row_grain": "string",
  "row_grain_confidence": 0.0,
  "primary_entities": ["..."],
  "natural_keys": ["..."],
  "natural_key_confidence": 0.0,
  "candidate_unique_id_columns": ["..."],
  "primary_time_columns": ["..."],
  "primary_time_confidence": 0.0,
  "preferred_use_cases": ["..."],
  "anti_pattern_use_cases": ["..."],
  "example_questions": ["..."],
  "entity_counting_guidance": {
    "entity_name": "string guidance"
  },
  "grain_warnings": ["..."],
  "required_join_patterns": ["..."],
  "ambiguity_notes": ["..."],
  "evidence": ["..."],
  "column_overrides": [
    {
      "name": "column_name",
      "semantic_role": "identifier | dimension | metric | time | status | label",
      "entity_tags": ["..."],
      "measure_tags": ["..."],
      "pii": true,
      "pii_type": "direct_identifier | quasi_identifier | sensitive_attribute",
      "aggregation_hints": ["..."],
      "filter_usefulness": "low | medium | high",
      "join_usefulness": "low | medium | high",
      "ambiguity_notes": ["..."],
      "confidence": 0.0,
      "evidence": ["..."]
    }
  ]
}
"""


class DashboardChatMetadataEnricher:
    """Use a reasoning model to infer semantic metadata from observed facts."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        model: str = "o4-mini",
        timeout_seconds: float = 300.0,
        client: OpenAI | None = None,
    ):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model
        self.client = client or (
            get_shared_openai_client(
                self.api_key,
                timeout_seconds=timeout_seconds,
                max_retries=0,
            )
            if self.api_key
            else None
        )

    def enrich_payload(
        self,
        *,
        payload: DashboardChatMetadataArtifactPayload,
        org_context_markdown: str,
        dashboard_context_markdown: str,
        warehouse_tools: DashboardChatWarehouseTools | None = None,
    ) -> DashboardChatMetadataArtifactPayload:
        """Enrich every table in one dashboard payload."""
        if self.client is None:
            return payload

        enriched_tables: list[DashboardChatMetadataTable] = []
        for table in payload.tables:
            try:
                profiling_results = self._run_planned_profiling(
                    table=table,
                    warehouse_tools=warehouse_tools,
                )
                enriched_tables.append(
                    self._enrich_table(
                        table=table,
                        dashboard_title=payload.dashboard_title,
                        dashboard_description=payload.dashboard_description,
                        org_context_markdown=org_context_markdown,
                        dashboard_context_markdown=dashboard_context_markdown,
                        profiling_results=profiling_results,
                    )
                )
            except Exception:
                logger.exception(
                    "Dashboard metadata enrichment failed for %s",
                    table.table_name,
                )
                enriched_tables.append(table)

        enriched_payload = payload.model_copy(deep=True)
        enriched_payload.tables = enriched_tables
        return enriched_payload

    def _run_planned_profiling(
        self,
        *,
        table: DashboardChatMetadataTable,
        warehouse_tools: DashboardChatWarehouseTools | None,
    ) -> list[dict[str, Any]]:
        if self.client is None or warehouse_tools is None:
            return []

        planning_payload = {
            "table_name": table.table_name,
            "observed": self._serialize_observed_table(table),
            "columns": [self._serialize_observed_column(column) for column in table.columns],
        }
        response = self.client.chat.completions.create(
            model=self.model,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": METADATA_PROFILING_PLAN_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": self._dump_prompt_json(planning_payload),
                },
            ],
        )
        content = response.choices[0].message.content or "{}"
        plan = json.loads(content)
        requests = list(plan.get("profiling_requests") or [])[:MAX_PROFILING_REQUESTS]
        results: list[dict[str, Any]] = []
        for request in requests:
            try:
                result = self._execute_profiling_request(
                    table=table,
                    request=request,
                    warehouse_tools=warehouse_tools,
                )
            except Exception:
                logger.exception(
                    "Dashboard metadata profiling request failed for %s",
                    table.table_name,
                )
                continue
            if result is not None:
                results.append(result)
        return results

    def _execute_profiling_request(
        self,
        *,
        table: DashboardChatMetadataTable,
        request: dict[str, Any],
        warehouse_tools: DashboardChatWarehouseTools,
    ) -> dict[str, Any] | None:
        kind = str(request.get("kind") or "").strip()
        columns = [
            str(column_name).strip()
            for column_name in (request.get("columns") or [])
            if str(column_name).strip()
        ]
        observed_columns = {column.name for column in table.columns}
        columns = [column_name for column_name in columns if column_name in observed_columns]
        if not kind or not columns:
            return None
        if kind == "uniqueness_check":
            return self._execute_uniqueness_check(
                table_name=table.table_name,
                columns=columns,
                warehouse_tools=warehouse_tools,
            )
        if kind == "sample_rows":
            return self._execute_sample_rows(
                table_name=table.table_name,
                columns=columns,
                limit=int(request.get("limit") or 5),
                warehouse_tools=warehouse_tools,
            )
        return None

    def _execute_uniqueness_check(
        self,
        *,
        table_name: str,
        columns: list[str],
        warehouse_tools: DashboardChatWarehouseTools,
    ) -> dict[str, Any]:
        quoted_table_name = self._quote_table_name(warehouse_tools, table_name)
        quoted_columns = [self._quote_column_name(warehouse_tools, column) for column in columns]
        if len(quoted_columns) == 1:
            distinct_expression = f"COUNT(DISTINCT {quoted_columns[0]}) AS distinct_rows"
        else:
            distinct_expression = (
                "COUNT(DISTINCT ("
                + ", ".join(quoted_columns)
                + ")) AS distinct_rows"
            )
        null_expressions = [
            f"SUM(CASE WHEN {quoted_column} IS NULL THEN 1 ELSE 0 END) AS c{index}_nulls"
            for index, quoted_column in enumerate(quoted_columns)
        ]
        rows = warehouse_tools.execute_sql(
            "SELECT COUNT(*) AS total_rows, "
            + distinct_expression
            + ", "
            + ", ".join(null_expressions)
            + f" FROM {quoted_table_name}"
        )
        row = rows[0] if rows else {}
        return {
            "kind": "uniqueness_check",
            "columns": columns,
            "total_rows": row.get("total_rows"),
            "distinct_rows": row.get("distinct_rows"),
            "null_counts": {
                columns[index]: row.get(f"c{index}_nulls")
                for index in range(len(columns))
            },
        }

    def _execute_sample_rows(
        self,
        *,
        table_name: str,
        columns: list[str],
        limit: int,
        warehouse_tools: DashboardChatWarehouseTools,
    ) -> dict[str, Any]:
        quoted_table_name = self._quote_table_name(warehouse_tools, table_name)
        quoted_columns = [self._quote_column_name(warehouse_tools, column) for column in columns]
        rows = warehouse_tools.execute_sql(
            "SELECT "
            + ", ".join(quoted_columns)
            + f" FROM {quoted_table_name} LIMIT {max(1, min(limit, 10))}"
        )
        return {
            "kind": "sample_rows",
            "columns": columns,
            "rows": rows,
        }

    def _enrich_table(
        self,
        *,
        table: DashboardChatMetadataTable,
        dashboard_title: str,
        dashboard_description: str,
        org_context_markdown: str,
        dashboard_context_markdown: str,
        profiling_results: list[dict[str, Any]],
    ) -> DashboardChatMetadataTable:
        prompt_payload = {
            "dashboard_title": dashboard_title,
            "dashboard_description": dashboard_description,
            "org_context_excerpt": (org_context_markdown or "")[:3000],
            "dashboard_context_excerpt": (dashboard_context_markdown or "")[:3000],
            "table_name": table.table_name,
            "observed_table": self._serialize_observed_table(table),
            "observed_columns": [
                self._serialize_observed_column(column)
                for column in table.columns
            ],
            "profiling_results": profiling_results,
        }
        response = self.client.chat.completions.create(
            model=self.model,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": METADATA_ENRICHMENT_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": self._dump_prompt_json(prompt_payload),
                },
            ],
        )
        content = response.choices[0].message.content or "{}"
        enrichment = json.loads(content)
        return self._apply_table_enrichment(table, enrichment)

    def _dump_prompt_json(self, payload: dict[str, Any]) -> str:
        return json.dumps(
            payload,
            ensure_ascii=False,
            default=self._json_default,
        )

    @staticmethod
    def _json_default(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        return str(value)

    def _serialize_observed_table(self, table: DashboardChatMetadataTable) -> dict[str, Any]:
        return {
            "unique_ids": table.observed.unique_ids,
            "layer": table.observed.layer,
            "schema_name": table.observed.schema_name,
            "model_name": table.observed.model_name,
            "human_label": table.observed.human_label,
            "table_description": table.observed.table_description,
            "time_coverage": table.observed.time_coverage,
            "total_row_count": table.observed.total_row_count,
            "approximate_size_hint": table.observed.approximate_size_hint,
            "column_count": table.observed.column_count,
            "chart_usage": [
                usage.model_dump(mode="json") for usage in table.observed.chart_usage
            ],
            "statistics": table.observed.statistics,
        }

    def _serialize_observed_column(self, column: DashboardChatMetadataColumn) -> dict[str, Any]:
        return {
            "name": column.observed.name,
            "data_type": column.observed.data_type,
            "description": column.observed.description,
            "nullable": column.observed.nullable,
            "null_percentage": column.observed.null_percentage,
            "distinct_count": column.observed.distinct_count,
            "sample_values": column.observed.sample_values[:10],
            "numeric_min": column.observed.numeric_min,
            "numeric_max": column.observed.numeric_max,
            "time_min": column.observed.time_min,
            "time_max": column.observed.time_max,
        }

    def _apply_table_enrichment(
        self,
        table: DashboardChatMetadataTable,
        enrichment: dict[str, Any],
    ) -> DashboardChatMetadataTable:
        table_copy = table.model_copy(deep=True)
        table_inferred = table_copy.inferred.model_copy(deep=True)
        list_fields = {
            "primary_entities",
            "natural_keys",
            "candidate_unique_id_columns",
            "primary_time_columns",
            "preferred_use_cases",
            "anti_pattern_use_cases",
            "example_questions",
            "grain_warnings",
            "required_join_patterns",
            "ambiguity_notes",
            "evidence",
        }
        for field_name in [
            "table_purpose",
            "table_type",
            "row_grain",
            "row_grain_confidence",
            "primary_entities",
            "natural_keys",
            "natural_key_confidence",
            "candidate_unique_id_columns",
            "primary_time_columns",
            "primary_time_confidence",
            "preferred_use_cases",
            "anti_pattern_use_cases",
            "example_questions",
            "entity_counting_guidance",
            "grain_warnings",
            "required_join_patterns",
            "ambiguity_notes",
            "evidence",
        ]:
            if field_name not in enrichment or not self._has_value(enrichment[field_name]):
                continue
            value = enrichment[field_name]
            if field_name in list_fields:
                value = self._normalize_string_list(value)
                if not value:
                    continue
            setattr(table_inferred, field_name, value)
        table_copy.inferred = table_inferred

        overrides_by_name = {
            str(item.get("name") or ""): item
            for item in (enrichment.get("column_overrides") or [])
            if item.get("name")
        }
        enriched_columns: list[DashboardChatMetadataColumn] = []
        for column in table_copy.columns:
            override = overrides_by_name.get(column.name)
            if not override:
                enriched_columns.append(column)
                continue
            inferred_column = column.inferred.model_copy(
                update={
                    key: self._normalize_override_value(key, value)
                    for key, value in override.items()
                    if key != "name" and self._has_value(value)
                }
            )
            enriched_columns.append(
                column.model_copy(update={"inferred": inferred_column})
            )
        table_copy.columns = enriched_columns
        return table_copy

    def _quote_table_name(
        self,
        warehouse_tools: DashboardChatWarehouseTools,
        table_name: str,
    ) -> str:
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
        normalized = warehouse_tools._normalize_identifier_component(column_name, "column name")
        if getattr(warehouse_tools.org_warehouse, "wtype", None) == "bigquery":
            return f"`{normalized}`"
        return warehouse_tools._quote_postgres_identifier(normalized)

    def _has_value(self, value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return value != ""
        if isinstance(value, (list, dict, tuple, set)):
            return len(value) > 0
        return True

    def _normalize_override_value(self, key: str, value: Any) -> Any:
        if key in {
            "entity_tags",
            "measure_tags",
            "aggregation_hints",
            "ambiguity_notes",
            "evidence",
        }:
            return self._normalize_string_list(value)
        return value

    def _normalize_string_list(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            stripped = value.strip()
            return [stripped] if stripped else []
        if isinstance(value, (list, tuple, set)):
            normalized: list[str] = []
            for item in value:
                if item is None:
                    continue
                text = str(item).strip()
                if text:
                    normalized.append(text)
            return normalized
        text = str(value).strip()
        return [text] if text else []
