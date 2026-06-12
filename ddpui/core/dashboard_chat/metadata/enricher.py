"""LLM enrichment for dashboard chat metadata artifacts."""

from __future__ import annotations

from datetime import date, datetime
import json
import os
from decimal import Decimal
from typing import Any

from openai import OpenAI

from ddpui.core.dashboard_chat.metadata.schemas import (
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
- primary filter time column and time-column meaning
- whether the table has already rolled up over detail needed for common question shapes

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

Your job is to infer semantic metadata for ONE table from observed physical facts, raw docs, chart usage, and optional profiling results.

RULES
1. Never invent tables, columns, joins, metrics, or time coverage that are not supported by the input.
2. Observed physical facts are the source of truth for names, stats, and lineage. Infer semantics from those facts; do not contradict them.
3. Infer, do not guess blindly. If the evidence is weak, leave fields empty instead of writing caveats.
4. Do not infer table-state semantics. Do not write output that implies rows should be collapsed to one final row per entity, or that values represent progress up to a reporting/current state, unless source documentation explicitly defines that metric meaning.
5. Write the final human-readable table description and column descriptions. Do not simply copy sparse dbt docs verbatim.
6. Prefer compact structured answerability guidance over broad prose lists.
7. Be generic and reusable. Do not mention a specific NGO unless the table facts make that necessary.
8. Do not put query strategy into metadata. Metadata can describe row grain, natural keys, metrics, dimensions, and the primary time column. It must not tell the SQL generator to collapse rows per entity, use max values per entity, or use reporting-state logic.
9. For repeated entity/date rows, describe only the observed grain, for example "one row per entity per date". Do not infer that values must be deduplicated to one row per entity unless source documentation explicitly defines that query requirement.
10. For time-bounded questions, metadata should support direct filtering on the primary time column and aggregation of matching rows. Do not encode instructions that replace a date window with one-row-per-entity or max-per-entity logic.
11. PII marking must be semantic, not a naive column-name rule. Mark pii=true for human names, person-identifying free text, mobile/contact numbers, and government-issued identifiers such as MGNREGA numbers, national IDs, Aadhaar IDs, voter IDs, ration-card IDs, or other official identity numbers. Mark pii=false for ordinary system-generated warehouse/application identifiers such as farmer_id, student_id, work_order_id, facility_id, school_id, district_id, or other surrogate keys when context indicates they are internal IDs rather than official identity numbers. If a column name contains "name" but the context clearly means an organization/place/program/grade/status label, do not mark it as PII.
12. Metric descriptions must not imply that values include prior rows or prior days unless source documentation explicitly defines that. Prefer wording like "recorded for this row" or "on the row date" when the table has a date column.
13. Output valid JSON only.

ANSWERABILITY FIELD MEANINGS
- retained_dimensions: dimensions physically present as columns in this table that can be filtered, grouped, or compared directly.
- rolled_up_over: dimensions or lower-grain entities that are not physically available because this table has already aggregated across them. Do not list columns that exist in this table here. Leave empty if the missing lower grain is not clear.
- comparison_axes_available: columns physically present in this table that can support comparisons.
- direct_answer_capabilities: question types this table can answer without joins or lower-grain tables.
- answerability_limitations: question needs this table cannot answer directly, with the required join or lower-grain table resolution when known.
- action_filter_rules: only domain action phrases that imply a required metric/status condition, for example "carted silt" requires a positive carted-silt metric. Do not create generic non-null rules for ordinary dimensions, dates, or dashboard filters.

GRAIN FIELD MEANINGS
- row_definition: one concise sentence describing the physical row grain.
- natural_keys: columns that define the natural row grain.
- candidate_unique_id_columns: columns that participate in a single-column or composite unique row identifier. If profiling proves a column combination uniquely identifies rows, list those columns here.
- primary_entities: entity concepts represented by rows, for example "work_order" or "farmer"; do not use raw column names unless the column name is the entity label itself.

Return JSON in this exact shape:
{
  "description": "string",
  "table_type": "fact | dim | aggregate | bridge | row_grain",
  "primary_entities": ["..."],
  "grain": {
    "row_definition": "string",
    "natural_keys": ["..."],
    "candidate_unique_id_columns": ["..."],
    "evidence": ["..."]
  },
  "temporal": {
    "primary_filter_time_column": "column_name",
    "time_column_meanings": {
      "date_time": "reporting_date"
    }
  },
  "counting": {
    "default_row_count_entity": "string",
    "entity_counting_guidance": {
      "entity_name": "string guidance"
    }
  },
  "answerability": {
    "retained_dimensions": ["..."],
    "rolled_up_over": ["..."],
    "comparison_axes_available": ["..."],
    "direct_answer_capabilities": ["..."],
    "answerability_limitations": [
      {
        "question_need": "string",
        "resolution": "requires_join | use_lower_grain_table | not_sufficient",
        "details": "string"
      }
    ],
    "action_filter_rules": [
      {
        "action": "string",
        "entity": "string",
        "required_conditions": ["..."]
      }
    ]
  },
  "column_overrides": [
    {
      "column_name": "column_name",
      "description": "string",
      "semantic_role": "identifier | dimension | metric | time | status | label",
      "value_semantics": "entity_identifier | entity_label | geography_label | additive_numeric_metric | non_additive_numeric_metric | percentage | ordinal_band | reporting_date | event_date | status_label | free_text | other",
      "pii": true
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
            "table": self._serialize_table_for_enrichment(table),
            "columns": [
                self._serialize_column_for_enrichment(table, column) for column in table.columns
            ],
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
                "COUNT(DISTINCT (" + ", ".join(quoted_columns) + ")) AS distinct_rows"
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
                columns[index]: row.get(f"c{index}_nulls") for index in range(len(columns))
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
            "table": self._serialize_table_for_enrichment(table),
            "columns": [
                self._serialize_column_for_enrichment(table, column) for column in table.columns
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

    def _serialize_table_for_enrichment(self, table: DashboardChatMetadataTable) -> dict[str, Any]:
        return {
            "dbt_unique_id": table.dbt_unique_id,
            "layer": table.layer,
            "schema_name": table.schema_name,
            "model_name": table.model_name,
            "existing_description": table.description,
            "upstream_models": table.upstream_models,
            "chart_usage": [usage.model_dump(mode="json") for usage in table.chart_usage],
            "statistics": table.statistics.model_dump(mode="json"),
            "current_primary_entities": table.primary_entities,
            "current_grain": table.grain.model_dump(mode="json"),
            "current_temporal": table.temporal.model_dump(mode="json"),
            "current_counting": table.counting.model_dump(mode="json"),
            "current_answerability": table.answerability.model_dump(mode="json"),
        }

    def _serialize_column_for_enrichment(
        self,
        table: DashboardChatMetadataTable,
        column: DashboardChatMetadataColumn,
    ) -> dict[str, Any]:
        return {
            "column_name": column.column_name,
            "data_type": column.data_type,
            "existing_description": column.description,
            "semantic_role": column.semantic_role,
            "value_semantics": column.value_semantics,
            "pii": column.pii,
            "nullable": column.statistics.nullable,
            "null_percentage": column.statistics.null_percentage,
            "distinct_count": table.statistics.distinct_counts.get(column.column_name),
            "sample_values": column.statistics.sample_values[:10],
            "range_profile": (
                column.statistics.range_profile.model_dump(mode="json")
                if column.statistics.range_profile is not None
                else None
            ),
        }

    def _apply_table_enrichment(
        self,
        table: DashboardChatMetadataTable,
        enrichment: dict[str, Any],
    ) -> DashboardChatMetadataTable:
        table_copy = table.model_copy(deep=True)

        if enrichment.get("description"):
            table_copy.description = str(enrichment.get("description") or "").strip()
        if enrichment.get("table_type"):
            table_copy.table_type = str(enrichment.get("table_type") or "").strip()
        if "primary_entities" in enrichment:
            table_copy.primary_entities = self._normalize_string_list(
                enrichment.get("primary_entities")
            )
        if enrichment.get("grain"):
            table_copy.grain = type(table_copy.grain).model_validate(enrichment.get("grain") or {})
        if enrichment.get("temporal"):
            table_copy.temporal = type(table_copy.temporal).model_validate(
                enrichment.get("temporal") or {}
            )
        if enrichment.get("counting"):
            table_copy.counting = type(table_copy.counting).model_validate(
                enrichment.get("counting") or {}
            )
        if enrichment.get("answerability"):
            table_copy.answerability = type(table_copy.answerability).model_validate(
                enrichment.get("answerability") or {}
            )

        overrides_by_name = {
            str(item.get("column_name") or item.get("name") or ""): item
            for item in (enrichment.get("column_overrides") or [])
            if item.get("column_name") or item.get("name")
        }
        enriched_columns: list[DashboardChatMetadataColumn] = []
        for column in table_copy.columns:
            override = overrides_by_name.get(column.name)
            if not override:
                enriched_columns.append(column)
                continue
            column_copy = column.model_copy(deep=True)
            if override.get("description"):
                column_copy.description = str(override.get("description") or "").strip()
            if override.get("semantic_role"):
                column_copy.semantic_role = str(override.get("semantic_role") or "").strip()
            if override.get("value_semantics"):
                column_copy.value_semantics = str(override.get("value_semantics") or "").strip()
            if "pii" in override:
                column_copy.pii = bool(override.get("pii"))
            enriched_columns.append(column_copy)
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
            f"{warehouse_tools._quote_postgres_identifier(schema_name)}."
            f"{warehouse_tools._quote_postgres_identifier(bare_table_name)}"
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
