"""Unit tests for dashboard chat metadata enricher prompt guards."""

from datetime import date, datetime
from decimal import Decimal
import json

from ddpui.core.dashboard_chat.metadata.enricher import (
    METADATA_ENRICHMENT_SYSTEM_PROMPT,
    DashboardChatMetadataEnricher,
)
from ddpui.core.dashboard_chat.metadata.schemas import (
    DashboardChatMetadataColumn,
    DashboardChatMetadataTable,
)


def test_metadata_enrichment_prompt_forbids_invented_latest_semantics():
    prompt = METADATA_ENRICHMENT_SYSTEM_PROMPT.lower()

    assert "never assume cumulative semantics" in prompt
    assert "latest-row semantics" in prompt
    assert "as-of-date semantics" in prompt
    assert "observed physical facts are the source of truth" in prompt
    assert "leave fields empty" in prompt
    assert "snapshot" not in prompt


def test_enricher_writes_flat_semantics_and_descriptions():
    enricher = DashboardChatMetadataEnricher(client=None)
    table = DashboardChatMetadataTable(
        table_name="analytics.work_orders",
        schema_name="analytics",
        model_name="work_orders",
        description="raw dbt doc",
        columns=[
            DashboardChatMetadataColumn(
                column_name="work_order_id",
                data_type="text",
            )
        ],
    )

    enriched = enricher._apply_table_enrichment(  # noqa: SLF001
        table,
        {
            "description": "Aggregated work order records.",
            "table_type": "row_grain",
            "primary_entities": ["work_order"],
            "grain": {
                "row_definition": "One row per work order",
                "natural_keys": ["work_order_name"],
                "candidate_unique_id_columns": ["work_order_id"],
                "evidence": ["work_order_id has near-unique distinct counts"],
            },
            "temporal": {"primary_filter_time_column": "date_time"},
            "counting": {
                "entity_counting_guidance": {
                    "work_order": "COUNT(DISTINCT work_order_id)",
                }
            },
            "column_overrides": [
                {
                    "column_name": "work_order_id",
                    "description": "Stable work order identifier.",
                    "semantic_role": "identifier",
                    "value_semantics": "entity_identifier",
                    "pii": False,
                }
            ],
        },
    )

    assert enriched.model_name == "work_orders"
    assert enriched.description == "Aggregated work order records."
    assert enriched.table_type == "row_grain"
    assert enriched.grain.row_definition == "One row per work order"
    assert enriched.grain.natural_keys == ["work_order_name"]
    assert enriched.grain.candidate_unique_id_columns == ["work_order_id"]
    assert enriched.temporal.primary_filter_time_column == "date_time"
    assert enriched.columns[0].description == "Stable work order identifier."
    assert enriched.columns[0].semantic_role == "identifier"
    assert enriched.columns[0].value_semantics == "entity_identifier"


def test_enricher_prompt_payload_serializer_handles_dates_and_decimals():
    enricher = DashboardChatMetadataEnricher(client=None)

    dumped = enricher._dump_prompt_json(  # noqa: SLF001
        {
            "date_value": date(2026, 5, 25),
            "datetime_value": datetime(2026, 5, 25, 9, 30, 0),
            "decimal_value": Decimal("12.5"),
        }
    )

    parsed = json.loads(dumped)

    assert parsed["date_value"] == "2026-05-25"
    assert parsed["datetime_value"] == "2026-05-25T09:30:00"
    assert parsed["decimal_value"] == 12.5
