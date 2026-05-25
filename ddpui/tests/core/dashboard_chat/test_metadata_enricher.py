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


def test_metadata_enrichment_prompt_forbids_invented_snapshot_semantics():
    prompt = METADATA_ENRICHMENT_SYSTEM_PROMPT.lower()

    assert "never assume snapshot semantics" in prompt
    assert "infer semantics from those facts" in prompt
    assert "if the evidence is weak, leave fields empty" in prompt


def test_enricher_writes_inferred_semantics_without_touching_observed_facts():
    enricher = DashboardChatMetadataEnricher(client=None)
    table = DashboardChatMetadataTable(
        table_name="analytics.work_orders",
        observed={
            "schema_name": "analytics",
            "model_name": "work_orders",
            "table_description": "Work-order metrics table",
        },
        columns=[
            DashboardChatMetadataColumn(
                observed={"name": "work_order_id", "data_type": "text"},
            )
        ],
    )

    enriched = enricher._apply_table_enrichment(  # noqa: SLF001
        table,
        {
            "row_grain": "One row per work order",
            "natural_keys": ["work_order_name"],
            "candidate_unique_id_columns": ["work_order_id"],
            "primary_time_columns": ["date_time"],
            "preferred_use_cases": ["Program monitoring"],
            "evidence": ["work_order_id has near-unique distinct counts"],
            "column_overrides": [
                {
                    "name": "work_order_id",
                    "semantic_role": "identifier",
                    "entity_tags": ["work_order"],
                    "confidence": 0.9,
                }
            ],
        },
    )

    assert enriched.observed.model_name == "work_orders"
    assert enriched.row_grain == "One row per work order"
    assert enriched.natural_keys == ["work_order_name"]
    assert enriched.candidate_unique_id_columns == ["work_order_id"]
    assert enriched.primary_time_columns == ["date_time"]
    assert "Program monitoring" in enriched.preferred_use_cases
    assert enriched.inferred.evidence == ["work_order_id has near-unique distinct counts"]
    assert enriched.columns[0].semantic_role == "identifier"
    assert enriched.columns[0].entity_tags == ["work_order"]


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
