"""Unit tests for dashboard chat metadata builder helpers."""

from ddpui.core.dashboard_chat.metadata.builder import DashboardChatMetadataArtifactBuilder
from ddpui.core.dashboard_chat.metadata.schemas import (
    DashboardChatMetadataArtifactPayload,
    DashboardChatMetadataColumn,
    DashboardChatMetadataTable,
    DashboardChatObservedColumn,
)


def test_table_time_coverage_collects_observed_time_ranges_by_column():
    builder = DashboardChatMetadataArtifactBuilder()
    columns = [
        DashboardChatMetadataColumn(
            observed=DashboardChatObservedColumn(
                name="encounter_date",
                data_type="date",
                time_min="2025-04-01",
                time_max="2026-03-31",
            )
        ),
        DashboardChatMetadataColumn(
            observed=DashboardChatObservedColumn(
                name="loaded_at",
                data_type="timestamp",
                time_min="2025-04-02T09:10:11",
                time_max="2026-03-31T23:59:59",
            )
        ),
    ]

    coverage = builder._table_time_coverage(columns)  # noqa: SLF001

    assert coverage == {
        "columns": [
            {
                "column": "encounter_date",
                "min": "2025-04-01",
                "max": "2026-03-31",
            },
            {
                "column": "loaded_at",
                "min": "2025-04-02T09:10:11",
                "max": "2026-03-31T23:59:59",
            },
        ]
    }


def test_rebuild_derived_indexes_uses_inferred_semantics_not_builder_heuristics():
    builder = DashboardChatMetadataArtifactBuilder()
    payload = builder.rebuild_derived_indexes(
        DashboardChatMetadataArtifactPayload(
            dashboard_id=1,
            org_id=1,
            tables=[
                DashboardChatMetadataTable(
                    table_name="analytics.student_scores",
                    observed={
                        "layer": "intermediate",
                        "schema_name": "analytics",
                        "model_name": "student_scores",
                        "statistics": {"distinct_counts": {"student_id": 100}},
                        "total_row_count": 120,
                    },
                    inferred={
                        "primary_entities": ["student"],
                        "candidate_unique_id_columns": ["student_id"],
                    },
                    columns=[
                        {
                            "observed": {"name": "student_id", "data_type": "text"},
                            "inferred": {"semantic_role": "identifier", "measure_tags": []},
                        },
                        {
                            "observed": {"name": "math_mastery", "data_type": "numeric"},
                            "inferred": {"semantic_role": "metric", "measure_tags": ["math", "mastery"]},
                        },
                    ],
                ),
                DashboardChatMetadataTable(
                    table_name="analytics.student_dim",
                    observed={
                        "layer": "intermediate",
                        "schema_name": "analytics",
                        "model_name": "student_dim",
                        "statistics": {"distinct_counts": {"student_id": 100}},
                        "total_row_count": 100,
                    },
                    inferred={
                        "primary_entities": ["student"],
                        "candidate_unique_id_columns": ["student_id"],
                    },
                    columns=[
                        {
                            "observed": {"name": "student_id", "data_type": "text"},
                            "inferred": {"semantic_role": "identifier", "measure_tags": []},
                        },
                        {
                            "observed": {"name": "student_name", "data_type": "text"},
                            "inferred": {"semantic_role": "label", "measure_tags": []},
                        },
                    ],
                ),
            ],
        )
    )

    assert payload.entity_index == {"student": ["analytics.student_dim", "analytics.student_scores"]}
    assert payload.measure_index == {"math": ["analytics.student_scores"], "mastery": ["analytics.student_scores"]}
    assert payload.column_index["student_id"] == [
        "analytics.student_dim",
        "analytics.student_scores",
    ]
    assert payload.join_paths[0].via_columns == ["student_id"]
