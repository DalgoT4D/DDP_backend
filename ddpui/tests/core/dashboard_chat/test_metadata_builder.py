"""Unit tests for dashboard chat metadata builder helpers."""

from ddpui.core.dashboard_chat.metadata.builder import DashboardChatMetadataArtifactBuilder
from ddpui.core.dashboard_chat.metadata.schemas import (
    DashboardChatMetadataArtifactPayload,
    DashboardChatMetadataColumn,
    DashboardChatMetadataTable,
)


def test_legacy_column_range_fields_flatten_into_range_profile():
    column = DashboardChatMetadataColumn.model_validate(
        {
            "observed": {
                "name": "encounter_date",
                "data_type": "date",
                "nullable": False,
                "time_min": "2025-04-01",
                "time_max": "2026-03-31",
            }
        }
    )

    assert column.column_name == "encounter_date"
    assert column.data_type == "date"
    assert column.statistics.range_profile is not None
    assert column.statistics.range_profile.time_min == "2025-04-01"
    assert column.statistics.range_profile.time_max == "2026-03-31"


def test_rebuild_derived_indexes_uses_candidate_unique_id_columns_for_join_paths():
    builder = DashboardChatMetadataArtifactBuilder()
    payload = builder.rebuild_derived_indexes(
        DashboardChatMetadataArtifactPayload(
            dashboard_id=1,
            org_id=1,
            tables=[
                DashboardChatMetadataTable(
                    table_name="analytics.student_scores",
                    layer="intermediate",
                    model_name="student_scores",
                    statistics={
                        "row_count": 120,
                        "column_count": 2,
                        "distinct_counts": {"student_id": 100},
                    },
                    primary_entities=["student"],
                    grain={"candidate_unique_id_columns": ["student_id"]},
                    columns=[
                        {
                            "column_name": "student_id",
                            "data_type": "text",
                            "semantic_role": "identifier",
                        },
                        {
                            "column_name": "math_mastery",
                            "data_type": "numeric",
                            "semantic_role": "metric",
                        },
                    ],
                ),
                DashboardChatMetadataTable(
                    table_name="analytics.student_dim",
                    layer="intermediate",
                    model_name="student_dim",
                    statistics={
                        "row_count": 100,
                        "column_count": 2,
                        "distinct_counts": {"student_id": 100},
                    },
                    primary_entities=["student"],
                    grain={"candidate_unique_id_columns": ["student_id"]},
                    columns=[
                        {
                            "column_name": "student_id",
                            "data_type": "text",
                            "semantic_role": "identifier",
                        },
                        {
                            "column_name": "student_name",
                            "data_type": "text",
                            "semantic_role": "label",
                        },
                    ],
                ),
            ],
        )
    )

    assert payload.join_paths[0].via_columns == ["student_id"]
    assert payload.join_paths[0].cardinality == "many_to_one"
