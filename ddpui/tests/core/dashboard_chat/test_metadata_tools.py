"""Unit tests for dashboard chat metadata exploration helpers."""

from ddpui.core.dashboard_chat.metadata.schemas import (
    DashboardChatMetadataArtifactPayload,
    DashboardChatMetadataJoinPath,
    DashboardChatMetadataTable,
)
from ddpui.core.dashboard_chat.metadata.search import (
    search_columns_by_name,
    search_metadata_tables,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.metadata_tools import (
    _resolve_time_scope,
)


def _artifact() -> DashboardChatMetadataArtifactPayload:
    return DashboardChatMetadataArtifactPayload(
        dashboard_id=1,
        org_id=1,
        tables=[
            DashboardChatMetadataTable(
                table_name="dev_intermediate.student_scores",
                layer="intermediate",
                model_name="student_scores",
                description="Student assessment scores across program stages.",
                table_type="fact",
                primary_entities=["student"],
                grain={
                    "row_definition": "One row per student assessment",
                    "candidate_unique_id_columns": ["student_id"],
                },
                columns=[
                    {
                        "column_name": "student_id",
                        "data_type": "integer",
                        "description": "Student identifier.",
                        "semantic_role": "identifier",
                        "value_semantics": "entity_identifier",
                    },
                    {
                        "column_name": "math_mastery_end",
                        "data_type": "numeric",
                        "description": "Endline maths mastery percentage.",
                        "semantic_role": "metric",
                        "value_semantics": "percentage",
                    },
                ],
            ),
            DashboardChatMetadataTable(
                table_name="dev_intermediate.student_dim",
                layer="intermediate",
                model_name="student_dim",
                description="Student roster and names.",
                table_type="dim",
                primary_entities=["student"],
                grain={
                    "row_definition": "One row per student",
                    "candidate_unique_id_columns": ["student_id"],
                },
                columns=[
                    {
                        "column_name": "student_id",
                        "data_type": "integer",
                        "description": "Student identifier.",
                        "semantic_role": "identifier",
                        "value_semantics": "entity_identifier",
                    },
                    {
                        "column_name": "student_name_end",
                        "data_type": "text",
                        "description": "Student full name.",
                        "semantic_role": "label",
                        "value_semantics": "entity_label",
                    },
                ],
            ),
        ],
        join_paths=[
            DashboardChatMetadataJoinPath(
                source_table="dev_intermediate.student_scores",
                target_table="dev_intermediate.student_dim",
                via_columns=["student_id"],
                cardinality="many_to_one",
                preferred=True,
            )
        ],
    )


def test_search_columns_by_name_returns_all_matching_tables():
    artifact = _artifact()

    matches = search_columns_by_name(artifact, column_name="student_id")

    assert len(matches) == 2
    assert {match["table_name"] for match in matches} == {
        "dev_intermediate.student_scores",
        "dev_intermediate.student_dim",
    }


def test_search_metadata_tables_prefers_fact_table_for_metric_question():
    artifact = _artifact()

    matches = search_metadata_tables(
        artifact,
        entity_terms=["student"],
        measure_terms=["math", "mastery"],
        question_type="ranking",
    )

    assert matches
    assert matches[0]["table_name"] == "dev_intermediate.student_scores"


def test_resolve_time_scope_maps_april_fiscal_quarters():
    resolved = _resolve_time_scope(
        "Compare Q3-Q4 25-26 results",
        current_date=__import__("datetime").date(2026, 5, 18),
    )

    assert resolved[0]["label"] == "Q3"
    assert resolved[0]["start_date"] == "2025-10-01"
    assert resolved[0]["end_date_exclusive"] == "2026-01-01"
    assert resolved[1]["label"] == "Q4"
    assert resolved[1]["start_date"] == "2026-01-01"
    assert resolved[1]["end_date_exclusive"] == "2026-04-01"
