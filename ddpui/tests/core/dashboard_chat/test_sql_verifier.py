"""Unit tests for semantic SQL verifier helpers."""

from ddpui.core.dashboard_chat.metadata.schemas import (
    DashboardChatMetadataArtifactPayload,
    DashboardChatMetadataTable,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_verifier import (
    _referenced_table_metadata,
    build_sql_risk_flags,
)


def test_build_sql_risk_flags_detects_latest_row_without_latest_request():
    flags = build_sql_risk_flags(
        sql=(
            "SELECT * FROM analytics.work_orders "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY work_order_name ORDER BY date_time DESC) = 1"
        ),
        user_query="How much total silt was excavated this year?",
        tables=[{"table_type": "row_grain"}],
    )

    assert "uses_latest_row_logic_without_explicit_latest_request" in flags


def test_build_sql_risk_flags_detects_name_aggregation_for_name_lists():
    flags = build_sql_risk_flags(
        sql="SELECT ARRAY_AGG(student_name_end) FROM analytics.student_scores",
        user_query="Give me the names of students below 20 percent in endline maths",
        tables=[{"table_type": "row_grain"}],
    )

    assert "aggregates_names_instead_of_returning_one_row_per_name" in flags


def test_referenced_table_metadata_matches_mixed_case_table_names_case_insensitively():
    artifact = DashboardChatMetadataArtifactPayload(
        schema_version=4,
        org_id=1,
        dashboard_id=4,
        dashboard_title="Bhumi",
        dashboard_description="",
        built_at="2026-05-29T00:00:00Z",
        source_fingerprint="test",
        allowlisted_tables=["dev_prod.overall_RC_analysis_levels_2525"],
        chart_table_map={},
        tables=[
            DashboardChatMetadataTable(
                table_name="dev_prod.overall_RC_analysis_levels_2525",
                layer="prod",
                schema_name="dev_prod",
                model_name="overall_RC_analysis_levels_2525",
                table_type="aggregate",
                description="RC level distribution by city and grade.",
                statistics={"row_count": 10},
                grain={"row_definition": "one row per city, grade, rc_level"},
                columns=[
                    {
                        "column_name": "city",
                        "semantic_role": "dimension",
                    }
                ],
            )
        ],
        join_paths=[],
    )

    metadata = _referenced_table_metadata(
        "SELECT city FROM dev_prod.overall_RC_analysis_levels_2525",
        artifact,
    )

    assert [item["table_name"] for item in metadata] == [
        "dev_prod.overall_RC_analysis_levels_2525"
    ]
