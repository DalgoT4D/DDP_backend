"""Unit tests for semantic SQL verifier helpers."""

from ddpui.core.dashboard_chat.metadata.schemas import (
    DashboardChatMetadataArtifactPayload,
    DashboardChatMetadataTable,
)
from ddpui.core.dashboard_chat.contracts.sql_contracts import (
    DashboardChatSqlVerificationResult,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_verifier import (
    _referenced_table_metadata,
    build_sql_risk_flags,
    verify_sql_against_question,
)


class FailingVerifierLlmClient:
    """Verifier fake that fails if deterministic checks do not short-circuit."""

    def verify_sql_against_question(self, **_kwargs):
        raise AssertionError("LLM verifier should not be called")


class WarningVerifierLlmClient:
    """Verifier fake that returns a broad warning."""

    def verify_sql_against_question(self, **_kwargs):
        return DashboardChatSqlVerificationResult(
            is_valid=True,
            severity="warning",
            reason_code="missing_breakdown",
            reasoning="The SQL only returns the overall roll-up and does not return per-city/per-grade trend.",
            issues=[],
            repair_instructions=[],
            warnings=[],
        )


class LimitRejectionVerifierLlmClient:
    """Verifier fake that objects only to a result limit."""

    def verify_sql_against_question(self, **_kwargs):
        return DashboardChatSqlVerificationResult(
            is_valid=False,
            severity="repair_once",
            reason_code="results_truncated_by_limit",
            reasoning="Uses LIMIT 200, potentially excluding qualifying students from the returned list.",
            issues=["Uses LIMIT 200, potentially excluding qualifying students."],
            repair_instructions=["Remove the LIMIT."],
            warnings=[],
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


def test_verify_sql_hard_blocks_latest_row_without_user_request():
    result = verify_sql_against_question(
        FailingVerifierLlmClient(),
        sql=(
            "WITH latest_per_work_order AS ("
            "SELECT *, ROW_NUMBER() OVER (PARTITION BY work_order_name ORDER BY date_time DESC) AS rn "
            "FROM analytics.work_orders)"
            "SELECT SUM(silt_achieved) FROM latest_per_work_order WHERE rn = 1"
        ),
        state={
            "user_query": "How much total silt was excavated this year?",
            "metadata_artifact_payload": {},
        },
    )

    assert result.is_valid is False
    assert result.severity == "hard_block"
    assert result.reason_code == "latest_logic_without_user_request"


def test_verify_sql_hard_blocks_latest_request_without_relative_date_filter():
    result = verify_sql_against_question(
        FailingVerifierLlmClient(),
        sql=(
            "WITH latest_per_work_order AS ("
            "SELECT *, ROW_NUMBER() OVER (PARTITION BY work_order_name ORDER BY date_time DESC) AS rn "
            "FROM analytics.work_orders)"
            "SELECT SUM(silt_achieved) FROM latest_per_work_order WHERE rn = 1"
        ),
        state={
            "user_query": "What is the latest total silt excavated this year?",
            "metadata_artifact_payload": {},
        },
    )

    assert result.is_valid is False
    assert result.severity == "hard_block"
    assert result.reason_code == "latest_logic_without_relative_time_filter"


def test_verify_sql_keeps_broad_warnings_non_blocking():
    result = verify_sql_against_question(
        WarningVerifierLlmClient(),
        sql="SELECT SUM(students) FROM dev_prod.overall_rc_analysis",
        state={
            "user_query": "Show RC trend across grades and cities",
            "metadata_artifact_payload": {},
            "intent_decision": {
                "intent": "query_with_sql",
                "confidence": 0.9,
                "reason": "Needs SQL",
                "force_tool_usage": True,
            },
        },
    )

    assert result.is_valid is True
    assert result.severity == "warning"
    assert result.reason_code == "missing_breakdown"


def test_verify_sql_allows_limit_rejections_unless_user_requests_exhaustive_output():
    result = verify_sql_against_question(
        LimitRejectionVerifierLlmClient(),
        sql=(
            "SELECT student_name_end FROM dev_staging.endline_2425_stg "
            "WHERE grade_taught_end = 3 AND math_mastery_end < 20 LIMIT 200"
        ),
        state={
            "user_query": "Give me the names of students below 20 percent in endline maths",
            "metadata_artifact_payload": {},
            "intent_decision": {
                "intent": "query_with_sql",
                "confidence": 0.9,
                "reason": "Needs SQL",
                "force_tool_usage": True,
            },
        },
    )

    assert result.is_valid is True
    assert result.severity == "warning"
    assert result.reason_code == "results_truncated_by_limit"


def test_verify_sql_keeps_limit_rejection_when_user_requests_exhaustive_output():
    result = verify_sql_against_question(
        LimitRejectionVerifierLlmClient(),
        sql=(
            "SELECT student_name_end FROM dev_staging.endline_2425_stg "
            "WHERE grade_taught_end = 3 AND math_mastery_end < 20 LIMIT 200"
        ),
        state={
            "user_query": "Give me the complete full list of all students below 20 percent",
            "metadata_artifact_payload": {},
            "intent_decision": {
                "intent": "query_with_sql",
                "confidence": 0.9,
                "reason": "Needs SQL",
                "force_tool_usage": True,
            },
        },
    )

    assert result.is_valid is False
    assert result.severity == "repair_once"
    assert result.reason_code == "results_truncated_by_limit"


def test_build_sql_risk_flags_detects_name_aggregation_for_name_lists():
    flags = build_sql_risk_flags(
        sql="SELECT ARRAY_AGG(student_name_end) FROM analytics.student_scores",
        user_query="Give me the names of students below 20 percent in endline maths",
        tables=[{"table_type": "row_grain"}],
    )

    assert "aggregates_names_instead_of_returning_one_row_per_name" in flags


def test_referenced_table_metadata_matches_mixed_case_table_names_case_insensitively():
    artifact = DashboardChatMetadataArtifactPayload(
        schema_version=5,
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

    assert [item["table_name"] for item in metadata] == ["dev_prod.overall_RC_analysis_levels_2525"]
