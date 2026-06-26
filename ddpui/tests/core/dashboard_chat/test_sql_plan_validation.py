"""Unit tests for SQL query-plan validation."""

from types import SimpleNamespace

from ddpui.core.dashboard_chat.metadata.schemas import (
    DashboardChatMetadataArtifactPayload,
    DashboardChatMetadataTable,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_plan_validation import (
    handle_set_sql_query_plan_tool,
    validate_sql_against_query_plan,
)


def test_set_sql_query_plan_records_plan_on_turn_context():
    turn_context = SimpleNamespace()

    result = handle_set_sql_query_plan_tool(
        {
            "metric_intent": "math topic mastery growth",
            "entity_grain": "student-topic",
            "comparison_axes": ["math_topic"],
            "stage_scope": "baseline_to_endline",
            "cohort_filter_stage": "endline",
            "required_measure_columns": ["math_perc_operations_base", "math_perc_operations_end"],
            "null_handling": "coalesce_missing_to_zero",
            "disallowed_assumptions": ["attendance filters"],
            "candidate_tables": ["dev_intermediate.scores"],
            "chosen_tables": ["dev_intermediate.scores"],
            "why_chosen_tables_answer_directly": "Contains paired stage metric columns.",
        },
        turn_context,
    )

    assert result["success"] is True
    assert turn_context.sql_query_plan["cohort_filter_stage"] == "endline"


def test_stage_growth_rejects_start_stage_filter_and_unrequested_attendance_filters():
    result = validate_sql_against_query_plan(
        sql=(
            "SELECT AVG(math_perc_time_end) - AVG(math_perc_time_base) "
            "FROM dev_intermediate.base_mid_end_comb_scores_2425_fct s "
            "JOIN dev_intermediate.base_mid_end_comb_students_2425_dim d USING (student_id) "
            "WHERE d.grade_taught_base = 2 "
            "AND d.baseline_attendence = TRUE AND d.endline_attendence = TRUE"
        ),
        state={
            "user_query": "For grade 2, which math topic showed the highest average mastery growth from baseline to endline?",
            "metadata_artifact_payload": {},
        },
        turn_context=SimpleNamespace(
            sql_query_plan={
                "cohort_filter_stage": "endline",
                "null_handling": "coalesce_missing_to_zero",
            }
        ),
        referenced_tables=[
            "dev_intermediate.base_mid_end_comb_scores_2425_fct",
            "dev_intermediate.base_mid_end_comb_students_2425_dim",
        ],
    )

    assert result is not None
    assert result.is_valid is False
    assert result.reason_code == "stage_filter_uses_starting_stage+unrequested_completion_filter"
    assert "Unrequested attendance/completion filters can change the growth cohort." in result.issues


def test_stage_growth_rejects_silent_null_dropping():
    result = validate_sql_against_query_plan(
        sql=(
            "SELECT AVG(math_perc_operations_end) - AVG(math_perc_operations_base) "
            "FROM dev_intermediate.base_mid_end_comb_scores_2425_fct "
            "WHERE math_perc_operations_base IS NOT NULL "
            "AND math_perc_operations_end IS NOT NULL"
        ),
        state={
            "user_query": "Which math topic showed the highest growth from baseline to endline?",
            "metadata_artifact_payload": {},
        },
        turn_context=SimpleNamespace(sql_query_plan={"null_handling": ""}),
        referenced_tables=["dev_intermediate.base_mid_end_comb_scores_2425_fct"],
    )

    assert result is not None
    assert result.reason_code == "growth_drops_missing_stage_values"


def test_stage_growth_rejects_aggregate_distribution_proxy():
    artifact = DashboardChatMetadataArtifactPayload(
        org_id=1,
        dashboard_id=1,
        tables=[
            DashboardChatMetadataTable(
                table_name="dev_prod.overall_RC_analysis_levels_2525",
                table_type="aggregate",
                columns=[
                    {"column_name": "city"},
                    {"column_name": "grade"},
                    {"column_name": "rc_level"},
                    {"column_name": "student_count_base"},
                    {"column_name": "student_count_end"},
                ],
            )
        ],
    )

    result = validate_sql_against_query_plan(
        sql=(
            "SELECT city, grade, rc_level, "
            "SUM(student_count_end) - SUM(student_count_base) AS count_change "
            "FROM dev_prod.overall_RC_analysis_levels_2525 "
            "GROUP BY city, grade, rc_level"
        ),
        state={
            "user_query": "What has been the overall RC trend across grades and cities from baseline to endline?",
            "metadata_artifact_payload": artifact.model_dump(mode="json"),
        },
        turn_context=SimpleNamespace(sql_query_plan={"null_handling": "not_applicable"}),
        referenced_tables=["dev_prod.overall_RC_analysis_levels_2525"],
    )

    assert result is not None
    assert result.reason_code == "aggregate_distribution_proxy_for_stage_growth"
