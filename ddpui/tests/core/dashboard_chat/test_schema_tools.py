"""Tests for schema tool metadata hints."""

from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.schema_tools import (
    _build_table_hint,
    _build_table_profile,
)


def test_build_table_profile_marks_aggregate_like_summary_tables():
    """Aggregate count tables without ids should be marked as aggregate-like."""
    profile = _build_table_profile(
        [
            {"name": "grade"},
            {"name": "rf_status"},
            {"name": "student_count_mid"},
        ]
    )

    assert profile["is_aggregate_like"] is True
    assert profile["has_join_key"] is False
    assert profile["has_entity_name_columns"] is False
    hint = _build_table_hint("dev_prod.overall_RF_analysis_level_2525", profile)
    assert hint is not None
    assert "aggregate-like" in hint


def test_build_table_profile_marks_numeric_score_tables():
    """Row-grain score tables should surface direct score/mastery fields."""
    profile = _build_table_profile(
        [
            {"name": "student_id"},
            {"name": "math_mastery_end"},
            {"name": "rf_perc_mid"},
            {"name": "rc_status_mid"},
        ]
    )

    assert profile["is_aggregate_like"] is False
    assert profile["has_join_key"] is True
    assert profile["has_numeric_score_columns"] is True
    hint = _build_table_hint("dev_intermediate.base_mid_end_comb_scores_2425_fct", profile)
    assert hint is not None
    assert "numeric score/mastery fields" in hint
