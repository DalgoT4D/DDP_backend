"""Unit tests for the Batch 1 metrics_service — pure-Python helpers (no warehouse)."""

import os
import django

import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.metrics_service import (
    MetricCompileError,
    compile_simple_expression,
    compute_period_over_period,
    compute_rag_status,
    validate_sql_expression,
)


# ── compile_simple_expression ────────────────────────────────────────────────


class TestCompileSimpleExpression:
    def test_single_term_no_formula_is_identity(self):
        terms = [{"id": "t1", "agg": "sum", "column": "revenue"}]
        assert compile_simple_expression(terms, "") == 'SUM("revenue")'

    def test_single_term_explicit_formula(self):
        terms = [{"id": "t1", "agg": "avg", "column": "score"}]
        assert compile_simple_expression(terms, "t1") == 'AVG("score")'

    def test_two_term_difference(self):
        terms = [
            {"id": "t1", "agg": "avg", "column": "col_a"},
            {"id": "t2", "agg": "avg", "column": "col_b"},
        ]
        assert compile_simple_expression(terms, "t1 - t2") == 'AVG("col_a") - AVG("col_b")'

    def test_ratio_with_parentheses_and_literal(self):
        terms = [
            {"id": "t1", "agg": "sum", "column": "numerator"},
            {"id": "t2", "agg": "sum", "column": "denominator"},
        ]
        assert (
            compile_simple_expression(terms, "(t1 / t2) * 100")
            == '(SUM("numerator") / SUM("denominator")) * 100'
        )

    def test_count_distinct_wraps_distinct(self):
        terms = [{"id": "t1", "agg": "count_distinct", "column": "user_id"}]
        assert compile_simple_expression(terms, "t1") == 'COUNT(DISTINCT "user_id")'

    def test_t10_not_shadowed_by_t1(self):
        """Substitution must be token-aware: 't10' should not start with 't1' expansion."""
        terms = [
            {"id": "t1", "agg": "sum", "column": "a"},
            {"id": "t10", "agg": "sum", "column": "b"},
        ]
        assert compile_simple_expression(terms, "t1 + t10") == 'SUM("a") + SUM("b")'

    def test_missing_term_raises(self):
        terms = [{"id": "t1", "agg": "sum", "column": "a"}]
        with pytest.raises(MetricCompileError, match="undefined terms"):
            compile_simple_expression(terms, "t1 + t99")

    def test_illegal_char_in_formula_raises(self):
        terms = [{"id": "t1", "agg": "sum", "column": "a"}]
        with pytest.raises(MetricCompileError):
            compile_simple_expression(terms, "t1; DROP TABLE x")

    def test_invalid_column_identifier_rejected(self):
        terms = [{"id": "t1", "agg": "sum", "column": 'a"; DROP'}]
        with pytest.raises(MetricCompileError, match="Invalid column"):
            compile_simple_expression(terms, "t1")

    def test_unknown_aggregation_rejected(self):
        terms = [{"id": "t1", "agg": "median", "column": "a"}]
        with pytest.raises(MetricCompileError, match="Unknown aggregation"):
            compile_simple_expression(terms, "t1")

    def test_empty_terms_rejected(self):
        with pytest.raises(MetricCompileError, match="at least one term"):
            compile_simple_expression([], "")

    def test_multi_term_requires_formula(self):
        terms = [
            {"id": "t1", "agg": "sum", "column": "a"},
            {"id": "t2", "agg": "sum", "column": "b"},
        ]
        with pytest.raises(MetricCompileError, match="requires a formula"):
            compile_simple_expression(terms, "")


# ── validate_sql_expression ──────────────────────────────────────────────────


class TestValidateSqlExpression:
    def test_scalar_expression_passes(self):
        validate_sql_expression("SUM(CASE WHEN status='active' THEN 1 END)")

    def test_rejects_semicolon(self):
        with pytest.raises(MetricCompileError, match="';'"):
            validate_sql_expression("SUM(col); DROP TABLE users")

    def test_rejects_comment_dash(self):
        with pytest.raises(MetricCompileError, match="comments"):
            validate_sql_expression("SUM(col) -- trailing comment")

    def test_rejects_block_comment(self):
        with pytest.raises(MetricCompileError, match="comments"):
            validate_sql_expression("SUM(col) /* evil */")

    @pytest.mark.parametrize(
        "expr",
        [
            "INSERT INTO x VALUES 1",
            "UPDATE x SET a=1",
            "DELETE FROM x",
            "DROP TABLE x",
            "TRUNCATE x",
            "ALTER TABLE x ADD COLUMN y INT",
            "CREATE TABLE x()",
            "MERGE INTO x USING y",
            "GRANT SELECT ON x TO y",
        ],
    )
    def test_rejects_dml_ddl_keywords(self, expr):
        with pytest.raises(MetricCompileError, match="forbidden keyword"):
            validate_sql_expression(expr)

    def test_empty_rejected(self):
        with pytest.raises(MetricCompileError, match="empty"):
            validate_sql_expression("")


# ── compute_rag_status ───────────────────────────────────────────────────────


class TestComputeRagStatus:
    def test_no_target_returns_grey(self):
        assert compute_rag_status(100, None, 80, 100, "increase") == ("grey", None)

    def test_zero_target_returns_grey(self):
        assert compute_rag_status(100, 0, 80, 100, "increase") == ("grey", None)

    def test_null_current_returns_grey(self):
        assert compute_rag_status(None, 100, 80, 100, "increase") == ("grey", None)

    def test_increase_green_at_target(self):
        status, ach = compute_rag_status(100, 100, 80, 100, "increase")
        assert status == "green"
        assert ach == 100.0

    def test_increase_amber_between(self):
        status, ach = compute_rag_status(90, 100, 80, 100, "increase")
        assert status == "amber"
        assert ach == 90.0

    def test_increase_red_below_amber(self):
        status, _ = compute_rag_status(50, 100, 80, 100, "increase")
        assert status == "red"

    def test_decrease_green_at_target(self):
        """For decrease metrics, 'at or below target' is green."""
        status, ach = compute_rag_status(80, 100, 120, 100, "decrease")
        assert status == "green"
        assert ach == 80.0

    def test_decrease_amber_between(self):
        status, _ = compute_rag_status(110, 100, 120, 100, "decrease")
        assert status == "amber"

    def test_decrease_red_above(self):
        status, _ = compute_rag_status(150, 100, 120, 100, "decrease")
        assert status == "red"


# ── compute_period_over_period ───────────────────────────────────────────────


class TestComputePeriodOverPeriod:
    def test_increase(self):
        trend = [
            {"period": "2026-01", "value": 100.0},
            {"period": "2026-02", "value": 130.0},
        ]
        delta, pct = compute_period_over_period(trend)
        assert delta == 30.0
        assert pct == 30.0

    def test_decrease(self):
        trend = [
            {"period": "2026-01", "value": 200.0},
            {"period": "2026-02", "value": 150.0},
        ]
        delta, pct = compute_period_over_period(trend)
        assert delta == -50.0
        assert pct == -25.0

    def test_zero_previous_returns_none_pct(self):
        trend = [
            {"period": "2026-01", "value": 0.0},
            {"period": "2026-02", "value": 50.0},
        ]
        delta, pct = compute_period_over_period(trend)
        assert delta == 50.0
        assert pct is None

    def test_too_few_points(self):
        assert compute_period_over_period([]) == (None, None)
        assert compute_period_over_period([{"period": "2026-02", "value": 10}]) == (
            None,
            None,
        )

    def test_nulls_skipped(self):
        trend = [
            {"period": "2026-01", "value": None},
            {"period": "2026-02", "value": 100.0},
            {"period": "2026-03", "value": 120.0},
        ]
        delta, pct = compute_period_over_period(trend)
        assert delta == 20.0
        assert pct == 20.0
