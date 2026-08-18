"""Tests for apply_table_search — OR'd case-insensitive search across columns"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from types import SimpleNamespace

import pytest
from ddpui.core.charts.charts_service import apply_table_search
from ddpui.core.datainsights.query_builder import AggQueryBuilder

pytestmark = pytest.mark.django_db


def make_metric(column=None, aggregation=None, alias=None, column_expression=None):
    return SimpleNamespace(
        column=column, aggregation=aggregation, alias=alias, column_expression=column_expression
    )


def get_where_sql(search_term, columns, metrics=None):
    qb = AggQueryBuilder()
    apply_table_search(qb, search_term, columns, metrics)
    return [
        str(clause.compile(compile_kwargs={"literal_binds": True})) for clause in qb.where_clauses
    ]


def get_having_sql(search_term, columns, metrics):
    qb = AggQueryBuilder()
    apply_table_search(qb, search_term, columns, metrics)
    return [
        str(clause.compile(compile_kwargs={"literal_binds": True})) for clause in qb.having_clauses
    ]


class TestApplyTableSearch:
    def test_ors_lowercase_like_across_all_columns(self):
        sql = get_where_sql("John", ["name", "city"])
        assert len(sql) == 1
        assert "lower(CAST(name AS VARCHAR))" in sql[0]
        assert "lower(CAST(city AS VARCHAR))" in sql[0]
        assert "%john%" in sql[0]
        assert " OR " in sql[0]

    def test_casts_non_text_columns_before_lower(self):
        """A boolean/numeric column must be cast to text first — Postgres errors
        with "function lower(boolean) does not exist" on lower(bool_col) directly."""
        sql = get_where_sql("true", ["attended"])
        assert len(sql) == 1
        assert "lower(CAST(attended AS VARCHAR))" in sql[0]

    def test_empty_search_term_is_noop(self):
        assert get_where_sql("", ["name"]) == []
        assert get_where_sql(None, ["name"]) == []

    def test_no_columns_is_noop(self):
        assert get_where_sql("John", []) == []

    def test_with_metrics_uses_having_not_where(self):
        """A metric's value only exists post-aggregation, so WHERE (pre-aggregation)
        can't filter on it — the whole condition must move to HAVING."""
        metrics = [make_metric(column=None, aggregation="count", alias="Total")]
        assert get_where_sql("5", ["name"], metrics) == []
        sql = get_having_sql("5", ["name"], metrics)
        assert len(sql) == 1

    def test_having_ors_dimension_and_metric_conditions(self):
        metrics = [make_metric(column=None, aggregation="count", alias="Total")]
        sql = get_having_sql("5", ["name"], metrics)
        assert "lower(CAST(name AS VARCHAR))" in sql[0]
        assert "lower(CAST(count(*) AS VARCHAR))" in sql[0]
        assert " OR " in sql[0]

    def test_having_searches_sum_metric_on_its_column(self):
        metrics = [make_metric(column="amount", aggregation="sum", alias="Total Amount")]
        sql = get_having_sql("100", [], metrics)
        assert "lower(CAST(sum(amount) AS VARCHAR))" in sql[0]

    def test_having_searches_expression_metric(self):
        metrics = [make_metric(column_expression="sum(a) / sum(b)", alias="ratio")]
        sql = get_having_sql("0.5", [], metrics)
        assert "lower(CAST(sum(a) / sum(b) AS VARCHAR))" in sql[0]
