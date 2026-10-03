"""Tests for apply_table_search — OR'd case-insensitive search across columns"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from ddpui.core.charts.charts_service import apply_table_search, build_chart_query
from ddpui.core.datainsights.query_builder import AggQueryBuilder
from ddpui.schemas.chart_schemas.data import ChartDataPayload
from ddpui.schemas.chart_schemas.config import ChartMetric
from ddpui.models.org import OrgWarehouse

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


class TestSearchAndSortCombined:
    """End-to-end: search and sort applied together via build_chart_query, to guard
    against the two features interfering (e.g. clause ordering, HAVING vs WHERE)."""

    def _compiled_sql(self, payload):
        mock_warehouse = MagicMock(spec=OrgWarehouse)
        mock_warehouse.wtype = "postgres"
        query_builder = build_chart_query(payload, mock_warehouse)
        return str(query_builder.build().compile(compile_kwargs={"literal_binds": True}))

    def test_search_and_sort_on_dimension_only_table(self):
        """No metrics: search is a WHERE, sort is an ORDER BY, both present together."""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["name", "city"],
            extra_config={
                "search": "john",
                "sort": [{"column": "name", "direction": "desc"}],
            },
        )

        sql = self._compiled_sql(payload)

        assert "WHERE" in sql.upper()
        assert "%john%" in sql
        assert "ORDER BY" in sql.upper()
        assert "NAME DESC" in sql.upper()
        # WHERE must precede ORDER BY — search filters before sort orders the result.
        assert sql.upper().index("WHERE") < sql.upper().index("ORDER BY")

    def test_search_and_sort_with_metrics_uses_having_before_order_by(self):
        """With metrics: search moves to HAVING (post-aggregation), sort still ORDER BY,
        and HAVING must still precede ORDER BY in the compiled SQL."""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["name"],
            metrics=[ChartMetric(aggregation="sum", column="amount", alias="Total Amount")],
            extra_config={
                "search": "100",
                "sort": [{"column": "Total Amount", "direction": "asc"}],
            },
        )

        sql = self._compiled_sql(payload)

        assert "HAVING" in sql.upper()
        assert "%100%" in sql
        assert "ORDER BY" in sql.upper()
        assert sql.upper().index("HAVING") < sql.upper().index("ORDER BY")

    def test_search_sort_and_pagination_all_combine_on_full_dataset(self):
        """Pagination (LIMIT/OFFSET) is applied by the caller (get_chart_data_table_preview)
        on top of the already search-filtered, sorted query — not before. This guards the
        actual DALGO-1601 fix: search/sort must see the full table, not a pre-sliced page."""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["name"],
            extra_config={
                "search": "john",
                "sort": [{"column": "name", "direction": "asc"}],
            },
        )
        mock_warehouse = MagicMock(spec=OrgWarehouse)
        mock_warehouse.wtype = "postgres"
        query_builder = build_chart_query(payload, mock_warehouse)

        # Simulate what get_chart_data_table_preview does after build_chart_query returns.
        query_builder.limit_records = 20
        query_builder.offset_records = 40

        sql = str(query_builder.build().compile(compile_kwargs={"literal_binds": True}))

        assert "%john%" in sql
        assert "ORDER BY" in sql.upper()
        assert "LIMIT" in sql.upper() or "OFFSET" in sql.upper()
        # LIMIT/OFFSET must be the outermost/last clause — after WHERE and ORDER BY.
        where_idx = sql.upper().index("WHERE")
        order_idx = sql.upper().index("ORDER BY")
        limit_idx = (
            sql.upper().index("LIMIT") if "LIMIT" in sql.upper() else sql.upper().index("OFFSET")
        )
        assert where_idx < order_idx < limit_idx


class TestSearchWithTimeGrainDimension:
    """Regression coverage for the primary time-grain dimension's search behavior.

    When metrics + time_grain + a warehouse are all present, the primary dimension is
    grouped by a derived date_trunc(...) expression (see build_multi_metric_query's
    is_time_grain_applicable), not its raw column — so search must reuse that exact
    same expression instead of the raw column name, or Postgres rejects it with
    "must appear in GROUP BY". Without a warehouse, GROUP BY still uses the raw
    column, so the raw primary dimension must remain searchable as-is.
    """

    def _payload(self):
        return ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["created_at", "region"],
            dimension_col="created_at",
            metrics=[ChartMetric(aggregation="sum", column="amount", alias="Total Amount")],
            extra_config={
                "search": "march",
                "time_grain": "month",
            },
        )

    def test_with_warehouse_searches_time_grain_expression_not_raw_column(self):
        """org_warehouse present: the primary dimension is searched via the same
        date_trunc(...) expression used in GROUP BY, not the raw column name."""
        mock_warehouse = MagicMock(spec=OrgWarehouse)
        mock_warehouse.wtype = "postgres"

        query_builder = build_chart_query(self._payload(), mock_warehouse)
        sql = str(query_builder.build().compile(compile_kwargs={"literal_binds": True}))

        assert "HAVING" in sql.upper()
        assert "%march%" in sql
        # The search condition reuses the derived GROUP BY expression...
        assert "date_trunc" in sql.lower()
        assert "lower(CAST(date_trunc('month', created_at) AS VARCHAR))" in sql
        # ...and does NOT also search the raw column directly — that would be a
        # dangling reference outside GROUP BY (created_at is only grouped via
        # DATE_TRUNC here, never raw too), catching a regression that forgets to
        # exclude the primary dimension from search_dimensions.
        assert "lower(CAST(created_at AS VARCHAR))" not in sql
        # region — the other dimension — is still searched via its raw column.
        assert "lower(CAST(region AS VARCHAR))" in sql

    def test_without_warehouse_searches_raw_primary_column(self):
        """org_warehouse absent: GROUP BY uses the raw column (no time-grain transform
        is actually applied), so the raw primary dimension stays in the search columns
        instead of being needlessly excluded."""
        query_builder = build_chart_query(self._payload(), org_warehouse=None)
        sql = str(query_builder.build().compile(compile_kwargs={"literal_binds": True}))

        assert "HAVING" in sql.upper()
        assert "%march%" in sql
        assert "date_trunc" not in sql.lower()
        assert "lower(CAST(created_at AS VARCHAR))" in sql
        assert "lower(CAST(region AS VARCHAR))" in sql

    def test_dimension_col_absent_from_dimensions_is_never_treated_as_time_grained(self):
        """Regression: dimension_col must actually be a member of dimensions to be
        treated as the time-grained primary dimension. If it isn't (a stale/mismatched
        payload), build_multi_metric_query's own GROUP BY loop never matches it either
        — nothing gets time-grain-transformed — so search must stay on raw columns too,
        not build a HAVING expression referencing a column that was never grouped."""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["region"],
            dimension_col="created_at",  # not in dimensions
            metrics=[ChartMetric(aggregation="sum", column="amount", alias="Total Amount")],
            extra_config={
                "search": "march",
                "time_grain": "month",
            },
        )
        mock_warehouse = MagicMock(spec=OrgWarehouse)
        mock_warehouse.wtype = "postgres"

        query_builder = build_chart_query(payload, mock_warehouse)
        sql = str(query_builder.build().compile(compile_kwargs={"literal_binds": True}))

        assert "HAVING" in sql.upper()
        assert "%march%" in sql
        assert "date_trunc" not in sql.lower()
        assert "created_at" not in sql
        assert "lower(CAST(region AS VARCHAR))" in sql
