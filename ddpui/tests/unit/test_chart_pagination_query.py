"""Regression tests: build_chart_query with pagination enabled.

Guards against the empty-SELECT bug where the column/aggregation/GROUP BY
logic sat inside the `else: # No pagination` branch of build_chart_query.
With pagination enabled the generated SQL was `SELECT FROM (SELECT * ... LIMIT n)`
— no dimension, no metric, no GROUP BY — so every category rendered as the
"Unknown" null label. The bug slipped through twice because nothing crashed:
the query is valid SQL and the API returns 200.

These tests compile the SQL and assert on the text, so they need no database
and catch the regression at the query-generation layer where it lives.
"""

from unittest.mock import MagicMock

from ddpui.schemas.chart_schemas import ChartDataPayload, ChartMetric
from ddpui.core.charts.charts_service import build_chart_query


def _org_warehouse(wtype="postgres"):
    ow = MagicMock()
    ow.wtype = wtype
    return ow


def _compile(payload):
    qb = build_chart_query(payload, _org_warehouse())
    return str(qb.build().compile(compile_kwargs={"literal_binds": True}))


def _paginated_payload(chart_type, page_size=20):
    return ChartDataPayload(
        chart_type=chart_type,
        schema_name="prod_analytics",
        table_name="prod_volunteer_class_child_view",
        dimension_col="partner_name",
        metrics=[
            ChartMetric(column="child_id", aggregation="count_distinct", alias="cnt_children")
        ],
        extra_config={"filters": [], "pagination": {"enabled": True, "page_size": page_size}},
    )


class TestPaginatedChartQueryKeepsColumns:
    """The paginated path must still SELECT the dimension and metric and GROUP BY."""

    def test_bar_chart_with_pagination_selects_dimension_metric_and_groups(self):
        sql = _compile(_paginated_payload("bar"))
        sql_upper = sql.upper()

        assert "partner_name" in sql, f"dimension column missing from SELECT:\n{sql}"
        assert "distinct" in sql.lower(), f"metric aggregate missing from SELECT:\n{sql}"
        assert "GROUP BY" in sql_upper, f"GROUP BY missing:\n{sql}"
        assert "LIMIT 20" in sql_upper, f"pagination LIMIT missing:\n{sql}"
        assert "paginated_data" in sql, f"pagination subquery missing:\n{sql}"

    def test_line_chart_with_pagination_selects_dimension_metric_and_groups(self):
        sql = _compile(_paginated_payload("line"))

        assert "partner_name" in sql
        assert "distinct" in sql.lower()
        assert "GROUP BY" in sql.upper()
        assert "LIMIT 20" in sql.upper()

    def test_pie_chart_with_pagination_selects_dimension_metric_and_groups(self):
        sql = _compile(_paginated_payload("pie"))

        assert "partner_name" in sql
        assert "distinct" in sql.lower()
        assert "GROUP BY" in sql.upper()
        assert "LIMIT 20" in sql.upper()

    def test_pagination_respects_page_size(self):
        sql = _compile(_paginated_payload("bar", page_size=75))
        assert "LIMIT 75" in sql.upper()

    def test_select_list_is_never_empty(self):
        """The literal failure mode: 'SELECT FROM' with nothing between."""
        for chart_type in ("bar", "line", "pie"):
            sql = _compile(_paginated_payload(chart_type))
            select_clause = sql.upper().split("FROM")[0].replace("SELECT", "").strip()
            assert select_clause, f"{chart_type}: empty SELECT list:\n{sql}"


class TestUnpaginatedChartQueryUnchanged:
    """The healthy path must stay healthy: no LIMIT subquery when pagination is off."""

    def test_bar_chart_without_pagination(self):
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="prod_analytics",
            table_name="prod_volunteer_class_child_view",
            dimension_col="partner_name",
            metrics=[
                ChartMetric(column="child_id", aggregation="count_distinct", alias="cnt_children")
            ],
            extra_config={"filters": []},
        )
        sql = _compile(payload)

        assert "partner_name" in sql
        assert "GROUP BY" in sql.upper()
        assert "LIMIT" not in sql.upper()
        assert "paginated_data" not in sql

    def test_pagination_disabled_flag_is_ignored(self):
        payload = _paginated_payload("bar")
        payload.extra_config["pagination"]["enabled"] = False
        sql = _compile(payload)

        assert "LIMIT" not in sql.upper()
        assert "GROUP BY" in sql.upper()


class TestPaginationExemptChartTypes:
    """Table and pivot charts have their own pagination/query paths and must not
    get the generic LIMIT/OFFSET subquery even if a saved config carries
    pagination.enabled (get_pagination_params exempts them)."""

    def test_table_chart_skips_generic_pagination_subquery(self):
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="prod_analytics",
            table_name="prod_volunteer_class_child_view",
            dimensions=["partner_name"],
            metrics=[
                ChartMetric(column="child_id", aggregation="count_distinct", alias="cnt_children")
            ],
            extra_config={"pagination": {"enabled": True, "page_size": 20}},
        )
        sql = _compile(payload)

        assert "paginated_data" not in sql
        assert "partner_name" in sql

    def test_pivot_table_skips_generic_pagination_subquery(self):
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="prod_analytics",
            table_name="prod_volunteer_class_child_view",
            row_dimensions=["partner_name"],
            metrics=[
                ChartMetric(column="child_id", aggregation="count_distinct", alias="cnt_children")
            ],
            extra_config={"pagination": {"enabled": True, "page_size": 20}},
        )
        sql = _compile(payload)

        assert "paginated_data" not in sql
        assert "partner_name" in sql
