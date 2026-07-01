import pytest
from unittest.mock import MagicMock
from ddpui.schemas.chart_schemas import ChartDataPayload, ChartMetric
from ddpui.core.charts.charts_service import build_chart_query


class TestBuildPivotQuery:
    def _make_org_warehouse(self, wtype="postgres"):
        ow = MagicMock()
        ow.wtype = wtype
        return ow

    def test_pivot_query_has_rollup(self):
        """Pivot table query should contain GROUP BY ROLLUP"""
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="beneficiaries",
            row_dimensions=["district", "program"],
            column_dimensions=["enrollment_date"],
            show_row_subtotals=True,
            show_grand_total=True,
            metrics=[
                ChartMetric(column="id", aggregation="count", alias="Beneficiaries"),
                ChartMetric(column="amount", aggregation="sum", alias="Total Spend"),
            ],
        )
        ow = self._make_org_warehouse()
        qb = build_chart_query(payload, ow)
        stmt = qb.build()
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        sql_upper = compiled.upper()

        assert "ROLLUP" in sql_upper
        assert "GROUPING" in sql_upper
        assert "_grp_district" in compiled
        assert "_grp_program" in compiled
        assert "_grp_pivot_col_0" in compiled

    def test_pivot_query_no_row_grain_no_trunc(self):
        """Without row grains the SQL must not truncate row dimensions (default path)."""
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="beneficiaries",
            row_dimensions=["district"],
            column_dimensions=[],
            show_row_subtotals=True,
            show_grand_total=True,
            metrics=[ChartMetric(column="id", aggregation="count", alias="Count")],
        )
        ow = self._make_org_warehouse()
        qb = build_chart_query(payload, ow)
        compiled = str(qb.build().compile(compile_kwargs={"literal_binds": True}))
        assert "date_trunc" not in compiled.lower()

    def _rollup_payload(self, **overrides):
        base = dict(
            chart_type="pivot_table",
            schema_name="public",
            table_name="beneficiaries",
            row_dimensions=["district"],
            column_dimensions=[],  # no col dims → any ROLLUP in SQL is the row rollup
            metrics=[ChartMetric(column="id", aggregation="count", alias="Count")],
        )
        base.update(overrides)
        return ChartDataPayload(**base)

    def _has_rollup(self, payload):
        qb = build_chart_query(payload, self._make_org_warehouse())
        return "ROLLUP" in str(qb.build().compile(compile_kwargs={"literal_binds": True})).upper()

    def test_row_rollup_off_when_no_totals(self):
        payload = self._rollup_payload(
            show_row_subtotals=False, show_grand_total=False, show_column_grand_total=False
        )
        assert self._has_rollup(payload) is False

    def test_row_rollup_on_for_column_grand_total(self):
        payload = self._rollup_payload(
            show_row_subtotals=False, show_grand_total=False, show_column_grand_total=True
        )
        assert self._has_rollup(payload) is True

    def test_row_rollup_on_for_row_subtotals_only(self):
        # Row subtotals alone force the rollup even when column grand total is off
        payload = self._rollup_payload(
            show_row_subtotals=True, show_grand_total=False, show_column_grand_total=False
        )
        assert self._has_rollup(payload) is True

    def test_row_rollup_falls_back_to_legacy_grand_total(self):
        # New flag unset → derive from legacy show_grand_total
        payload = self._rollup_payload(
            show_row_subtotals=False, show_grand_total=True, show_column_grand_total=None
        )
        assert self._has_rollup(payload) is True

    def test_column_grand_total_overrides_legacy(self):
        # Explicit new flag wins over legacy show_grand_total
        payload = self._rollup_payload(
            show_row_subtotals=False, show_grand_total=True, show_column_grand_total=False
        )
        assert self._has_rollup(payload) is False

    def test_pivot_query_multiple_column_dimensions(self):
        """Multiple column dimensions should produce multiple pivot_col labels and GROUPING markers"""
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="beneficiaries",
            row_dimensions=["district"],
            column_dimensions=["state", "program"],
            show_row_subtotals=True,
            show_grand_total=True,
            metrics=[
                ChartMetric(column="id", aggregation="count", alias="Count"),
            ],
        )
        ow = self._make_org_warehouse()
        qb = build_chart_query(payload, ow)
        stmt = qb.build()
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))

        assert "pivot_col_0" in compiled
        assert "pivot_col_1" in compiled
        assert "_grp_pivot_col_0" in compiled
        assert "_grp_pivot_col_1" in compiled

    def test_pivot_query_no_column_dimensions(self):
        """Without column_dimensions, only row ROLLUP should be present"""
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="beneficiaries",
            row_dimensions=["district"],
            show_row_subtotals=True,
            show_grand_total=True,
            metrics=[
                ChartMetric(column="id", aggregation="count", alias="Count"),
            ],
        )
        ow = self._make_org_warehouse()
        qb = build_chart_query(payload, ow)
        stmt = qb.build()
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        sql_upper = compiled.upper()

        assert "ROLLUP" in sql_upper
        assert "_grp_district" in compiled
        assert "_grp_pivot_col" not in compiled

    def test_pivot_query_no_subtotals(self):
        """With subtotals off, should use plain GROUP BY instead of ROLLUP on row dims"""
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="beneficiaries",
            row_dimensions=["district"],
            column_dimensions=["month_col"],
            show_row_subtotals=False,
            show_grand_total=False,
            metrics=[
                ChartMetric(column="id", aggregation="count", alias="Count"),
            ],
        )
        ow = self._make_org_warehouse()
        qb = build_chart_query(payload, ow)
        stmt = qb.build()
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        sql_upper = compiled.upper()

        assert "GROUP BY" in sql_upper

    def test_pivot_query_applies_filters(self):
        """Dashboard and chart filters should be applied as WHERE clauses"""
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="beneficiaries",
            row_dimensions=["district"],
            metrics=[
                ChartMetric(column="id", aggregation="count", alias="Count"),
            ],
            extra_config={
                "filters": [{"column": "status", "operator": "equals", "value": "active"}]
            },
        )
        ow = self._make_org_warehouse()
        qb = build_chart_query(payload, ow)
        stmt = qb.build()
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))

        assert "status" in compiled.lower()
