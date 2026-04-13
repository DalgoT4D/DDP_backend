import pytest
from ddpui.schemas.chart_schema import ChartDataPayload


class TestPivotTablePayload:
    def test_pivot_table_payload_accepts_dimensions(self):
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="beneficiaries",
            row_dimensions=["district", "program"],
            column_dimensions=["enrollment_date", "category"],
            column_time_grains={"enrollment_date": "month"},
            show_row_subtotals=True,
            show_grand_total=True,
        )
        assert payload.row_dimensions == ["district", "program"]
        assert payload.column_dimensions == ["enrollment_date", "category"]
        assert payload.column_time_grains == {"enrollment_date": "month"}
        assert payload.show_row_subtotals is True
        assert payload.show_grand_total is True

    def test_pivot_table_payload_defaults(self):
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="beneficiaries",
        )
        assert payload.row_dimensions is None
        assert payload.column_dimensions is None
        assert payload.column_time_grains is None
        assert payload.show_row_subtotals is False
        assert payload.show_grand_total is True

    def test_non_pivot_payload_unaffected(self):
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="beneficiaries",
        )
        assert payload.row_dimensions is None
