import pytest
from unittest.mock import MagicMock
from ddpui.schemas.chart_schema import ChartDataPayload
from ddpui.core.charts.pivot_service import check_pivot_cardinality


class TestCheckPivotCardinality:
    def test_raises_on_high_cardinality(self):
        warehouse = MagicMock()
        warehouse.execute.return_value = [{"cnt": 100}]
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="data",
            column_dimensions=["category"],
        )

        with pytest.raises(ValueError, match="too many unique values"):
            check_pivot_cardinality(warehouse, payload)

    def test_passes_on_low_cardinality(self):
        warehouse = MagicMock()
        warehouse.execute.return_value = [{"cnt": 10}]
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="data",
            column_dimensions=["category"],
        )
        check_pivot_cardinality(warehouse, payload)

    def test_skipped_when_no_column_dimensions(self):
        warehouse = MagicMock()
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="data",
        )
        check_pivot_cardinality(warehouse, payload)
        warehouse.execute.assert_not_called()

    def test_multiple_column_dimensions_checked(self):
        warehouse = MagicMock()
        warehouse.execute.return_value = [{"cnt": 10}]
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="data",
            column_dimensions=["category", "region"],
        )
        check_pivot_cardinality(warehouse, payload)
        # Should be called once per column dimension
        assert warehouse.execute.call_count == 2
