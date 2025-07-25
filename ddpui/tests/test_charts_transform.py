"""Unit tests for chart transformation functions"""
import pytest
from unittest.mock import Mock
from decimal import Decimal
from datetime import datetime

from ddpui.api.charts_api import transform_data_for_chart, get_query_hash


class TestTransformDataForChart:
    """Test cases for transform_data_for_chart function"""

    def test_bar_chart_raw_data(self):
        """Test transformation for bar chart with raw data"""
        results = [
            {"category": "Electronics", "amount": 1000},
            {"category": "Clothing", "amount": 500},
            {"category": "Food", "amount": 750},
        ]

        payload = Mock(
            x_axis="category",
            y_axis="amount",
            extra_dimension=None,
            dimension_col=None,
            aggregate_col=None,
            aggregate_func=None,
        )

        transformed = transform_data_for_chart(results, "bar", "raw", payload)

        assert transformed["xAxisData"] == ["Electronics", "Clothing", "Food"]
        assert transformed["series"][0]["name"] == "amount"
        assert transformed["series"][0]["data"] == [1000, 500, 750]
        assert transformed["legend"] == ["amount"]

    def test_bar_chart_aggregated_data(self):
        """Test transformation for bar chart with aggregated data"""
        results = [
            {"category": "Electronics", "sum_amount": 5000},
            {"category": "Clothing", "sum_amount": 3000},
            {"category": "Food", "sum_amount": 4000},
        ]

        payload = Mock(
            dimension_col="category",
            aggregate_col="amount",
            aggregate_func="sum",
            extra_dimension=None,
        )

        transformed = transform_data_for_chart(results, "bar", "aggregated", payload)

        assert transformed["xAxisData"] == ["Electronics", "Clothing", "Food"]
        assert transformed["series"][0]["name"] == "sum(amount)"
        assert transformed["series"][0]["data"] == [5000, 3000, 4000]

    def test_bar_chart_with_extra_dimension(self):
        """Test transformation for bar chart with extra dimension"""
        results = [
            {"category": "Electronics", "region": "North", "sum_amount": 2000},
            {"category": "Electronics", "region": "South", "sum_amount": 3000},
            {"category": "Clothing", "region": "North", "sum_amount": 1500},
            {"category": "Clothing", "region": "South", "sum_amount": 1500},
        ]

        payload = Mock(
            dimension_col="category",
            aggregate_col="amount",
            aggregate_func="sum",
            extra_dimension="region",
        )

        transformed = transform_data_for_chart(results, "bar", "aggregated", payload)

        assert "Electronics" in transformed["xAxisData"]
        assert "Clothing" in transformed["xAxisData"]
        assert len(transformed["series"]) == 2  # Two regions
        assert transformed["legend"] == ["North", "South"]

    def test_pie_chart_raw_data(self):
        """Test transformation for pie chart with raw data"""
        results = [
            {"category": "Electronics"},
            {"category": "Electronics"},
            {"category": "Clothing"},
            {"category": "Food"},
            {"category": "Food"},
            {"category": "Food"},
        ]

        payload = Mock(x_axis="category", y_axis=None)

        transformed = transform_data_for_chart(results, "pie", "raw", payload)

        pie_data = {item["name"]: item["value"] for item in transformed["pieData"]}
        assert pie_data["Electronics"] == 2
        assert pie_data["Clothing"] == 1
        assert pie_data["Food"] == 3
        assert transformed["seriesName"] == "category"

    def test_pie_chart_aggregated_data(self):
        """Test transformation for pie chart with aggregated data"""
        results = [
            {"category": "Electronics", "sum_amount": 5000},
            {"category": "Clothing", "sum_amount": 3000},
            {"category": "Food", "sum_amount": 4000},
        ]

        payload = Mock(dimension_col="category", aggregate_col="amount", aggregate_func="sum")

        transformed = transform_data_for_chart(results, "pie", "aggregated", payload)

        pie_data = {item["name"]: item["value"] for item in transformed["pieData"]}
        assert pie_data["Electronics"] == 5000
        assert pie_data["Clothing"] == 3000
        assert pie_data["Food"] == 4000
        assert transformed["seriesName"] == "sum(amount)"

    def test_line_chart_transformation(self):
        """Test transformation for line chart"""
        results = [
            {"month": "Jan", "sales": 100},
            {"month": "Feb", "sales": 150},
            {"month": "Mar", "sales": 120},
        ]

        payload = Mock(x_axis="month", y_axis="sales", extra_dimension=None)

        transformed = transform_data_for_chart(results, "line", "raw", payload)

        assert transformed["xAxisData"] == ["Jan", "Feb", "Mar"]
        assert transformed["series"][0]["name"] == "sales"
        assert transformed["series"][0]["data"] == [100, 150, 120]

    def test_empty_results(self):
        """Test transformation with empty results"""
        results = []
        payload = Mock(x_axis="category", y_axis="amount")

        transformed = transform_data_for_chart(results, "bar", "raw", payload)

        assert transformed["xAxisData"] == []
        assert transformed["series"][0]["data"] == []

    def test_decimal_handling(self):
        """Test that Decimal values are handled properly"""
        results = [
            {"category": "Electronics", "amount": Decimal("1000.50")},
            {"category": "Clothing", "amount": Decimal("500.25")},
        ]

        payload = Mock(x_axis="category", y_axis="amount", extra_dimension=None)

        transformed = transform_data_for_chart(results, "bar", "raw", payload)
        # Decimals should be converted to float
        assert transformed["series"][0]["data"] == [1000.5, 500.25]

    def test_datetime_handling(self):
        """Test that datetime values are handled properly"""
        results = [
            {"date": datetime(2024, 1, 1), "sales": 100},
            {"date": datetime(2024, 1, 2), "sales": 150},
        ]

        payload = Mock(x_axis="date", y_axis="sales", extra_dimension=None)

        transformed = transform_data_for_chart(results, "line", "raw", payload)
        # Dates should be converted to ISO format strings
        assert isinstance(transformed["xAxisData"][0], str)
        assert "2024-01-01" in transformed["xAxisData"][0]

    def test_none_column_validation(self):
        """Test that None values in columns are handled properly"""
        results = [
            {"category": "Electronics", "amount": 1000},
            {"category": "Clothing", "amount": 500},
        ]

        payload = Mock(x_axis="category", y_axis=None, extra_dimension=None)  # None y_axis

        # This should handle None gracefully
        transformed = transform_data_for_chart(results, "bar", "raw", payload)
        # Should return empty dict when y_axis is None
        assert transformed == {}

    def test_empty_dimension_values(self):
        """Test handling of empty string dimension values"""
        results = [
            {"category": "", "sum_amount": 100},
            {"category": "Electronics", "sum_amount": 200},
            {"category": "", "sum_amount": 150},
        ]

        payload = Mock(
            dimension_col="category",
            aggregate_col="amount",
            aggregate_func="sum",
            extra_dimension=None,
        )

        transformed = transform_data_for_chart(results, "bar", "aggregated", payload)
        # Empty strings should be preserved
        assert "" in transformed["xAxisData"]
        assert "Electronics" in transformed["xAxisData"]

    def test_very_large_dataset(self):
        """Test handling of large datasets"""
        # Create a large dataset
        results = [{"x": i, "y": i * 2} for i in range(10000)]

        payload = Mock(x_axis="x", y_axis="y", extra_dimension=None)

        # Should handle large datasets without error
        transformed = transform_data_for_chart(results, "line", "raw", payload)
        assert len(transformed["xAxisData"]) == 10000
        assert len(transformed["series"][0]["data"]) == 10000

    def test_invalid_chart_type(self):
        """Test handling of invalid chart type"""
        results = [{"x": 1, "y": 2}]
        payload = Mock(x_axis="x", y_axis="y")

        # Should handle unknown chart type gracefully
        transformed = transform_data_for_chart(results, "invalid_type", "raw", payload)
        # Should return empty dict for invalid chart type
        assert transformed == {}


class TestGetQueryHash:
    """Test cases for get_query_hash function"""

    def test_query_hash_generation(self):
        """Test that query hash is generated consistently"""
        from ddpui.api.charts_api import ChartDataPayload

        payload = ChartDataPayload(
            chart_type="bar",
            computation_type="aggregated",
            schema_name="analytics",
            table_name="sales",
            dimension_col="category",
            aggregate_col="amount",
            aggregate_func="sum",
            offset=0,
            limit=100,
        )

        hash1 = get_query_hash(payload)
        hash2 = get_query_hash(payload)

        assert hash1 == hash2
        assert len(hash1) == 64  # SHA256 hash length

    def test_different_payloads_different_hashes(self):
        """Test that different payloads generate different hashes"""
        from ddpui.api.charts_api import ChartDataPayload

        payload1 = ChartDataPayload(
            chart_type="bar",
            computation_type="raw",
            schema_name="analytics",
            table_name="sales",
            x_axis="category",
            y_axis="amount",
            offset=0,
            limit=100,
        )

        payload2 = ChartDataPayload(
            chart_type="bar",
            computation_type="raw",
            schema_name="analytics",
            table_name="sales",
            x_axis="category",
            y_axis="quantity",  # Different column
            offset=0,
            limit=100,
        )

        hash1 = get_query_hash(payload1)
        hash2 = get_query_hash(payload2)

        assert hash1 != hash2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
