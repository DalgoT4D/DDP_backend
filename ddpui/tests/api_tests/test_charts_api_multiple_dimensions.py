"""API Tests for Charts endpoints with Multiple Dimensions and Metrics

Tests the chart-data and chart-data-preview endpoints with:
1. Multiple dimensions (2+ dimensions)
2. Multiple metrics (2+ metrics)
3. Combination of multiple dimensions and metrics
4. Table charts with multiple dimensions
5. Edge cases and error handling
"""

import os
import django
from unittest.mock import patch, MagicMock
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.charts_api import (
    get_chart_data,
    get_chart_data_preview,
    generate_chart_data_and_config,
)
from ddpui.schemas.chart_schema import ChartDataPayload, ChartMetric
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    """A django User object"""
    from django.contrib.auth.models import User

    user = User.objects.create(
        username="multidimuser", email="multidimuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    """An Org object"""
    org = Org.objects.create(
        name="MultiDim Test Org",
        slug="multidim-test-org",
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


@pytest.fixture
def org_warehouse(org):
    """An OrgWarehouse object"""
    warehouse = OrgWarehouse.objects.create(
        org=org,
        wtype="postgres",
        name="Test Warehouse",
        airbyte_destination_id="dest-id",
    )
    yield warehouse
    warehouse.delete()


@pytest.fixture
def orguser(authuser, org):
    """An OrgUser with account manager role"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


# ================================================================================
# Test get_chart_data with Multiple Dimensions
# ================================================================================


class TestGetChartDataMultipleDimensions:
    """Tests for get_chart_data endpoint with multiple dimensions"""

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_table_chart_two_dimensions_one_metric(
        self, mock_generate, orguser, org_warehouse, seed_db
    ):
        """Test table chart with 2 dimensions and 1 metric"""
        mock_generate.return_value = {
            "data": {
                "tableData": [
                    {"KEY": "key1", "region": "North", "Total Count": 100},
                    {"KEY": "key2", "region": "South", "Total Count": 200},
                ],
                "columns": ["KEY", "region", "Total Count"],
            },
            "echarts_config": {},
        }

        request = mock_request(orguser)

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["KEY", "region"],
            metrics=[ChartMetric(aggregation="count", column=None, alias="Total Count")],
        )

        response = get_chart_data(request, payload)

        assert response.data is not None
        # Verify dimensions were processed
        table_data = response.data.get("tableData", response.data.get("data", {}))
        assert table_data is not None
        columns = response.data.get("columns", [])
        assert "KEY" in columns
        assert "region" in columns

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_table_chart_three_dimensions_two_metrics(
        self, mock_generate, orguser, org_warehouse, seed_db
    ):
        """Test table chart with 3 dimensions and 2 metrics"""
        mock_generate.return_value = {
            "data": {
                "tableData": [
                    {
                        "KEY": "key1",
                        "region": "North",
                        "country": "USA",
                        "Total Count": 100,
                        "Revenue": 5000.50,
                    },
                    {
                        "KEY": "key2",
                        "region": "South",
                        "country": "USA",
                        "Total Count": 200,
                        "Revenue": 10000.75,
                    },
                ],
                "columns": ["KEY", "region", "country", "Total Count", "Revenue"],
            },
            "echarts_config": {},
        }

        request = mock_request(orguser)

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["KEY", "region", "country"],
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="sum", column="revenue", alias="Revenue"),
            ],
        )

        response = get_chart_data(request, payload)

        assert response.data is not None
        columns = response.data.get("columns", [])
        # Verify all 3 dimensions are present
        assert "KEY" in columns
        assert "region" in columns
        assert "country" in columns
        # Verify metrics are present
        assert len(columns) >= 5  # 3 dimensions + 2 metrics

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_table_chart_dimensions_only_no_metrics(
        self, mock_generate, orguser, org_warehouse, seed_db
    ):
        """Test table chart with dimensions only (no metrics)"""
        mock_generate.return_value = {
            "data": {
                "tableData": [
                    {"KEY": "key1", "region": "North", "country": "USA"},
                    {"KEY": "key2", "region": "South", "country": "Canada"},
                ],
                "columns": ["KEY", "region", "country"],
            },
            "echarts_config": {},
        }

        request = mock_request(orguser)

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["KEY", "region", "country"],
            metrics=None,
        )

        response = get_chart_data(request, payload)

        assert response.data is not None
        columns = response.data.get("columns", [])
        # Should have exactly 3 dimensions, no metrics
        assert len(columns) == 3
        assert "KEY" in columns
        assert "region" in columns
        assert "country" in columns


# ================================================================================
# Test get_chart_data_preview with Multiple Dimensions
# ================================================================================


class TestGetChartDataPreviewMultipleDimensions:
    """Tests for get_chart_data_preview endpoint with multiple dimensions"""

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_preview_two_dimensions_one_metric(self, mock_preview, orguser, org_warehouse, seed_db):
        """Test preview with 2 dimensions and 1 metric"""
        mock_preview.return_value = {
            "columns": ["KEY", "region", "Total Count"],
            "column_types": {"KEY": "string", "region": "string", "Total Count": "number"},
            "data": [
                {"KEY": "key1", "region": "North", "Total Count": 100},
                {"KEY": "key2", "region": "South", "Total Count": 200},
            ],
            "page": 0,
            "limit": 10,
        }

        request = mock_request(orguser)

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["KEY", "region"],
            metrics=[ChartMetric(aggregation="count", column=None, alias="Total Count")],
        )

        response = get_chart_data_preview(request, payload, page=0, limit=10)

        assert response.columns is not None
        assert len(response.columns) >= 2  # Should have at least 2 dimension columns
        assert "KEY" in response.columns
        assert "region" in response.columns
        assert len(response.data) > 0

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_preview_three_dimensions_two_metrics(
        self, mock_preview, orguser, org_warehouse, seed_db
    ):
        """Test preview with 3 dimensions and 2 metrics"""
        mock_preview.return_value = {
            "columns": ["KEY", "region", "country", "Total Count", "Revenue"],
            "column_types": {
                "KEY": "string",
                "region": "string",
                "country": "string",
                "Total Count": "number",
                "Revenue": "number",
            },
            "data": [
                {
                    "KEY": "key1",
                    "region": "North",
                    "country": "USA",
                    "Total Count": 100,
                    "Revenue": 5000.50,
                },
            ],
            "page": 0,
            "limit": 10,
        }

        request = mock_request(orguser)

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["KEY", "region", "country"],
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="sum", column="revenue", alias="Revenue"),
            ],
        )

        response = get_chart_data_preview(request, payload, page=0, limit=10)

        assert response.columns is not None
        assert len(response.columns) >= 3  # Should have at least 3 dimension columns
        assert "KEY" in response.columns
        assert "region" in response.columns
        assert "country" in response.columns
        # Should also have metric columns
        assert len(response.columns) >= 5  # 3 dimensions + 2 metrics

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_preview_dimensions_only_no_metrics(
        self, mock_preview, orguser, org_warehouse, seed_db
    ):
        """Test preview with dimensions only (no metrics)"""
        mock_preview.return_value = {
            "columns": ["KEY", "region", "country"],
            "column_types": {"KEY": "string", "region": "string", "country": "string"},
            "data": [
                {"KEY": "key1", "region": "North", "country": "USA"},
                {"KEY": "key2", "region": "South", "country": "Canada"},
            ],
            "page": 0,
            "limit": 10,
        }

        request = mock_request(orguser)

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["KEY", "region", "country"],
            metrics=None,
        )

        response = get_chart_data_preview(request, payload, page=0, limit=10)

        assert response.columns is not None
        assert len(response.columns) == 3  # Should have exactly 3 dimension columns
        assert "KEY" in response.columns
        assert "region" in response.columns
        assert "country" in response.columns
        assert len(response.data) > 0


# ================================================================================
# Test generate_chart_data_and_config with Multiple Dimensions
# ================================================================================


class TestGenerateChartDataMultipleDimensions:
    """Tests for generate_chart_data_and_config function with multiple dimensions"""

    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    @patch("ddpui.core.charts.charts_service.execute_chart_query")
    def test_generate_table_two_dimensions(self, mock_execute, mock_warehouse, org_warehouse):
        """Test generating table chart data with 2 dimensions"""
        # Mock warehouse client
        mock_warehouse_obj = MagicMock()
        mock_warehouse_obj.engine = MagicMock()
        mock_warehouse.return_value = mock_warehouse_obj

        # Mock query execution to return data with both dimensions
        mock_execute.return_value = [
            {"KEY": "key1", "region": "North", "Total Count": 100},
            {"KEY": "key2", "region": "South", "Total Count": 200},
        ]

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["KEY", "region"],
            metrics=[ChartMetric(aggregation="count", column=None, alias="Total Count")],
        )

        result = generate_chart_data_and_config(payload, org_warehouse)

        assert result is not None
        assert "data" in result
        # Verify table data structure
        table_data = result["data"].get("tableData", result["data"])
        assert table_data is not None
        assert "columns" in result["data"]
        columns = result["data"]["columns"]
        assert "KEY" in columns
        assert "region" in columns

    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    @patch("ddpui.core.charts.charts_service.execute_chart_query")
    def test_generate_table_three_dimensions_two_metrics(
        self, mock_execute, mock_warehouse, org_warehouse
    ):
        """Test generating table chart data with 3 dimensions and 2 metrics"""
        # Mock warehouse client
        mock_warehouse_obj = MagicMock()
        mock_warehouse_obj.engine = MagicMock()
        mock_warehouse.return_value = mock_warehouse_obj

        # Mock query execution
        mock_execute.return_value = [
            {
                "KEY": "key1",
                "region": "North",
                "country": "USA",
                "Total Count": 100,
                "Revenue": 5000.50,
            },
        ]

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["KEY", "region", "country"],
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="sum", column="revenue", alias="Revenue"),
            ],
        )

        result = generate_chart_data_and_config(payload, org_warehouse)

        assert result is not None
        assert "data" in result
        columns = result["data"]["columns"]
        # Should have all 3 dimensions
        assert "KEY" in columns
        assert "region" in columns
        assert "country" in columns
        # Should have metric columns
        assert len(columns) >= 5  # 3 dimensions + 2 metrics

    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    @patch("ddpui.core.charts.charts_service.execute_chart_query")
    def test_generate_table_dimensions_only(self, mock_execute, mock_warehouse, org_warehouse):
        """Test generating table chart data with dimensions only"""
        # Mock warehouse client
        mock_warehouse_obj = MagicMock()
        mock_warehouse_obj.engine = MagicMock()
        mock_warehouse.return_value = mock_warehouse_obj

        # Mock query execution
        mock_execute.return_value = [
            {"KEY": "key1", "region": "North", "country": "USA"},
            {"KEY": "key2", "region": "South", "country": "Canada"},
        ]

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["KEY", "region", "country"],
            metrics=None,
        )

        result = generate_chart_data_and_config(payload, org_warehouse)

        assert result is not None
        assert "data" in result
        columns = result["data"]["columns"]
        # Should have exactly 3 dimensions
        assert len(columns) == 3
        assert "KEY" in columns
        assert "region" in columns
        assert "country" in columns


# ================================================================================
# Test Error Cases
# ================================================================================


# ================================================================================
# Test Multiple Dimensions with Multiple Metrics (Comprehensive)
# ================================================================================


class TestMultipleDimensionsMultipleMetrics:
    """Comprehensive tests for multiple dimensions with multiple metrics"""

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_two_dimensions_two_metrics(self, mock_generate, orguser, org_warehouse, seed_db):
        """Test table chart with 2 dimensions and 2 metrics"""
        mock_generate.return_value = {
            "data": {
                "tableData": [
                    {
                        "KEY": "key1",
                        "region": "North",
                        "Total Count": 100,
                        "Revenue": 5000.50,
                    },
                    {
                        "KEY": "key2",
                        "region": "South",
                        "Total Count": 200,
                        "Revenue": 10000.75,
                    },
                ],
                "columns": ["KEY", "region", "Total Count", "Revenue"],
            },
            "echarts_config": {},
        }

        request = mock_request(orguser)

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["KEY", "region"],
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="sum", column="revenue", alias="Revenue"),
            ],
        )

        response = get_chart_data(request, payload)

        assert response.data is not None
        columns = response.data.get("columns", [])
        assert len(columns) == 4  # 2 dimensions + 2 metrics
        assert "KEY" in columns
        assert "region" in columns
        assert "Total Count" in columns
        assert "Revenue" in columns

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_two_dimensions_three_metrics(self, mock_generate, orguser, org_warehouse, seed_db):
        """Test table chart with 2 dimensions and 3 metrics"""
        mock_generate.return_value = {
            "data": {
                "tableData": [
                    {
                        "KEY": "key1",
                        "region": "North",
                        "Total Count": 100,
                        "Revenue": 5000.50,
                        "Average Price": 50.00,
                    },
                ],
                "columns": ["KEY", "region", "Total Count", "Revenue", "Average Price"],
            },
            "echarts_config": {},
        }

        request = mock_request(orguser)

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["KEY", "region"],
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="sum", column="revenue", alias="Revenue"),
                ChartMetric(aggregation="avg", column="price", alias="Average Price"),
            ],
        )

        response = get_chart_data(request, payload)

        assert response.data is not None
        columns = response.data.get("columns", [])
        assert len(columns) == 5  # 2 dimensions + 3 metrics
        assert "KEY" in columns
        assert "region" in columns
        assert "Total Count" in columns
        assert "Revenue" in columns
        assert "Average Price" in columns

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_four_dimensions_three_metrics(self, mock_generate, orguser, org_warehouse, seed_db):
        """Test table chart with 4 dimensions and 3 metrics"""
        mock_generate.return_value = {
            "data": {
                "tableData": [
                    {
                        "KEY": "key1",
                        "region": "North",
                        "country": "USA",
                        "state": "CA",
                        "Total Count": 100,
                        "Revenue": 5000.50,
                        "Average Price": 50.00,
                    },
                ],
                "columns": [
                    "KEY",
                    "region",
                    "country",
                    "state",
                    "Total Count",
                    "Revenue",
                    "Average Price",
                ],
            },
            "echarts_config": {},
        }

        request = mock_request(orguser)

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["KEY", "region", "country", "state"],
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="sum", column="revenue", alias="Revenue"),
                ChartMetric(aggregation="avg", column="price", alias="Average Price"),
            ],
        )

        response = get_chart_data(request, payload)

        assert response.data is not None
        columns = response.data.get("columns", [])
        assert len(columns) == 7  # 4 dimensions + 3 metrics
        assert "KEY" in columns
        assert "region" in columns
        assert "country" in columns
        assert "state" in columns
        assert "Total Count" in columns
        assert "Revenue" in columns
        assert "Average Price" in columns

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_preview_two_dimensions_three_metrics(
        self, mock_preview, orguser, org_warehouse, seed_db
    ):
        """Test preview with 2 dimensions and 3 metrics"""
        mock_preview.return_value = {
            "columns": [
                "KEY",
                "region",
                "Total Count",
                "Revenue",
                "Average Price",
            ],
            "column_types": {
                "KEY": "string",
                "region": "string",
                "Total Count": "number",
                "Revenue": "number",
                "Average Price": "number",
            },
            "data": [
                {
                    "KEY": "key1",
                    "region": "North",
                    "Total Count": 100,
                    "Revenue": 5000.50,
                    "Average Price": 50.00,
                },
            ],
            "page": 0,
            "limit": 10,
        }

        request = mock_request(orguser)

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["KEY", "region"],
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="sum", column="revenue", alias="Revenue"),
                ChartMetric(aggregation="avg", column="price", alias="Average Price"),
            ],
        )

        response = get_chart_data_preview(request, payload, page=0, limit=10)

        assert response.columns is not None
        assert len(response.columns) == 5  # 2 dimensions + 3 metrics
        assert "KEY" in response.columns
        assert "region" in response.columns
        assert "Total Count" in response.columns
        assert "Revenue" in response.columns
        assert "Average Price" in response.columns

    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    @patch("ddpui.core.charts.charts_service.execute_chart_query")
    def test_generate_two_dimensions_four_metrics(
        self, mock_execute, mock_warehouse, org_warehouse
    ):
        """Test generating table chart data with 2 dimensions and 4 metrics"""
        mock_warehouse_obj = MagicMock()
        mock_warehouse_obj.engine = MagicMock()
        mock_warehouse.return_value = mock_warehouse_obj

        mock_execute.return_value = [
            {
                "KEY": "key1",
                "region": "North",
                "Total Count": 100,
                "Revenue": 5000.50,
                "Average Price": 50.00,
                "Max Price": 100.00,
            },
        ]

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["KEY", "region"],
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="sum", column="revenue", alias="Revenue"),
                ChartMetric(aggregation="avg", column="price", alias="Average Price"),
                ChartMetric(aggregation="max", column="price", alias="Max Price"),
            ],
        )

        result = generate_chart_data_and_config(payload, org_warehouse)

        assert result is not None
        assert "data" in result
        columns = result["data"]["columns"]
        assert len(columns) == 6  # 2 dimensions + 4 metrics
        assert "KEY" in columns
        assert "region" in columns
        assert "Total Count" in columns
        assert "Revenue" in columns
        assert "Average Price" in columns
        assert "Max Price" in columns

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_three_dimensions_four_metrics_different_types(
        self, mock_generate, orguser, org_warehouse, seed_db
    ):
        """Test table chart with 3 dimensions and 4 metrics of different aggregation types"""
        mock_generate.return_value = {
            "data": {
                "tableData": [
                    {
                        "KEY": "key1",
                        "region": "North",
                        "country": "USA",
                        "Total Count": 100,
                        "Revenue": 5000.50,
                        "Average Price": 50.00,
                        "Min Price": 10.00,
                    },
                ],
                "columns": [
                    "KEY",
                    "region",
                    "country",
                    "Total Count",
                    "Revenue",
                    "Average Price",
                    "Min Price",
                ],
            },
            "echarts_config": {},
        }

        request = mock_request(orguser)

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["KEY", "region", "country"],
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="sum", column="revenue", alias="Revenue"),
                ChartMetric(aggregation="avg", column="price", alias="Average Price"),
                ChartMetric(aggregation="min", column="price", alias="Min Price"),
            ],
        )

        response = get_chart_data(request, payload)

        assert response.data is not None
        columns = response.data.get("columns", [])
        assert len(columns) == 7  # 3 dimensions + 4 metrics
        # Verify all dimensions
        assert "KEY" in columns
        assert "region" in columns
        assert "country" in columns
        # Verify all metrics
        assert "Total Count" in columns
        assert "Revenue" in columns
        assert "Average Price" in columns
        assert "Min Price" in columns


# ================================================================================
# Test Error Cases
# ================================================================================


class TestMultipleDimensionsErrorCases:
    """Tests for error handling with multiple dimensions"""

    def test_table_chart_no_dimensions(self, orguser, org_warehouse, seed_db):
        """Test table chart with no dimensions should raise error"""
        request = mock_request(orguser)

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=None,
            metrics=[ChartMetric(aggregation="count", column=None)],
        )

        with pytest.raises(HttpError) as excinfo:
            get_chart_data(request, payload)

        assert excinfo.value.status_code in [400, 500]  # Should be validation or server error

    def test_table_chart_empty_dimensions(self, orguser, org_warehouse, seed_db):
        """Test table chart with empty dimensions array should raise error"""
        request = mock_request(orguser)

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=[],
            metrics=[ChartMetric(aggregation="count", column=None)],
        )

        with pytest.raises(HttpError) as excinfo:
            get_chart_data(request, payload)

        assert excinfo.value.status_code in [400, 500]
