"""Tests for chart API endpoints"""
import pytest
from django.test import TestCase
from django.contrib.auth.models import User
from unittest.mock import patch, MagicMock

from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.visualization import Chart, ChartSnapshot
from ddpui.models.role_based_access import Role
from ddpui.tests.api_tests.test_user_org_api import mock_request, seed_db


class TestChartsAPI(TestCase):
    """Test cases for chart API endpoints"""

    def setUp(self):
        """Set up test data"""
        # Create test organization and user
        self.org = Org.objects.create(name="Test Org", slug="test-org")

        self.user = User.objects.create_user(
            username="testuser", email="test@example.com", password="testpass123"
        )

        self.role = Role.objects.create(name="Admin", slug="admin")

        self.orguser = OrgUser.objects.create(user=self.user, org=self.org, new_role=self.role)

        # Create test warehouse
        self.warehouse = OrgWarehouse.objects.create(
            org=self.org,
            name="Test Warehouse",
            credentials={"host": "localhost", "database": "test"},
        )

        # Create test chart
        self.chart = Chart.objects.create(
            title="Test Chart",
            description="Test description",
            chart_type="bar",
            computation_type="raw",
            schema_name="public",
            table_name="sales",
            x_axis_column="date",
            y_axis_column="amount",
            user=self.orguser,
            org=self.org,
        )

    @patch("ddpui.auth.org_user_auth")
    def test_list_charts(self, mock_auth):
        """Test listing charts for an organization"""
        mock_auth.return_value = self.orguser

        from ddpui.api.charts_api import list_charts

        request = mock_request(self.orguser)
        response = list_charts(request, self.orguser)

        self.assertEqual(len(response), 1)
        self.assertEqual(response[0].title, "Test Chart")

    @patch("ddpui.auth.org_user_auth")
    def test_get_chart(self, mock_auth):
        """Test retrieving a specific chart"""
        mock_auth.return_value = self.orguser

        from ddpui.api.charts_api import get_chart

        request = mock_request(self.orguser)
        response = get_chart(request, self.chart.id, self.orguser)

        self.assertEqual(response.id, self.chart.id)
        self.assertEqual(response.title, "Test Chart")

    @patch("ddpui.auth.org_user_auth")
    def test_create_chart(self, mock_auth):
        """Test creating a new chart"""
        mock_auth.return_value = self.orguser

        from ddpui.api.charts_api import create_chart, ChartCreate

        payload = ChartCreate(
            title="New Chart",
            description="New chart description",
            chart_type="pie",
            computation_type="aggregated",
            schema_name="public",
            table_name="orders",
            dimension_column="category",
            aggregate_column="total",
            aggregate_function="sum",
            customizations={"showLegend": True},
        )

        request = mock_request(self.orguser)
        response = create_chart(request, payload, self.orguser)

        self.assertEqual(response.title, "New Chart")
        self.assertEqual(response.chart_type, "pie")
        self.assertEqual(response.computation_type, "aggregated")

        # Verify chart was saved
        saved_chart = Chart.objects.get(id=response.id)
        self.assertEqual(saved_chart.title, "New Chart")
        self.assertEqual(saved_chart.org, self.org)
        self.assertEqual(saved_chart.user, self.orguser)

    @patch("ddpui.auth.org_user_auth")
    def test_update_chart(self, mock_auth):
        """Test updating a chart"""
        mock_auth.return_value = self.orguser

        from ddpui.api.charts_api import update_chart, ChartUpdate

        payload = ChartUpdate(
            title="Updated Chart",
            description="Updated description",
            is_favorite=True,
            customizations={"orientation": "horizontal"},
        )

        request = mock_request(self.orguser)
        response = update_chart(request, self.chart.id, payload, self.orguser)

        self.assertEqual(response.title, "Updated Chart")
        self.assertEqual(response.description, "Updated description")
        self.assertEqual(response.is_favorite, True)
        self.assertEqual(response.customizations["orientation"], "horizontal")

    @patch("ddpui.auth.org_user_auth")
    def test_delete_chart(self, mock_auth):
        """Test deleting a chart"""
        mock_auth.return_value = self.orguser

        from ddpui.api.charts_api import delete_chart

        chart_id = self.chart.id
        request = mock_request(self.orguser)
        response = delete_chart(request, chart_id, self.orguser)

        self.assertEqual(response["success"], True)

        # Verify chart was deleted
        with self.assertRaises(Chart.DoesNotExist):
            Chart.objects.get(id=chart_id)

    @patch("ddpui.auth.org_user_auth")
    @patch("ddpui.datainsights.warehouse.warehouse_factory.WarehouseFactory.get_warehouse_client")
    def test_get_chart_data_bar(self, mock_warehouse, mock_auth):
        """Test getting bar chart data"""
        mock_auth.return_value = self.orguser

        # Mock warehouse response
        mock_warehouse_instance = MagicMock()
        mock_warehouse_instance.execute.return_value = [
            {"date": "2024-01-01", "amount": 100},
            {"date": "2024-01-02", "amount": 150},
            {"date": "2024-01-03", "amount": 120},
        ]
        mock_warehouse.return_value = mock_warehouse_instance

        from ddpui.api.charts_api import get_chart_data, ChartDataPayload

        payload = ChartDataPayload(
            chart_type="bar",
            computation_type="raw",
            schema_name="public",
            table_name="sales",
            x_axis="date",
            y_axis="amount",
            customizations={"showDataLabels": True},
        )

        request = mock_request(self.orguser)
        response = get_chart_data(request, payload, self.orguser)

        # Verify response structure
        self.assertIn("data", response.dict())
        self.assertIn("echarts_config", response.dict())

        # Verify data transformation
        chart_data = response.data
        self.assertEqual(len(chart_data["xAxisData"]), 3)
        self.assertEqual(chart_data["xAxisData"], ["2024-01-01", "2024-01-02", "2024-01-03"])
        self.assertEqual(len(chart_data["series"]), 1)
        self.assertEqual(chart_data["series"][0]["data"], [100, 150, 120])

        # Verify ECharts config
        config = response.echarts_config
        self.assertEqual(config["series"][0]["type"], "bar")
        self.assertEqual(config["series"][0]["label"]["show"], True)

    @patch("ddpui.auth.org_user_auth")
    @patch("ddpui.datainsights.warehouse.warehouse_factory.WarehouseFactory.get_warehouse_client")
    def test_get_chart_data_pie_aggregated(self, mock_warehouse, mock_auth):
        """Test getting pie chart data with aggregation"""
        mock_auth.return_value = self.orguser

        # Mock warehouse response for aggregated data
        mock_warehouse_instance = MagicMock()
        mock_warehouse_instance.execute.return_value = [
            {"category": "Electronics", "sum_total": 5000},
            {"category": "Clothing", "sum_total": 3000},
            {"category": "Food", "sum_total": 2000},
        ]
        mock_warehouse.return_value = mock_warehouse_instance

        from ddpui.api.charts_api import get_chart_data, ChartDataPayload

        payload = ChartDataPayload(
            chart_type="pie",
            computation_type="aggregated",
            schema_name="public",
            table_name="orders",
            dimension_col="category",
            aggregate_col="total",
            aggregate_func="sum",
            customizations={"chartStyle": "donut"},
        )

        request = mock_request(self.orguser)
        response = get_chart_data(request, payload, self.orguser)

        # Verify data transformation for pie chart
        chart_data = response.data
        self.assertEqual(len(chart_data["pieData"]), 3)
        self.assertEqual(chart_data["pieData"][0]["name"], "Electronics")
        self.assertEqual(chart_data["pieData"][0]["value"], 5000)

        # Verify ECharts config for donut
        config = response.echarts_config
        self.assertEqual(config["series"][0]["type"], "pie")
        self.assertEqual(config["series"][0]["radius"], ["40%", "70%"])  # Donut style

    def test_echarts_config_generator_bar(self):
        """Test ECharts config generation for bar charts"""
        from ddpui.core.echarts_config_generator import EChartsConfigGenerator

        data = {
            "xAxisData": ["Q1", "Q2", "Q3", "Q4"],
            "series": [{"name": "Revenue", "data": [100, 200, 150, 300]}],
            "legend": ["Revenue"],
        }

        customizations = {
            "title": "Quarterly Revenue",
            "orientation": "vertical",
            "showDataLabels": True,
            "xAxisTitle": "Quarter",
            "yAxisTitle": "Revenue ($)",
        }

        config = EChartsConfigGenerator.generate_bar_config(data, customizations)

        self.assertEqual(config["title"]["text"], "Quarterly Revenue")
        self.assertEqual(config["xAxis"]["type"], "category")
        self.assertEqual(config["xAxis"]["name"], "Quarter")
        self.assertEqual(config["yAxis"]["name"], "Revenue ($)")
        self.assertEqual(config["series"][0]["label"]["show"], True)

    def test_echarts_config_generator_pie(self):
        """Test ECharts config generation for pie charts"""
        from ddpui.core.echarts_config_generator import EChartsConfigGenerator

        data = {
            "pieData": [
                {"value": 335, "name": "Direct"},
                {"value": 310, "name": "Email"},
                {"value": 234, "name": "Affiliate"},
            ],
            "seriesName": "Traffic Sources",
        }

        customizations = {
            "title": "Traffic Distribution",
            "chartStyle": "donut",
            "showDataLabels": True,
            "labelFormat": "name_percentage",
            "legendPosition": "bottom",
        }

        config = EChartsConfigGenerator.generate_pie_config(data, customizations)

        self.assertEqual(config["title"]["text"], "Traffic Distribution")
        self.assertEqual(config["series"][0]["radius"], ["40%", "70%"])  # Donut
        self.assertEqual(config["series"][0]["label"]["formatter"], "{b}\\n{d}%")
        self.assertEqual(config["legend"]["bottom"], 10)

    def test_echarts_config_generator_line(self):
        """Test ECharts config generation for line charts"""
        from ddpui.core.echarts_config_generator import EChartsConfigGenerator

        data = {
            "xAxisData": ["Jan", "Feb", "Mar", "Apr", "May"],
            "series": [
                {"name": "Sales", "data": [120, 132, 101, 134, 90]},
                {"name": "Revenue", "data": [220, 182, 191, 234, 290]},
            ],
            "legend": ["Sales", "Revenue"],
        }

        customizations = {
            "title": "Monthly Trends",
            "lineStyle": "smooth",
            "showDataPoints": False,
            "showDataLabels": True,
            "xAxisTitle": "Month",
            "yAxisTitle": "Value",
        }

        config = EChartsConfigGenerator.generate_line_config(data, customizations)

        self.assertEqual(config["title"]["text"], "Monthly Trends")
        self.assertEqual(config["series"][0]["smooth"], True)
        self.assertEqual(config["series"][0]["showSymbol"], False)
        self.assertEqual(config["series"][0]["label"]["show"], True)
        self.assertEqual(len(config["series"]), 2)
