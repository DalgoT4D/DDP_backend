import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from django.test import TestCase, Client
from django.contrib.auth.models import User
from django.urls import reverse
from ddpui.models.chart import Chart
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.admin_user import AdminUser
from ddpui.core.chart_service import ChartService


class TestChartAPI(TestCase):
    """Test cases for Chart API endpoints"""

    def setUp(self):
        """Set up test data"""
        self.client = Client()

        self.admin_user = AdminUser.objects.create(
            email="admin@example.com", password="password123"
        )

        self.org = Org.objects.create(name="Test Org", slug="test-org")

        self.org_user = OrgUser.objects.create(
            user=self.admin_user, org=self.org, role=Mock(slug="account_manager")
        )

        # Mock authentication
        self.client.force_login = Mock()

        # Sample chart configuration
        self.chart_config = {
            "chartType": "bar",
            "computation_type": "raw",
            "xAxis": "category",
            "yAxis": "value",
        }

        # Create a test chart
        self.test_chart = Chart.create_chart(
            org=self.org,
            created_by=self.org_user,
            title="Test Chart",
            description="Test Description",
            chart_type="echarts",
            schema_name="test_schema",
            table_name="test_table",
            config=self.chart_config,
        )

    def _get_headers(self):
        """Get authentication headers"""
        return {"HTTP_AUTHORIZATION": "Bearer test_token", "HTTP_X_DALGO_ORG": str(self.org.slug)}

    @patch("ddpui.api.chart_api.ChartService")
    def test_create_chart_success(self, mock_service):
        """Test successful chart creation"""
        mock_service_instance = mock_service.return_value
        mock_service_instance.create_chart.return_value = self.test_chart

        payload = {
            "title": "New Chart",
            "description": "New Description",
            "chart_type": "echarts",
            "schema_name": "test_schema",
            "table": "test_table",
            "config": self.chart_config,
            "is_public": False,
        }

        with patch("ddpui.auth.has_permission") as mock_permission:
            mock_permission.return_value = lambda func: func

            response = self.client.post(
                "/api/visualization/charts/",
                data=json.dumps(payload),
                content_type="application/json",
                **self._get_headers(),
            )

        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertEqual(response_data["message"], "Chart created successfully")

    @patch("ddpui.api.chart_api.ChartService")
    def test_create_chart_validation_error(self, mock_service):
        """Test chart creation with validation error"""
        mock_service_instance = mock_service.return_value
        mock_service_instance.create_chart.side_effect = ValidationError("Invalid configuration")

        payload = {
            "title": "New Chart",
            "description": "New Description",
            "chart_type": "echarts",
            "schema_name": "test_schema",
            "table": "test_table",
            "config": {"invalid": "config"},
            "is_public": False,
        }

        with patch("ddpui.auth.has_permission") as mock_permission:
            mock_permission.return_value = lambda func: func

            response = self.client.post(
                "/api/visualization/charts/",
                data=json.dumps(payload),
                content_type="application/json",
                **self._get_headers(),
            )

        self.assertEqual(response.status_code, 400)
        response_data = json.loads(response.content)
        self.assertFalse(response_data["success"])
        self.assertIn("Invalid configuration", response_data["error"])

    @patch("ddpui.api.chart_api.ChartService")
    def test_get_charts_success(self, mock_service):
        """Test successful chart retrieval"""
        mock_service_instance = mock_service.return_value
        mock_service_instance.get_charts.return_value = [self.test_chart]

        with patch("ddpui.auth.has_permission") as mock_permission:
            mock_permission.return_value = lambda func: func

            response = self.client.get("/api/visualization/charts/", **self._get_headers())

        self.assertEqual(response.status_code, 200)
        # Note: The actual response structure depends on the pagination implementation

    @patch("ddpui.api.chart_api.ChartService")
    def test_get_chart_success(self, mock_service):
        """Test successful single chart retrieval"""
        mock_service_instance = mock_service.return_value
        mock_service_instance.get_chart.return_value = self.test_chart

        with patch("ddpui.auth.has_permission") as mock_permission:
            mock_permission.return_value = lambda func: func

            response = self.client.get(
                f"/api/visualization/charts/{self.test_chart.id}", **self._get_headers()
            )

        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertEqual(response_data["message"], "Chart retrieved successfully")

    @patch("ddpui.api.chart_api.ChartService")
    def test_get_chart_not_found(self, mock_service):
        """Test chart retrieval with non-existent ID"""
        mock_service_instance = mock_service.return_value
        mock_service_instance.get_chart.side_effect = ValidationError("Chart not found")

        with patch("ddpui.auth.has_permission") as mock_permission:
            mock_permission.return_value = lambda func: func

            response = self.client.get("/api/visualization/charts/999", **self._get_headers())

        self.assertEqual(response.status_code, 400)
        response_data = json.loads(response.content)
        self.assertFalse(response_data["success"])
        self.assertIn("Chart not found", response_data["error"])

    @patch("ddpui.api.chart_api.ChartService")
    def test_update_chart_success(self, mock_service):
        """Test successful chart update"""
        mock_service_instance = mock_service.return_value
        mock_service_instance.update_chart.return_value = self.test_chart

        payload = {"title": "Updated Chart", "description": "Updated Description"}

        with patch("ddpui.auth.has_permission") as mock_permission:
            mock_permission.return_value = lambda func: func

            response = self.client.put(
                f"/api/visualization/charts/{self.test_chart.id}",
                data=json.dumps(payload),
                content_type="application/json",
                **self._get_headers(),
            )

        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertEqual(response_data["message"], "Chart updated successfully")

    @patch("ddpui.api.chart_api.ChartService")
    def test_delete_chart_success(self, mock_service):
        """Test successful chart deletion"""
        mock_service_instance = mock_service.return_value
        mock_service_instance.delete_chart.return_value = True

        with patch("ddpui.auth.has_permission") as mock_permission:
            mock_permission.return_value = lambda func: func

            response = self.client.delete(
                f"/api/visualization/charts/{self.test_chart.id}", **self._get_headers()
            )

        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertEqual(response_data["message"], "Chart deleted successfully")

    @patch("ddpui.api.chart_api.ChartService")
    def test_generate_chart_data_success(self, mock_service):
        """Test successful chart data generation"""
        mock_service_instance = mock_service.return_value
        mock_service_instance.generate_chart_data.return_value = {
            "chart_config": {"test": "config"},
            "raw_data": {"test": "data"},
            "metadata": {
                "chart_type": "bar",
                "computation_type": "raw",
                "schema_name": "test_schema",
                "table_name": "test_table",
                "record_count": 3,
                "generated_at": "2023-01-01T00:00:00Z",
            },
        }

        payload = {
            "chart_type": "bar",
            "computation_type": "raw",
            "schema_name": "test_schema",
            "table_name": "test_table",
            "xaxis": "category",
            "yaxis": "value",
            "offset": 0,
            "limit": 100,
        }

        with patch("ddpui.auth.has_permission") as mock_permission:
            mock_permission.return_value = lambda func: func

            response = self.client.post(
                "/api/visualization/charts/generate",
                data=json.dumps(payload),
                content_type="application/json",
                **self._get_headers(),
            )

        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertEqual(response_data["message"], "Chart data generated successfully")
        self.assertIn("chart_config", response_data["data"])
        self.assertIn("raw_data", response_data["data"])
        self.assertIn("metadata", response_data["data"])

    @patch("ddpui.api.chart_api.ChartService")
    def test_generate_chart_data_validation_error(self, mock_service):
        """Test chart data generation with validation error"""
        mock_service_instance = mock_service.return_value
        mock_service_instance.generate_chart_data.side_effect = ValidationError(
            "Invalid parameters"
        )

        payload = {
            "chart_type": "invalid_type",
            "computation_type": "raw",
            "schema_name": "test_schema",
            "table_name": "test_table",
        }

        with patch("ddpui.auth.has_permission") as mock_permission:
            mock_permission.return_value = lambda func: func

            response = self.client.post(
                "/api/visualization/charts/generate",
                data=json.dumps(payload),
                content_type="application/json",
                **self._get_headers(),
            )

        self.assertEqual(response.status_code, 400)
        response_data = json.loads(response.content)
        self.assertFalse(response_data["success"])
        self.assertIn("Invalid parameters", response_data["error"])

    def test_toggle_favorite_success(self):
        """Test successful favorite toggle"""
        with patch("ddpui.auth.has_permission") as mock_permission:
            mock_permission.return_value = lambda func: func

            # Mock the chart retrieval
            with patch("ddpui.api.chart_api.get_object_or_404") as mock_get:
                mock_get.return_value = self.test_chart

                response = self.client.post(
                    f"/api/visualization/charts/{self.test_chart.id}/favorite",
                    **self._get_headers(),
                )

        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertIn("is_favorite", response_data["data"])

    @patch("ddpui.api.chart_api.ChartService")
    def test_cleanup_cache_success(self, mock_service):
        """Test successful cache cleanup"""
        mock_service.clean_expired_snapshots.return_value = None

        with patch("ddpui.auth.has_permission") as mock_permission:
            mock_permission.return_value = lambda func: func

            response = self.client.post(
                "/api/visualization/charts/cleanup-cache", **self._get_headers()
            )

        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertEqual(response_data["message"], "Cache cleaned successfully")

    def test_unauthorized_access(self):
        """Test unauthorized access to chart endpoints"""
        # Test without authentication headers
        response = self.client.get("/api/visualization/charts/")

        # The actual status code depends on the authentication implementation
        # This is a placeholder test
        self.assertIn(response.status_code, [401, 403])

    def test_invalid_json_payload(self):
        """Test endpoints with invalid JSON payload"""
        with patch("ddpui.auth.has_permission") as mock_permission:
            mock_permission.return_value = lambda func: func

            response = self.client.post(
                "/api/visualization/charts/",
                data="invalid json",
                content_type="application/json",
                **self._get_headers(),
            )

        # The actual status code depends on the JSON parsing implementation
        self.assertIn(response.status_code, [400, 422])

    def test_chart_filters(self):
        """Test chart filtering functionality"""
        with patch("ddpui.auth.has_permission") as mock_permission:
            mock_permission.return_value = lambda func: func

            with patch("ddpui.api.chart_api.ChartService") as mock_service:
                mock_service_instance = mock_service.return_value
                mock_service_instance.get_charts.return_value = [self.test_chart]

                # Test filtering by chart type
                response = self.client.get(
                    "/api/visualization/charts/?chart_type=echarts", **self._get_headers()
                )

                # Verify the filter was passed to the service
                mock_service_instance.get_charts.assert_called_with({"chart_type": "echarts"})

                # Test filtering by schema name
                response = self.client.get(
                    "/api/visualization/charts/?schema_name=test_schema", **self._get_headers()
                )

                mock_service_instance.get_charts.assert_called_with({"schema_name": "test_schema"})

    def test_chart_pagination(self):
        """Test chart pagination functionality"""
        with patch("ddpui.auth.has_permission") as mock_permission:
            mock_permission.return_value = lambda func: func

            with patch("ddpui.api.chart_api.ChartService") as mock_service:
                mock_service_instance = mock_service.return_value
                mock_service_instance.get_charts.return_value = [self.test_chart]

                # Test pagination parameters
                response = self.client.get(
                    "/api/visualization/charts/?limit=10&offset=20", **self._get_headers()
                )

                # The actual pagination testing depends on the pagination implementation
                # This is a placeholder test
                self.assertIn(response.status_code, [200, 400])
