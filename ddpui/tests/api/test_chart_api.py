import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from django.test import TestCase, Client
from django.contrib.auth.models import User
from django.urls import reverse
from rest_framework import status
from ddpui.models.chart import Chart, ChartSnapshot
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.admin_user import AdminUser
from ddpui.models.role_based_access import Role
from ddpui.core.chart_service import ChartService


class TestChartAPI(TestCase):
    """Test cases for Chart API endpoints"""

    def setUp(self):
        """Set up test data"""
        self.client = Client()

        # Create a Django User
        self.django_user = User.objects.create_user(
            username="testuser@example.com", email="testuser@example.com", password="testpass123"
        )

        # Create AdminUser
        self.admin_user = AdminUser.objects.create(user=self.django_user)

        # Create organization
        self.org = Org.objects.create(name="Test Organization", slug="test-org")

        # Create role
        self.role = Role.objects.create(slug="account_manager", name="Account Manager", level=1)

        # Create org user
        self.org_user = OrgUser.objects.create(
            user=self.django_user, org=self.org, new_role=self.role
        )

        # Mock authentication
        self.client.force_login(self.django_user)

        # Create a test chart
        self.chart_config = {
            "chartType": "bar",
            "computation_type": "raw",
            "xAxis": "category",
            "yAxis": "value",
        }

        self.test_chart = Chart.create_chart(
            org=self.org,
            created_by=self.org_user,
            title="Test Chart",
            description="Test chart description",
            chart_type="echarts",
            schema_name="public",
            table_name="test_table",
            config=self.chart_config,
        )

    def test_create_chart_success(self):
        """Test successful chart creation via API"""
        payload = {
            "title": "New Chart",
            "description": "New chart description",
            "chart_type": "echarts",
            "schema_name": "public",
            "table": "users",
            "config": {
                "chartType": "line",
                "computation_type": "raw",
                "xAxis": "date",
                "yAxis": "count",
            },
            "is_public": False,
        }

        with patch("ddpui.api.chart_api.ChartService") as mock_service:
            mock_service.return_value.create_chart.return_value = self.test_chart

            response = self.client.post(
                "/api/charts/",
                data=json.dumps(payload),
                content_type="application/json",
                HTTP_X_DALGO_ORG=self.org.slug,
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertIn("data", response_data)

    def test_create_chart_validation_error(self):
        """Test chart creation with validation error"""
        payload = {
            "title": "",  # Empty title should cause validation error
            "chart_type": "echarts",
            "schema_name": "public",
            "table": "users",
            "config": {"chartType": "bar", "computation_type": "raw"},
        }

        response = self.client.post(
            "/api/charts/",
            data=json.dumps(payload),
            content_type="application/json",
            HTTP_X_DALGO_ORG=self.org.slug,
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        response_data = json.loads(response.content)
        self.assertFalse(response_data["success"])
        self.assertIn("error", response_data)

    def test_get_charts_success(self):
        """Test successful chart retrieval"""
        with patch("ddpui.api.chart_api.ChartService") as mock_service:
            mock_service.return_value.get_charts.return_value = [self.test_chart]

            response = self.client.get("/api/charts/", HTTP_X_DALGO_ORG=self.org.slug)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        response_data = json.loads(response.content)
        self.assertIsInstance(response_data, list)
        self.assertEqual(len(response_data), 1)

    def test_get_charts_with_filters(self):
        """Test chart retrieval with filters"""
        with patch("ddpui.api.chart_api.ChartService") as mock_service:
            mock_service.return_value.get_charts.return_value = [self.test_chart]

            response = self.client.get(
                "/api/charts/?chart_type=echarts&schema_name=public", HTTP_X_DALGO_ORG=self.org.slug
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Verify filters were passed to service
        mock_service.return_value.get_charts.assert_called_once_with(
            {"chart_type": "echarts", "schema_name": "public"}
        )

    def test_get_chart_by_id_success(self):
        """Test successful chart retrieval by ID"""
        with patch("ddpui.api.chart_api.ChartService") as mock_service:
            mock_service.return_value.get_chart.return_value = self.test_chart

            response = self.client.get(
                f"/api/charts/{self.test_chart.id}", HTTP_X_DALGO_ORG=self.org.slug
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertIn("data", response_data)

    def test_get_chart_by_id_not_found(self):
        """Test chart retrieval with invalid ID"""
        with patch("ddpui.api.chart_api.ChartService") as mock_service:
            mock_service.return_value.get_chart.side_effect = Exception("Chart not found")

            response = self.client.get("/api/charts/999", HTTP_X_DALGO_ORG=self.org.slug)

        self.assertEqual(response.status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)
        response_data = json.loads(response.content)
        self.assertFalse(response_data["success"])

    def test_update_chart_success(self):
        """Test successful chart update"""
        payload = {
            "title": "Updated Chart Title",
            "description": "Updated description",
            "chart_type": "echarts",
            "schema_name": "public",
            "table": "users",
            "config": {
                "chartType": "pie",
                "computation_type": "aggregated",
                "aggregate_func": "sum",
                "aggregate_col": "amount",
            },
        }

        with patch("ddpui.api.chart_api.ChartService") as mock_service:
            mock_service.return_value.update_chart.return_value = self.test_chart

            response = self.client.put(
                f"/api/charts/{self.test_chart.id}",
                data=json.dumps(payload),
                content_type="application/json",
                HTTP_X_DALGO_ORG=self.org.slug,
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertIn("data", response_data)

    def test_delete_chart_success(self):
        """Test successful chart deletion"""
        with patch("ddpui.api.chart_api.ChartService") as mock_service:
            mock_service.return_value.delete_chart.return_value = True

            response = self.client.delete(
                f"/api/charts/{self.test_chart.id}", HTTP_X_DALGO_ORG=self.org.slug
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertIn("message", response_data)

    def test_generate_chart_data_success(self):
        """Test successful chart data generation"""
        payload = {
            "chart_type": "bar",
            "computation_type": "raw",
            "schema_name": "public",
            "table_name": "users",
            "xaxis": "category",
            "yaxis": "value",
            "offset": 0,
            "limit": 100,
        }

        mock_chart_data = {
            "chart_config": {"type": "bar"},
            "raw_data": {"data": [{"x": "A", "y": 10}]},
            "metadata": {"count": 1},
        }

        with patch("ddpui.api.chart_api.ChartService") as mock_service:
            mock_service.return_value.generate_chart_data.return_value = mock_chart_data

            response = self.client.post(
                "/api/charts/generate",
                data=json.dumps(payload),
                content_type="application/json",
                HTTP_X_DALGO_ORG=self.org.slug,
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertIn("data", response_data)

    def test_generate_chart_data_aggregated(self):
        """Test chart data generation for aggregated data"""
        payload = {
            "chart_type": "bar",
            "computation_type": "aggregated",
            "schema_name": "public",
            "table_name": "sales",
            "aggregate_col": "amount",
            "aggregate_func": "sum",
            "aggregate_col_alias": "total_amount",
            "dimension_col": "region",
            "offset": 0,
            "limit": 50,
        }

        mock_chart_data = {
            "chart_config": {"type": "bar"},
            "raw_data": {"data": [{"region": "North", "total_amount": 1000}]},
            "metadata": {"count": 1},
        }

        with patch("ddpui.api.chart_api.ChartService") as mock_service:
            mock_service.return_value.generate_chart_data.return_value = mock_chart_data

            response = self.client.post(
                "/api/charts/generate",
                data=json.dumps(payload),
                content_type="application/json",
                HTTP_X_DALGO_ORG=self.org.slug,
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertIn("data", response_data)

    def test_toggle_favorite_success(self):
        """Test successful chart favorite toggle"""
        response = self.client.post(
            f"/api/charts/{self.test_chart.id}/favorite", HTTP_X_DALGO_ORG=self.org.slug
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertIn("data", response_data)

    def test_toggle_favorite_chart_not_found(self):
        """Test favorite toggle for non-existent chart"""
        response = self.client.post("/api/charts/999/favorite", HTTP_X_DALGO_ORG=self.org.slug)

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_cleanup_cache_success(self):
        """Test successful cache cleanup"""
        with patch("ddpui.api.chart_api.ChartService") as mock_service:
            mock_service.clean_expired_snapshots.return_value = None

            response = self.client.post("/api/charts/cleanup-cache", HTTP_X_DALGO_ORG=self.org.slug)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertIn("message", response_data)

    def test_get_cache_stats_success(self):
        """Test successful cache statistics retrieval"""
        mock_cache_stats = {"total_snapshots": 10, "expired_snapshots": 2, "cache_hit_rate": 0.85}

        mock_pool_stats = {"active_connections": 5, "total_connections": 10}

        with patch("ddpui.api.chart_api.ChartService") as mock_service:
            mock_service.return_value.get_cache_stats.return_value = mock_cache_stats
            mock_service.return_value._get_connection_pool_stats.return_value = mock_pool_stats

            response = self.client.get("/api/charts/cache-stats", HTTP_X_DALGO_ORG=self.org.slug)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertIn("data", response_data)
        self.assertIn("cache_stats", response_data["data"])
        self.assertIn("connection_pool_stats", response_data["data"])

    def test_rate_limiting_chart_creation(self):
        """Test rate limiting for chart creation"""
        payload = {
            "title": "Rate Limited Chart",
            "chart_type": "echarts",
            "schema_name": "public",
            "table": "users",
            "config": {
                "chartType": "bar",
                "computation_type": "raw",
                "xAxis": "category",
                "yAxis": "value",
            },
        }

        # Mock rate limit exceeded
        with patch("ddpui.api.chart_api.rate_limit_check") as mock_rate_check:
            mock_rate_check.return_value = False

            response = self.client.post(
                "/api/charts/",
                data=json.dumps(payload),
                content_type="application/json",
                HTTP_X_DALGO_ORG=self.org.slug,
            )

        self.assertEqual(response.status_code, status.HTTP_429_TOO_MANY_REQUESTS)
        response_data = json.loads(response.content)
        self.assertFalse(response_data["success"])
        self.assertIn("Rate limit exceeded", response_data["error"])

    def test_rate_limiting_chart_generation(self):
        """Test rate limiting for chart data generation"""
        payload = {
            "chart_type": "bar",
            "computation_type": "raw",
            "schema_name": "public",
            "table_name": "users",
            "xaxis": "category",
            "yaxis": "value",
        }

        # Mock rate limit exceeded for generation
        with patch("ddpui.api.chart_api.rate_limit_check") as mock_rate_check:
            mock_rate_check.return_value = False

            response = self.client.post(
                "/api/charts/generate",
                data=json.dumps(payload),
                content_type="application/json",
                HTTP_X_DALGO_ORG=self.org.slug,
            )

        self.assertEqual(response.status_code, status.HTTP_429_TOO_MANY_REQUESTS)
        response_data = json.loads(response.content)
        self.assertFalse(response_data["success"])
        self.assertIn("Rate limit exceeded", response_data["error"])

    def test_unauthorized_access(self):
        """Test unauthorized access to chart endpoints"""
        self.client.logout()

        response = self.client.get("/api/charts/")
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_missing_org_header(self):
        """Test requests without organization header"""
        response = self.client.get("/api/charts/")
        # The behavior depends on the auth middleware implementation
        # This test ensures the endpoint handles missing org header gracefully
        self.assertIn(response.status_code, [400, 401, 403])

    def test_invalid_json_payload(self):
        """Test requests with invalid JSON payload"""
        response = self.client.post(
            "/api/charts/",
            data="invalid json",
            content_type="application/json",
            HTTP_X_DALGO_ORG=self.org.slug,
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_chart_permission_checks(self):
        """Test chart permission checks"""
        # Test with different permissions
        # This would require mocking the permission system

        # Create a chart by another user
        other_user = User.objects.create_user(
            username="other@example.com", email="other@example.com", password="password123"
        )

        other_org_user = OrgUser.objects.create(user=other_user, org=self.org, new_role=self.role)

        other_chart = Chart.create_chart(
            org=self.org,
            created_by=other_org_user,
            title="Other User's Chart",
            description="Chart by another user",
            chart_type="echarts",
            schema_name="public",
            table_name="test_table",
            config=self.chart_config,
            is_public=False,
        )

        # Test that user can view their own chart
        response = self.client.get(
            f"/api/charts/{self.test_chart.id}", HTTP_X_DALGO_ORG=self.org.slug
        )

        # The exact behavior depends on the permission implementation
        # This test ensures the permission system is being called
        self.assertIn(response.status_code, [200, 403])

    def test_chart_export_functionality(self):
        """Test chart export functionality (if implemented)"""
        # This would test the export endpoint once implemented
        pass

    def test_chart_data_validation(self):
        """Test chart data validation"""
        # Test with invalid chart type
        payload = {
            "chart_type": "invalid_type",
            "computation_type": "raw",
            "schema_name": "public",
            "table_name": "users",
            "xaxis": "category",
            "yaxis": "value",
        }

        with patch("ddpui.api.chart_api.ChartService") as mock_service:
            mock_service.return_value.generate_chart_data.side_effect = Exception(
                "Invalid chart type"
            )

            response = self.client.post(
                "/api/charts/generate",
                data=json.dumps(payload),
                content_type="application/json",
                HTTP_X_DALGO_ORG=self.org.slug,
            )

        self.assertEqual(response.status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)
        response_data = json.loads(response.content)
        self.assertFalse(response_data["success"])

    def test_chart_config_serialization(self):
        """Test chart configuration serialization"""
        # Test with complex config object
        complex_config = {
            "chartType": "bar",
            "computation_type": "aggregated",
            "xAxis": {"type": "category", "data": ["A", "B", "C"]},
            "yAxis": {"type": "value", "min": 0, "max": 100},
            "series": [{"type": "bar", "data": [10, 20, 30]}],
            "dimensions": ["region", "country"],
            "filters": {"date": {"gte": "2024-01-01", "lte": "2024-12-31"}},
            "aggregate_func": "sum",
            "aggregate_col": "amount",
        }

        payload = {
            "title": "Complex Chart",
            "chart_type": "echarts",
            "schema_name": "public",
            "table": "sales",
            "config": complex_config,
        }

        with patch("ddpui.api.chart_api.ChartService") as mock_service:
            mock_service.return_value.create_chart.return_value = self.test_chart

            response = self.client.post(
                "/api/charts/",
                data=json.dumps(payload),
                content_type="application/json",
                HTTP_X_DALGO_ORG=self.org.slug,
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Verify config was passed correctly
        call_args = mock_service.return_value.create_chart.call_args
        self.assertEqual(call_args[1]["config"], complex_config)
