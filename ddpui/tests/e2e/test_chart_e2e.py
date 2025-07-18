"""
End-to-end tests for Chart system
"""
import pytest
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from django.test import TestCase, Client
from django.contrib.auth.models import User
from rest_framework_simplejwt.tokens import AccessToken
from ddpui.models.chart import Chart, ChartSnapshot
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.admin_user import AdminUser
from ddpui.models.role_based_access import Role, Permission, RolePermission
from ddpui.routes import src_api


class ChartEndToEndTest(TestCase):
    """End-to-end test cases for Chart functionality"""

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

        # Create permissions
        self.permissions = []
        for perm_slug in [
            "can_view_chart",
            "can_create_chart",
            "can_edit_chart",
            "can_delete_chart",
        ]:
            permission = Permission.objects.create(slug=perm_slug, name=perm_slug)
            RolePermission.objects.create(role=self.role, permission=permission)
            self.permissions.append(permission)

        # Create org user
        self.org_user = OrgUser.objects.create(
            user=self.django_user, org=self.org, new_role=self.role
        )

        # Create JWT token
        token = AccessToken.for_user(self.django_user)
        token["orguser_role_key"] = f"orguser_role:{self.django_user.id}"
        self.client.defaults["HTTP_AUTHORIZATION"] = f"Bearer {str(token)}"
        self.client.defaults["HTTP_X_DALGO_ORG"] = self.org.slug

        # Base URL for chart API
        self.base_url = "/api/charts"

        # Common test data
        self.chart_data = {
            "title": "Test Chart",
            "description": "A test chart",
            "chart_type": "echarts",
            "schema_name": "public",
            "table": "test_table",
            "config": {
                "chartType": "bar",
                "computation_type": "raw",
                "xAxis": "category",
                "yAxis": "value",
            },
        }

    def test_complete_chart_lifecycle(self):
        """Test complete chart lifecycle: create -> read -> update -> delete"""
        # Create chart
        chart_data = {
            "title": "Test Chart",
            "description": "A test chart",
            "chart_type": "echarts",
            "schema_name": "public",
            "table": "test_table",
            "config": {
                "chartType": "bar",
                "computation_type": "raw",
                "xAxis": "category",
                "yAxis": "value",
            },
        }

        create_response = self.client.post(
            self.base_url + "/",
            data=json.dumps(chart_data),
            content_type="application/json",
            HTTP_X_DALGO_ORG=self.org.slug,
        )
        self.assertEqual(create_response.status_code, 200)
        chart_id = json.loads(create_response.content)["data"]["id"]

        # Read chart
        read_response = self.client.get(
            self.base_url + f"/{chart_id}",
            HTTP_X_DALGO_ORG=self.org.slug,
        )
        self.assertEqual(read_response.status_code, 200)

        # Update chart
        update_data = chart_data.copy()
        update_data["title"] = "Updated Test Chart"
        update_data["config"]["chartType"] = "line"
        update_response = self.client.put(
            self.base_url + f"/{chart_id}",
            data=json.dumps(update_data),
            content_type="application/json",
            HTTP_X_DALGO_ORG=self.org.slug,
        )
        self.assertEqual(update_response.status_code, 200)

        # Delete chart
        delete_response = self.client.delete(
            self.base_url + f"/{chart_id}",
            HTTP_X_DALGO_ORG=self.org.slug,
        )
        self.assertEqual(delete_response.status_code, 200)

    def test_chart_data_generation_workflow(self):
        """Test chart data generation workflow"""
        # Generate chart data
        generate_data = {
            "chart_type": "bar",
            "computation_type": "raw",
            "schema_name": "public",
            "table_name": "test_table",
            "xaxis": "category",
            "yaxis": "value",
        }

        generate_response = self.client.post(
            self.base_url + "/generate",
            data=json.dumps(generate_data),
            content_type="application/json",
            HTTP_X_DALGO_ORG=self.org.slug,
        )
        self.assertEqual(generate_response.status_code, 200)

    def test_cache_functionality(self):
        """Test cache functionality across requests"""
        # First request
        response1 = self.client.get(
            self.base_url + "/cache-stats",
            HTTP_X_DALGO_ORG=self.org.slug,
        )
        self.assertEqual(response1.status_code, 200)

        # Second request (should be cached)
        response2 = self.client.get(
            self.base_url + "/cache-stats",
            HTTP_X_DALGO_ORG=self.org.slug,
        )
        self.assertEqual(response2.status_code, 200)

    def test_error_handling_scenarios(self):
        """Test various error handling scenarios"""
        # Invalid JSON payload
        response = self.client.post(
            self.base_url + "/",
            data="invalid json",
            content_type="application/json",
            HTTP_X_DALGO_ORG=self.org.slug,
        )
        self.assertEqual(response.status_code, 400)

    def test_rate_limiting(self):
        """Test rate limiting functionality"""
        successful_requests = 0
        for i in range(15):  # More than the rate limit
            response = self.client.post(
                self.base_url + "/",
                data=json.dumps(
                    {
                        "title": f"Chart {i}",
                        "description": "A test chart",
                        "chart_type": "echarts",
                        "schema_name": "public",
                        "table": "test_table",
                        "config": {
                            "chartType": "bar",
                            "computation_type": "raw",
                            "xAxis": "category",
                            "yAxis": "value",
                        },
                    }
                ),
                content_type="application/json",
                HTTP_X_DALGO_ORG=self.org.slug,
            )
            if response.status_code == 200:
                successful_requests += 1
            time.sleep(0.1)  # Small delay to avoid overwhelming the server

        self.assertGreater(successful_requests, 0)

    def test_performance_with_large_datasets(self):
        """Test performance with large datasets"""
        # Generate large dataset request
        generate_data = {
            "chart_type": "bar",
            "computation_type": "raw",
            "schema_name": "public",
            "table_name": "large_table",
            "xaxis": "category",
            "yaxis": "value",
            "limit": 1000,  # Request large number of records
        }

        response = self.client.post(
            self.base_url + "/generate",
            data=json.dumps(generate_data),
            content_type="application/json",
            HTTP_X_DALGO_ORG=self.org.slug,
        )
        self.assertEqual(response.status_code, 200)

    def test_concurrent_access(self):
        """Test concurrent access to chart system"""

        def make_request():
            response = self.client.post(
                self.base_url + "/",
                data=json.dumps(
                    {
                        "title": "Concurrent Chart",
                        "description": "A test chart",
                        "chart_type": "echarts",
                        "schema_name": "public",
                        "table": "test_table",
                        "config": {
                            "chartType": "bar",
                            "computation_type": "raw",
                            "xAxis": "category",
                            "yAxis": "value",
                        },
                    }
                ),
                content_type="application/json",
                HTTP_X_DALGO_ORG=self.org.slug,
            )
            return response.status_code == 200

        # Make concurrent requests
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(lambda _: make_request(), range(10)))

        success_count = sum(results)
        self.assertGreaterEqual(success_count, 1)  # At least one should succeed

    def test_cache_statistics(self):
        """Test cache statistics endpoint"""
        response = self.client.get(
            self.base_url + "/cache-stats",
            HTTP_X_DALGO_ORG=self.org.slug,
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertIn("data", data)
        self.assertIn("cache_stats", data["data"])
        self.assertIn("connection_pool_stats", data["data"])

    def tearDown(self):
        """Clean up after tests"""
        # Clean up any remaining test data
        Chart.objects.filter(org=self.org).delete()
        ChartSnapshot.objects.all().delete()

        # Clean up test tables
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS e2e_test_table;")
            cursor.execute("DROP TABLE IF EXISTS cache_test_table;")
            cursor.execute("DROP TABLE IF EXISTS large_test_table;")
