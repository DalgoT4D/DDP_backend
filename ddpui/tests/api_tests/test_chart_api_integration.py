"""
Integration tests for Chart API endpoints
"""
import json
import os
from django.test import TestCase, Client
from django.contrib.auth.models import User
from django.urls import reverse
from rest_framework.test import APITestCase
from rest_framework import status
from rest_framework_simplejwt.tokens import AccessToken

from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.chart import Chart
from ddpui.models.role_based_access import Role, Permission, RolePermission
from ddpui.auth import CustomTokenObtainSerializer
from ddpui.utils.redis_client import RedisClient


class ChartAPIIntegrationTest(TestCase):
    """Integration tests for Chart API"""

    def setUp(self):
        """Set up test fixtures"""
        # Create test user
        self.user = User.objects.create_user(
            username="testuser@example.com", email="testuser@example.com", password="testpass123"
        )

        # Create test organization
        self.org = Org.objects.create(name="Test Org", slug="test-org")

        # Create test role with permissions
        self.role = Role.objects.create(slug="test-role", name="Test Role")

        # Create chart permissions
        chart_permissions = [
            "can_view_chart",
            "can_create_chart",
            "can_edit_chart",
            "can_delete_chart",
        ]

        for perm_slug in chart_permissions:
            permission, created = Permission.objects.get_or_create(
                slug=perm_slug, defaults={"name": perm_slug}
            )
            RolePermission.objects.create(role=self.role, permission=permission)

        # Create test org user
        self.orguser = OrgUser.objects.create(user=self.user, org=self.org, new_role=self.role)

        # Create test warehouse
        self.warehouse = OrgWarehouse.objects.create(
            org=self.org,
            wtype="postgres",
            name="test-warehouse",
            credentials='{"host": "localhost", "port": 5432, "database": "ddp_backend", "username": "postgres", "password": "postgres"}',
        )

        # Create JWT token using Django Ninja auth system
        self.access_token = CustomTokenObtainSerializer.get_token(self.user)

        # Set up client with JWT authentication
        self.client = Client()
        self.client.defaults["HTTP_AUTHORIZATION"] = f"Bearer {str(self.access_token)}"
        self.client.defaults["HTTP_X_DALGO_ORG"] = self.org.slug

        # Test chart data
        self.chart_data = {
            "title": "Test Chart",
            "description": "Test Description",
            "chart_type": "bar",
            "schema_name": "public",
            "table": "test_table",
            "config": {
                "chartType": "bar",
                "computation_type": "raw",
                "xAxis": "date",
                "yAxis": "value",
            },
            "is_public": False,
        }

    def test_create_chart_success(self):
        """Test successful chart creation"""
        response = self.client.post(
            "/api/charts/",
            json.dumps(self.chart_data),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertIn("data", response_data)
        self.assertEqual(response_data["data"]["title"], "Test Chart")

    def test_create_chart_without_permission(self):
        """Test chart creation without permission"""
        # Remove permissions
        RolePermission.objects.filter(role=self.role).delete()

        response = self.client.post(
            "/api/charts/",
            json.dumps(self.chart_data),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 403)

    def test_create_chart_invalid_data(self):
        """Test chart creation with invalid data"""
        invalid_data = self.chart_data.copy()
        invalid_data["config"]["chartType"] = "invalid_type"

        response = self.client.post(
            "/api/charts/", json.dumps(invalid_data), content_type="application/json"
        )

        self.assertEqual(response.status_code, 400)
        response_data = json.loads(response.content)
        self.assertFalse(response_data["success"])

    def test_create_chart_missing_required_fields(self):
        """Test chart creation with missing required fields"""
        invalid_data = self.chart_data.copy()
        del invalid_data["config"]["computation_type"]

        response = self.client.post(
            "/api/charts/", json.dumps(invalid_data), content_type="application/json"
        )

        self.assertEqual(response.status_code, 400)
        response_data = json.loads(response.content)
        self.assertFalse(response_data["success"])

    def test_create_chart_sql_injection_prevention(self):
        """Test SQL injection prevention in chart creation"""
        malicious_data = self.chart_data.copy()
        malicious_data["config"]["xAxis"] = "date; DROP TABLE users; --"

        response = self.client.post(
            "/api/charts/",
            json.dumps(malicious_data),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        response_data = json.loads(response.content)
        self.assertFalse(response_data["success"])

    def test_get_charts_success(self):
        """Test successful chart retrieval"""
        # Create test chart
        chart = Chart.create_chart(
            org=self.org,
            created_by=self.orguser,
            title="Test Chart",
            description="Test Description",
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            config=self.chart_data["config"],
        )

        response = self.client.get("/api/charts/")

        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.content)
        self.assertEqual(len(response_data["items"]), 1)
        self.assertEqual(response_data["items"][0]["title"], "Test Chart")

    def test_get_charts_with_filters(self):
        """Test chart retrieval with filters"""
        # Create test charts
        chart1 = Chart.create_chart(
            org=self.org,
            created_by=self.orguser,
            title="Bar Chart",
            description="Bar Description",
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            config=self.chart_data["config"],
        )

        chart2 = Chart.create_chart(
            org=self.org,
            created_by=self.orguser,
            title="Line Chart",
            description="Line Description",
            chart_type="line",
            schema_name="public",
            table_name="test_table",
            config=self.chart_data["config"],
        )

        # Test filtering by chart_type
        response = self.client.get("/api/charts/?chart_type=bar")

        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.content)
        self.assertEqual(len(response_data["items"]), 1)
        self.assertEqual(response_data["items"][0]["chart_type"], "bar")

    def test_get_charts_pagination(self):
        """Test chart retrieval with pagination"""
        # Create multiple charts
        for i in range(5):
            Chart.create_chart(
                org=self.org,
                created_by=self.orguser,
                title=f"Chart {i}",
                description=f"Description {i}",
                chart_type="bar",
                schema_name="public",
                table_name="test_table",
                config=self.chart_data["config"],
            )

        # Test pagination
        response = self.client.get("/api/charts/?limit=2&offset=0")

        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.content)
        self.assertEqual(len(response_data["items"]), 2)
        self.assertEqual(response_data["count"], 5)

    def test_get_single_chart_success(self):
        """Test successful single chart retrieval"""
        chart = Chart.create_chart(
            org=self.org,
            created_by=self.orguser,
            title="Test Chart",
            description="Test Description",
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            config=self.chart_data["config"],
        )

        response = self.client.get(f"/api/charts/{chart.id}")

        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertEqual(response_data["data"]["title"], "Test Chart")

    def test_get_single_chart_not_found(self):
        """Test single chart retrieval when chart doesn't exist"""
        response = self.client.get("/api/charts/999")

        self.assertEqual(response.status_code, 400)
        response_data = json.loads(response.content)
        self.assertFalse(response_data["success"])

    def test_update_chart_success(self):
        """Test successful chart update"""
        chart = Chart.create_chart(
            org=self.org,
            created_by=self.orguser,
            title="Original Title",
            description="Original Description",
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            config=self.chart_data["config"],
        )

        update_data = {"title": "Updated Title", "description": "Updated Description"}

        response = self.client.put(
            f"/api/charts/{chart.id}",
            json.dumps(update_data),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertEqual(response_data["data"]["title"], "Updated Title")

    def test_update_chart_not_found(self):
        """Test chart update when chart doesn't exist"""
        update_data = {"title": "Updated Title"}

        response = self.client.put(
            "/api/charts/999",
            json.dumps(update_data),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        response_data = json.loads(response.content)
        self.assertFalse(response_data["success"])

    def test_delete_chart_success(self):
        """Test successful chart deletion"""
        chart = Chart.create_chart(
            org=self.org,
            created_by=self.orguser,
            title="Test Chart",
            description="Test Description",
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            config=self.chart_data["config"],
        )

        response = self.client.delete(f"/api/charts/{chart.id}")

        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertFalse(Chart.objects.filter(id=chart.id).exists())

    def test_delete_chart_not_found(self):
        """Test chart deletion when chart doesn't exist"""
        response = self.client.delete("/api/charts/999")

        self.assertEqual(response.status_code, 400)
        response_data = json.loads(response.content)
        self.assertFalse(response_data["success"])

    def test_generate_chart_data_success(self):
        """Test successful chart data generation"""
        # Create test table data
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS test_table (
                    date DATE,
                    value INTEGER
                );
            """
            )
            cursor.execute(
                """
                INSERT INTO test_table (date, value) VALUES
                ('2024-01-01', 100),
                ('2024-01-02', 200),
                ('2024-01-03', 150)
                ON CONFLICT DO NOTHING;
            """
            )

        generate_data = {
            "chart_type": "bar",
            "computation_type": "raw",
            "schema_name": "public",
            "table_name": "test_table",
            "xaxis": "date",
            "yaxis": "value",
            "limit": 10,
        }

        response = self.client.post("/api/charts/generate", generate_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data["success"])
        self.assertIn("data", response.data)
        self.assertIn("chart_config", response.data["data"])
        self.assertIn("raw_data", response.data["data"])

    def test_generate_chart_data_invalid_table(self):
        """Test chart data generation with invalid table"""
        generate_data = {
            "chart_type": "bar",
            "computation_type": "raw",
            "schema_name": "public",
            "table_name": "nonexistent_table",
            "xaxis": "date",
            "yaxis": "value",
        }

        response = self.client.post(
            "/api/charts/generate",
            json.dumps(generate_data),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        response_data = json.loads(response.content)
        self.assertFalse(response_data["success"])

    def test_generate_chart_data_sql_injection(self):
        """Test SQL injection prevention in chart data generation"""
        generate_data = {
            "chart_type": "bar",
            "computation_type": "raw",
            "schema_name": "public",
            "table_name": "test_table",
            "xaxis": "date; DROP TABLE users; --",
            "yaxis": "value",
        }

        response = self.client.post(
            "/api/charts/generate",
            json.dumps(generate_data),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        response_data = json.loads(response.content)
        self.assertFalse(response_data["success"])

    def test_toggle_favorite_success(self):
        """Test successful favorite toggle"""
        chart = Chart.create_chart(
            org=self.org,
            created_by=self.orguser,
            title="Test Chart",
            description="Test Description",
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            config=self.chart_data["config"],
        )

        response = self.client.post(f"/api/charts/{chart.id}/favorite")

        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])
        self.assertTrue(response_data["data"]["is_favorite"])

    def test_toggle_favorite_not_found(self):
        """Test favorite toggle when chart doesn't exist"""
        response = self.client.post("/api/charts/999/favorite")

        self.assertEqual(response.status_code, 500)
        response_data = json.loads(response.content)
        self.assertFalse(response_data["success"])

    def test_cleanup_cache_success(self):
        """Test successful cache cleanup"""
        response = self.client.post("/api/charts/cleanup-cache")

        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.content)
        self.assertTrue(response_data["success"])

    def test_unauthorized_access(self):
        """Test unauthorized access to chart endpoints"""
        # Remove authentication
        self.client.defaults.pop("HTTP_AUTHORIZATION", None)

        response = self.client.get("/api/charts/")

        self.assertEqual(response.status_code, 401)

    def test_cross_org_access_prevention(self):
        """Test prevention of cross-organization access"""
        # Create another organization
        other_org = Org.objects.create(name="Other Org", slug="other-org")

        other_user = User.objects.create_user(
            username="otheruser@example.com", email="otheruser@example.com", password="testpass123"
        )

        other_orguser = OrgUser.objects.create(user=other_user, org=other_org, new_role=self.role)

        # Create chart in other org
        other_chart = Chart.create_chart(
            org=other_org,
            created_by=other_orguser,
            title="Other Chart",
            description="Other Description",
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            config=self.chart_data["config"],
        )

        # Try to access other org's chart
        response = self.client.get(f"/api/charts/{other_chart.id}")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertFalse(response.data["success"])

    def test_malformed_json_request(self):
        """Test handling of malformed JSON requests"""
        response = self.client.post(
            "/api/charts/", '{"invalid": json}', content_type="application/json"
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_large_payload_handling(self):
        """Test handling of large payloads"""
        large_data = self.chart_data.copy()
        large_data["description"] = "x" * 10000  # Very long description

        response = self.client.post("/api/charts/", large_data, format="json")

        # Should still work but description should be truncated
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data["success"])
        self.assertLessEqual(len(response.data["data"]["description"]), 1000)

    def test_concurrent_chart_creation(self):
        """Test concurrent chart creation"""
        import threading
        import time

        results = []

        def create_chart(title):
            data = self.chart_data.copy()
            data["title"] = title
            response = self.client.post("/api/charts/", data, format="json")
            results.append(response.status_code)

        # Create multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=create_chart, args=(f"Chart {i}",))
            threads.append(thread)

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # All requests should succeed
        self.assertEqual(len(results), 5)
        self.assertTrue(all(status == 200 for status in results))

    def tearDown(self):
        """Clean up after tests"""
        # Clean up test table
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS test_table;")
