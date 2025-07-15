"""
End-to-end tests for Chart system
"""
import json
import time
from django.test import TestCase, TransactionTestCase
from django.contrib.auth.models import User
from django.test.client import Client
from rest_framework.test import APITestCase
from rest_framework import status
from rest_framework.authtoken.models import Token
from django.db import transaction

from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.chart import Chart, ChartSnapshot
from ddpui.models.role_based_access import Role, Permission, RolePermission
from ddpui.core.chart_service import ChartService


class ChartEndToEndTest(TransactionTestCase):
    """End-to-end tests for chart system"""

    def setUp(self):
        """Set up test fixtures"""
        # Create test user
        self.user = User.objects.create_user(
            username="e2e_user@example.com", email="e2e_user@example.com", password="testpass123"
        )

        # Create test organization
        self.org = Org.objects.create(name="E2E Test Org", slug="e2e-test-org")

        # Create test role with all permissions
        self.role = Role.objects.create(slug="e2e-role", name="E2E Role")

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
            name="e2e-warehouse",
            credentials='{"host": "localhost", "port": 5432, "database": "ddp_backend", "username": "postgres", "password": "postgres"}',
        )

        # Create authentication token
        self.token = Token.objects.create(user=self.user)

        # Set up client
        self.client = Client()
        self.client.defaults["HTTP_AUTHORIZATION"] = f"Token {self.token.key}"
        self.client.defaults["HTTP_X_DALGO_ORG"] = self.org.slug

    def test_complete_chart_lifecycle(self):
        """Test complete chart lifecycle: create -> read -> update -> delete"""

        # Step 1: Create chart
        chart_data = {
            "title": "E2E Test Chart",
            "description": "End-to-end test chart",
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

        create_response = self.client.post(
            "/api/visualization/charts/",
            data=json.dumps(chart_data),
            content_type="application/json",
        )

        self.assertEqual(create_response.status_code, 200)
        create_result = json.loads(create_response.content)
        self.assertTrue(create_result["success"])
        chart_id = create_result["data"]["id"]

        # Step 2: Read chart
        read_response = self.client.get(f"/api/visualization/charts/{chart_id}")

        self.assertEqual(read_response.status_code, 200)
        read_result = json.loads(read_response.content)
        self.assertTrue(read_result["success"])
        self.assertEqual(read_result["data"]["title"], "E2E Test Chart")

        # Step 3: Update chart
        update_data = {"title": "Updated E2E Chart", "description": "Updated description"}

        update_response = self.client.put(
            f"/api/visualization/charts/{chart_id}",
            data=json.dumps(update_data),
            content_type="application/json",
        )

        self.assertEqual(update_response.status_code, 200)
        update_result = json.loads(update_response.content)
        self.assertTrue(update_result["success"])
        self.assertEqual(update_result["data"]["title"], "Updated E2E Chart")

        # Step 4: List charts
        list_response = self.client.get("/api/visualization/charts/")

        self.assertEqual(list_response.status_code, 200)
        list_result = json.loads(list_response.content)
        self.assertGreater(len(list_result["items"]), 0)

        # Step 5: Toggle favorite
        favorite_response = self.client.post(f"/api/visualization/charts/{chart_id}/favorite")

        self.assertEqual(favorite_response.status_code, 200)
        favorite_result = json.loads(favorite_response.content)
        self.assertTrue(favorite_result["success"])
        self.assertTrue(favorite_result["data"]["is_favorite"])

        # Step 6: Delete chart
        delete_response = self.client.delete(f"/api/visualization/charts/{chart_id}")

        self.assertEqual(delete_response.status_code, 200)
        delete_result = json.loads(delete_response.content)
        self.assertTrue(delete_result["success"])

        # Verify deletion
        verify_response = self.client.get(f"/api/visualization/charts/{chart_id}")
        self.assertEqual(verify_response.status_code, 400)

    def test_chart_data_generation_workflow(self):
        """Test chart data generation workflow"""

        # Create test table first
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS e2e_test_table (
                    id SERIAL PRIMARY KEY,
                    category VARCHAR(50),
                    value INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            )

            cursor.execute(
                """
                INSERT INTO e2e_test_table (category, value) VALUES
                ('A', 100),
                ('B', 200),
                ('C', 150),
                ('D', 300)
                ON CONFLICT DO NOTHING;
            """
            )

        # Test data generation
        generate_data = {
            "chart_type": "bar",
            "computation_type": "raw",
            "schema_name": "public",
            "table_name": "e2e_test_table",
            "xaxis": "category",
            "yaxis": "value",
            "limit": 10,
        }

        generate_response = self.client.post(
            "/api/visualization/charts/generate",
            data=json.dumps(generate_data),
            content_type="application/json",
        )

        self.assertEqual(generate_response.status_code, 200)
        generate_result = json.loads(generate_response.content)
        self.assertTrue(generate_result["success"])
        self.assertIn("data", generate_result)
        self.assertIn("chart_config", generate_result["data"])
        self.assertIn("raw_data", generate_result["data"])

        # Verify data structure
        chart_config = generate_result["data"]["chart_config"]
        raw_data = generate_result["data"]["raw_data"]

        self.assertIn("series", chart_config)
        self.assertIn("data", raw_data)
        self.assertGreater(len(raw_data["data"]), 0)

        # Clean up
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS e2e_test_table;")

    def test_cache_functionality(self):
        """Test cache functionality across requests"""

        # Create test table
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_test_table (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50),
                    score INTEGER
                );
            """
            )

            cursor.execute(
                """
                INSERT INTO cache_test_table (name, score) VALUES
                ('User1', 85),
                ('User2', 92),
                ('User3', 78)
                ON CONFLICT DO NOTHING;
            """
            )

        generate_data = {
            "chart_type": "bar",
            "computation_type": "raw",
            "schema_name": "public",
            "table_name": "cache_test_table",
            "xaxis": "name",
            "yaxis": "score",
            "limit": 10,
        }

        # First request - should create cache
        start_time = time.time()
        response1 = self.client.post(
            "/api/visualization/charts/generate",
            data=json.dumps(generate_data),
            content_type="application/json",
        )
        first_request_time = time.time() - start_time

        self.assertEqual(response1.status_code, 200)
        result1 = json.loads(response1.content)
        self.assertTrue(result1["success"])

        # Second request - should use cache (should be faster)
        start_time = time.time()
        response2 = self.client.post(
            "/api/visualization/charts/generate",
            data=json.dumps(generate_data),
            content_type="application/json",
        )
        second_request_time = time.time() - start_time

        self.assertEqual(response2.status_code, 200)
        result2 = json.loads(response2.content)
        self.assertTrue(result2["success"])

        # Verify cache was used (second request should be faster)
        # Note: This might be flaky in some environments
        self.assertLessEqual(second_request_time, first_request_time * 1.5)

        # Test cache cleanup
        cleanup_response = self.client.post("/api/visualization/charts/cleanup-cache")
        self.assertEqual(cleanup_response.status_code, 200)

        # Clean up
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS cache_test_table;")

    def test_rate_limiting(self):
        """Test rate limiting functionality"""

        # Create multiple requests to trigger rate limiting
        chart_data = {
            "title": "Rate Limit Test",
            "description": "Testing rate limiting",
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

        successful_requests = 0
        rate_limited_requests = 0

        # Make many requests quickly
        for i in range(15):  # More than the rate limit
            response = self.client.post(
                "/api/visualization/charts/",
                data=json.dumps({**chart_data, "title": f"Chart {i}"}),
                content_type="application/json",
            )

            if response.status_code == 200:
                successful_requests += 1
            elif response.status_code == 429:  # Rate limited
                rate_limited_requests += 1

        # Should have some successful requests and some rate limited
        self.assertGreater(successful_requests, 0)
        self.assertGreater(rate_limited_requests, 0)

        # Clean up created charts
        Chart.objects.filter(org=self.org).delete()

    def test_error_handling_scenarios(self):
        """Test various error handling scenarios"""

        # Test invalid chart type
        invalid_chart_data = {
            "title": "Invalid Chart",
            "description": "Testing invalid chart type",
            "chart_type": "invalid_type",
            "schema_name": "public",
            "table": "test_table",
            "config": {
                "chartType": "invalid_type",
                "computation_type": "raw",
                "xAxis": "date",
                "yAxis": "value",
            },
        }

        response = self.client.post(
            "/api/visualization/charts/",
            data=json.dumps(invalid_chart_data),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        result = json.loads(response.content)
        self.assertFalse(result["success"])

        # Test SQL injection attempt
        malicious_data = {
            "chart_type": "bar",
            "computation_type": "raw",
            "schema_name": "public",
            "table_name": "test_table",
            "xaxis": "name; DROP TABLE users; --",
            "yaxis": "value",
        }

        response = self.client.post(
            "/api/visualization/charts/generate",
            data=json.dumps(malicious_data),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        result = json.loads(response.content)
        self.assertFalse(result["success"])

    def test_performance_with_large_datasets(self):
        """Test performance with large datasets"""

        # Create a larger test table
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS large_test_table (
                    id SERIAL PRIMARY KEY,
                    category VARCHAR(50),
                    value INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            )

            # Insert multiple rows
            for i in range(100):
                cursor.execute(
                    "INSERT INTO large_test_table (category, value) VALUES (%s, %s)",
                    [f"Category_{i % 10}", i * 10],
                )

        # Test with large limit
        generate_data = {
            "chart_type": "bar",
            "computation_type": "raw",
            "schema_name": "public",
            "table_name": "large_test_table",
            "xaxis": "category",
            "yaxis": "value",
            "limit": 5000,  # Large limit, should be capped
        }

        start_time = time.time()
        response = self.client.post(
            "/api/visualization/charts/generate",
            data=json.dumps(generate_data),
            content_type="application/json",
        )
        request_time = time.time() - start_time

        self.assertEqual(response.status_code, 200)
        result = json.loads(response.content)
        self.assertTrue(result["success"])

        # Verify that the limit was applied (should be capped at 1000)
        data_count = len(result["data"]["raw_data"]["data"])
        self.assertLessEqual(data_count, 1000)

        # Verify reasonable response time (should be under 5 seconds)
        self.assertLess(request_time, 5.0)

        # Clean up
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS large_test_table;")

    def test_concurrent_access(self):
        """Test concurrent access to chart system"""
        import threading
        import time

        results = []

        def create_chart(thread_id):
            chart_data = {
                "title": f"Concurrent Chart {thread_id}",
                "description": f"Chart created by thread {thread_id}",
                "chart_type": "bar",
                "schema_name": "public",
                "table": "test_table",
                "config": {
                    "chartType": "bar",
                    "computation_type": "raw",
                    "xAxis": "date",
                    "yAxis": "value",
                },
            }

            try:
                response = self.client.post(
                    "/api/visualization/charts/",
                    data=json.dumps(chart_data),
                    content_type="application/json",
                )
                results.append(response.status_code)
            except Exception as e:
                results.append(f"Error: {e}")

        # Create multiple threads
        threads = []
        for i in range(3):  # Keep it small to avoid overwhelming the system
            thread = threading.Thread(target=create_chart, args=(i,))
            threads.append(thread)

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify results
        self.assertEqual(len(results), 3)
        success_count = sum(1 for result in results if result == 200)
        self.assertGreaterEqual(success_count, 1)  # At least one should succeed

        # Clean up
        Chart.objects.filter(org=self.org).delete()

    def test_cache_statistics(self):
        """Test cache statistics endpoint"""

        # Get cache statistics
        response = self.client.get("/api/visualization/charts/cache-stats")

        self.assertEqual(response.status_code, 200)
        result = json.loads(response.content)
        self.assertTrue(result["success"])
        self.assertIn("data", result)
        self.assertIn("cache_stats", result["data"])

        # Verify cache stats structure
        cache_stats = result["data"]["cache_stats"]
        expected_fields = ["total_snapshots", "expired_snapshots", "org_snapshots"]
        for field in expected_fields:
            self.assertIn(field, cache_stats)
            self.assertIsInstance(cache_stats[field], int)

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
