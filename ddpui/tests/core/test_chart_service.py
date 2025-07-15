import pytest
from unittest.mock import Mock, patch, MagicMock
from django.test import TestCase
from django.core.exceptions import ValidationError
from django.utils import timezone
from ddpui.models.chart import Chart, ChartSnapshot
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.admin_user import AdminUser
from ddpui.core.chart_service import ChartService


class TestChartService(TestCase):
    """Test cases for ChartService"""

    def setUp(self):
        """Set up test data"""
        from django.contrib.auth.models import User
        from ddpui.models.role_based_access import Role

        # Create a Django User first
        self.django_user = User.objects.create_user(
            username="admin@example.com", email="admin@example.com", password="password123"
        )

        # Create AdminUser using the Django User
        self.admin_user = AdminUser.objects.create(user=self.django_user)

        self.org = Org.objects.create(name="Test Org", slug="test-org")

        # Create a test role
        self.role = Role.objects.create(slug="account_manager", name="Account Manager", level=1)

        self.org_user = OrgUser.objects.create(
            user=self.django_user, org=self.org, new_role=self.role
        )

        self.chart_service = ChartService(self.org, self.org_user)

        # Mock warehouse
        self.chart_service.warehouse = Mock()
        self.chart_service.warehouse.execute.return_value = [
            {"x": "A", "y": 10},
            {"x": "B", "y": 20},
            {"x": "C", "y": 15},
        ]

    def test_create_chart_success(self):
        """Test successful chart creation"""
        config = {
            "chartType": "bar",
            "computation_type": "raw",
            "xAxis": "category",
            "yAxis": "value",
        }

        chart = self.chart_service.create_chart(
            title="Test Chart",
            description="Test Description",
            chart_type="bar",
            schema_name="test_schema",
            table_name="test_table",
            config=config,
        )

        self.assertIsInstance(chart, Chart)
        self.assertEqual(chart.title, "Test Chart")
        self.assertEqual(chart.description, "Test Description")
        self.assertEqual(chart.chart_type, "bar")
        self.assertEqual(chart.schema_name, "test_schema")
        self.assertEqual(chart.table_name, "test_table")
        self.assertEqual(chart.config, config)
        self.assertEqual(chart.org, self.org)
        self.assertEqual(chart.created_by, self.org_user)

    def test_create_chart_invalid_config(self):
        """Test chart creation with invalid configuration"""
        invalid_config = {"chartType": "invalid_type", "computation_type": "raw"}

        with self.assertRaises(ValidationError):
            self.chart_service.create_chart(
                title="Test Chart",
                description="Test Description",
                chart_type="bar",
                schema_name="test_schema",
                table_name="test_table",
                config=invalid_config,
            )

    def test_create_chart_missing_required_fields(self):
        """Test chart creation with missing required fields"""
        invalid_config = {
            "chartType": "bar"
            # Missing computation_type
        }

        with self.assertRaises(ValidationError):
            self.chart_service.create_chart(
                title="Test Chart",
                description="Test Description",
                chart_type="bar",
                schema_name="test_schema",
                table_name="test_table",
                config=invalid_config,
            )

    def test_update_chart_success(self):
        """Test successful chart update"""
        # Create a chart first
        config = {
            "chartType": "bar",
            "computation_type": "raw",
            "xAxis": "category",
            "yAxis": "value",
        }

        chart = Chart.create_chart(
            org=self.org,
            created_by=self.org_user,
            title="Original Title",
            description="Original Description",
            chart_type="bar",
            schema_name="test_schema",
            table_name="test_table",
            config=config,
        )

        # Update the chart
        updated_chart = self.chart_service.update_chart(
            chart_id=chart.id, title="Updated Title", description="Updated Description"
        )

        self.assertEqual(updated_chart.title, "Updated Title")
        self.assertEqual(updated_chart.description, "Updated Description")
        self.assertEqual(updated_chart.chart_type, "bar")  # Unchanged

    def test_update_chart_not_found(self):
        """Test updating non-existent chart"""
        with self.assertRaises(ValidationError):
            self.chart_service.update_chart(chart_id=999, title="Updated Title")

    def test_delete_chart_success(self):
        """Test successful chart deletion"""
        # Create a chart first
        config = {
            "chartType": "bar",
            "computation_type": "raw",
            "xAxis": "category",
            "yAxis": "value",
        }

        chart = Chart.create_chart(
            org=self.org,
            created_by=self.org_user,
            title="Test Chart",
            description="Test Description",
            chart_type="bar",
            schema_name="test_schema",
            table_name="test_table",
            config=config,
        )

        # Delete the chart
        result = self.chart_service.delete_chart(chart.id)

        self.assertTrue(result)
        self.assertFalse(Chart.objects.filter(id=chart.id).exists())

    def test_delete_chart_not_found(self):
        """Test deleting non-existent chart"""
        with self.assertRaises(ValidationError):
            self.chart_service.delete_chart(999)

    def test_get_chart_success(self):
        """Test successful chart retrieval"""
        # Create a chart first
        config = {
            "chartType": "bar",
            "computation_type": "raw",
            "xAxis": "category",
            "yAxis": "value",
        }

        chart = Chart.create_chart(
            org=self.org,
            created_by=self.org_user,
            title="Test Chart",
            description="Test Description",
            chart_type="bar",
            schema_name="test_schema",
            table_name="test_table",
            config=config,
        )

        # Get the chart
        retrieved_chart = self.chart_service.get_chart(chart.id)

        self.assertEqual(retrieved_chart.id, chart.id)
        self.assertEqual(retrieved_chart.title, "Test Chart")

    def test_get_chart_not_found(self):
        """Test retrieving non-existent chart"""
        with self.assertRaises(ValidationError):
            self.chart_service.get_chart(999)

    def test_get_charts_with_filters(self):
        """Test getting charts with filters"""
        # Create test charts
        config = {
            "chartType": "bar",
            "computation_type": "raw",
            "xAxis": "category",
            "yAxis": "value",
        }

        chart1 = Chart.create_chart(
            org=self.org,
            created_by=self.org_user,
            title="Chart 1",
            description="Description 1",
            chart_type="bar",
            schema_name="schema1",
            table_name="table1",
            config=config,
        )

        chart2 = Chart.create_chart(
            org=self.org,
            created_by=self.org_user,
            title="Chart 2",
            description="Description 2",
            chart_type="nivo",
            schema_name="schema2",
            table_name="table2",
            config=config,
        )

        # Test filtering by chart_type
        bar_charts = self.chart_service.get_charts({"chart_type": "bar"})
        self.assertEqual(len(bar_charts), 1)
        self.assertEqual(bar_charts[0].id, chart1.id)

        # Test filtering by schema_name
        schema1_charts = self.chart_service.get_charts({"schema_name": "schema1"})
        self.assertEqual(len(schema1_charts), 1)
        self.assertEqual(schema1_charts[0].id, chart1.id)

    def test_generate_raw_chart_data(self):
        """Test generating raw chart data"""
        chart_data = self.chart_service.generate_chart_data(
            chart_type="bar",
            computation_type="raw",
            schema_name="test_schema",
            table_name="test_table",
            xaxis="category",
            yaxis="value",
            offset=0,
            limit=100,
        )

        self.assertIn("chart_config", chart_data)
        self.assertIn("raw_data", chart_data)
        self.assertIn("metadata", chart_data)

        # Check metadata
        metadata = chart_data["metadata"]
        self.assertEqual(metadata["chart_type"], "bar")
        self.assertEqual(metadata["computation_type"], "raw")
        self.assertEqual(metadata["schema_name"], "test_schema")
        self.assertEqual(metadata["table_name"], "test_table")

    def test_generate_aggregated_chart_data(self):
        """Test generating aggregated chart data"""
        # Mock aggregated query result
        self.chart_service.warehouse.execute.return_value = [
            {"category": "A", "total_value": 100},
            {"category": "B", "total_value": 200},
            {"category": "C", "total_value": 150},
        ]

        chart_data = self.chart_service.generate_chart_data(
            chart_type="bar",
            computation_type="aggregated",
            schema_name="test_schema",
            table_name="test_table",
            xaxis="category",
            aggregate_col="value",
            aggregate_func="sum",
            aggregate_col_alias="total_value",
            offset=0,
            limit=100,
        )

        self.assertIn("chart_config", chart_data)
        self.assertIn("raw_data", chart_data)
        self.assertIn("metadata", chart_data)

        # Check metadata
        metadata = chart_data["metadata"]
        self.assertEqual(metadata["chart_type"], "bar")
        self.assertEqual(metadata["computation_type"], "aggregated")

    def test_validate_chart_config_valid(self):
        """Test chart configuration validation with valid config"""
        valid_config = {
            "chartType": "bar",
            "computation_type": "raw",
            "xAxis": "category",
            "yAxis": "value",
        }

        # Should not raise an exception
        self.chart_service._validate_chart_config(valid_config)

    def test_validate_chart_config_missing_required_fields(self):
        """Test chart configuration validation with missing required fields"""
        invalid_config = {
            "chartType": "bar"
            # Missing computation_type
        }

        with self.assertRaises(ValidationError):
            self.chart_service._validate_chart_config(invalid_config)

    def test_validate_chart_config_invalid_chart_type(self):
        """Test chart configuration validation with invalid chart type"""
        invalid_config = {"chartType": "invalid_type", "computation_type": "raw"}

        with self.assertRaises(ValidationError):
            self.chart_service._validate_chart_config(invalid_config)

    def test_validate_chart_config_invalid_computation_type(self):
        """Test chart configuration validation with invalid computation type"""
        invalid_config = {"chartType": "bar", "computation_type": "invalid_type"}

        with self.assertRaises(ValidationError):
            self.chart_service._validate_chart_config(invalid_config)

    def test_validate_chart_config_raw_missing_axes(self):
        """Test chart configuration validation for raw data missing axes"""
        invalid_config = {
            "chartType": "bar",
            "computation_type": "raw",
            "xAxis": "category"
            # Missing yAxis
        }

        with self.assertRaises(ValidationError):
            self.chart_service._validate_chart_config(invalid_config)

    def test_validate_chart_config_aggregated_missing_func(self):
        """Test chart configuration validation for aggregated data missing function"""
        invalid_config = {
            "chartType": "bar",
            "computation_type": "aggregated",
            "xAxis": "category"
            # Missing aggregate_func
        }

        with self.assertRaises(ValidationError):
            self.chart_service._validate_chart_config(invalid_config)

    def test_transform_data_for_chart(self):
        """Test data transformation for chart visualization"""
        query_result = [{"x": "A", "y": 10}, {"x": "B", "y": 20}, {"x": "C", "y": 15}]

        transformed_data = self.chart_service._transform_data_for_chart(
            query_result, "bar", "raw", "category", "value"
        )

        self.assertIn("data", transformed_data)
        self.assertIn("x-axis", transformed_data)
        self.assertIn("y-axis", transformed_data)

        # Check data structure
        self.assertEqual(len(transformed_data["data"]), 3)
        self.assertEqual(transformed_data["x-axis"], ["A", "B", "C"])
        self.assertEqual(transformed_data["y-axis"], [10, 20, 15])

    def test_generate_echarts_config_bar_chart(self):
        """Test ECharts configuration generation for bar chart"""
        chart_data = {
            "data": [
                {"x": "A", "y": 10, "name": "A", "value": 10},
                {"x": "B", "y": 20, "name": "B", "value": 20},
            ],
            "x-axis": ["A", "B"],
            "y-axis": [10, 20],
        }

        config = self.chart_service._generate_echarts_config(chart_data, "bar", "category", "value")

        self.assertIn("tooltip", config)
        self.assertIn("legend", config)
        self.assertIn("xAxis", config)
        self.assertIn("yAxis", config)
        self.assertIn("series", config)

        # Check series configuration
        series = config["series"][0]
        self.assertEqual(series["type"], "bar")
        self.assertEqual(series["data"], [10, 20])

    def test_generate_echarts_config_pie_chart(self):
        """Test ECharts configuration generation for pie chart"""
        chart_data = {
            "data": [
                {"x": "A", "y": 10, "name": "A", "value": 10},
                {"x": "B", "y": 20, "name": "B", "value": 20},
            ],
            "x-axis": ["A", "B"],
            "y-axis": [10, 20],
        }

        config = self.chart_service._generate_echarts_config(chart_data, "pie", "category", "value")

        self.assertIn("series", config)

        # Check series configuration
        series = config["series"][0]
        self.assertEqual(series["type"], "pie")
        self.assertEqual(len(series["data"]), 2)
        self.assertEqual(series["data"][0]["name"], "A")
        self.assertEqual(series["data"][0]["value"], 10)

    @patch("ddpui.core.chart_service.ChartSnapshot")
    def test_cache_data(self, mock_snapshot):
        """Test data caching functionality"""
        cache_key = "test_cache_key"
        data = {"chart_config": {"test": "config"}, "raw_data": {"test": "data"}}

        self.chart_service._cache_data(cache_key, data)

        # Verify that ChartSnapshot.objects.create was called
        mock_snapshot.objects.create.assert_called_once()

    def test_generate_cache_key(self):
        """Test cache key generation"""
        cache_key = self.chart_service._generate_cache_key(
            "bar", "raw", "schema", "table", "x", "y"
        )

        self.assertIsInstance(cache_key, str)
        self.assertEqual(len(cache_key), 32)  # MD5 hash length

    def test_clean_expired_snapshots(self):
        """Test cleaning expired snapshots"""
        # This would normally test the cleanup functionality
        # For now, we just ensure the method exists and can be called
        ChartService.clean_expired_snapshots()

        # In a real test, we would create expired snapshots and verify they are deleted

    def test_sql_injection_prevention_in_column_names(self):
        """Test SQL injection prevention in column names"""
        # Test various SQL injection patterns
        malicious_inputs = [
            "column; DROP TABLE users",
            "column--",
            "column/*comment*/",
            "column UNION SELECT * FROM users",
            "column' OR '1'='1",
            "column'; DELETE FROM users; --",
            "column) OR 1=1--",
            "column EXEC xp_cmdshell('dir')",
            "column; sp_helpdb",
        ]

        for malicious_input in malicious_inputs:
            with self.subTest(input=malicious_input):
                self.assertFalse(self.chart_service._is_valid_column_name(malicious_input))

    def test_valid_column_names(self):
        """Test valid column names"""
        valid_inputs = [
            "column_name",
            "column123",
            "schema.table.column",
            "CamelCaseColumn",
            "under_score_column",
            "column1_with_numbers2",
            "a",
            "a1b2c3",
        ]

        for valid_input in valid_inputs:
            with self.subTest(input=valid_input):
                self.assertTrue(self.chart_service._is_valid_column_name(valid_input))

    def test_invalid_column_names(self):
        """Test invalid column names"""
        invalid_inputs = [
            "",
            None,
            "column with spaces",
            "column@domain",
            "column#hash",
            "column$dollar",
            "column%percent",
            "column&ampersand",
            "column*asterisk",
            "column+plus",
            "column=equals",
            "column?question",
            "column[bracket",
            "column]bracket",
            "column{brace",
            "column}brace",
            "column|pipe",
            "column\\backslash",
            "column:colon",
            "column;semicolon",
            "column'quote",
            'column"doublequote',
            "column<less",
            "column>greater",
            "column,comma",
            "column/slash",
            "column~tilde",
            "column`backtick",
        ]

        for invalid_input in invalid_inputs:
            with self.subTest(input=invalid_input):
                self.assertFalse(self.chart_service._is_valid_column_name(invalid_input))

    def test_string_sanitization_edge_cases(self):
        """Test string sanitization edge cases"""
        # Test null bytes
        self.assertEqual(self.chart_service._sanitize_string_input("test\x00null"), "testnull")

        # Test unicode characters
        self.assertEqual(
            self.chart_service._sanitize_string_input("test\u0000unicode"), "testunicode"
        )

        # Test length limiting
        long_string = "a" * 500
        result = self.chart_service._sanitize_string_input(long_string, 100)
        self.assertEqual(len(result), 100)

        # Test whitespace handling
        self.assertEqual(
            self.chart_service._sanitize_string_input("   \t\n  test  \t\n   "), "test"
        )

    def test_validate_chart_config_schema_object(self):
        """Test chart config validation with schema object"""

        # Mock schema object with dict() method
        class MockSchema:
            def dict(self):
                return {
                    "chartType": "bar",
                    "computation_type": "raw",
                    "xAxis": "category",
                    "yAxis": "value",
                }

        schema_obj = MockSchema()

        # Should not raise an exception
        self.chart_service._validate_chart_config(schema_obj)

    def test_validate_chart_config_with_dimensions(self):
        """Test chart config validation with dimensions"""
        # Test with string dimension
        config_with_string_dimension = {
            "chartType": "bar",
            "computation_type": "raw",
            "xAxis": "category",
            "yAxis": "value",
            "dimensions": "region",
        }

        # Should not raise an exception
        self.chart_service._validate_chart_config(config_with_string_dimension)

        # Test with list dimensions
        config_with_list_dimensions = {
            "chartType": "bar",
            "computation_type": "raw",
            "xAxis": "category",
            "yAxis": "value",
            "dimensions": ["region", "country"],
        }

        # Should not raise an exception
        self.chart_service._validate_chart_config(config_with_list_dimensions)

        # Test with invalid dimensions type
        config_with_invalid_dimensions = {
            "chartType": "bar",
            "computation_type": "raw",
            "xAxis": "category",
            "yAxis": "value",
            "dimensions": 123,  # Invalid type
        }

        with self.assertRaises(ValidationError):
            self.chart_service._validate_chart_config(config_with_invalid_dimensions)

    def test_validate_chart_config_aggregated_functions(self):
        """Test chart config validation with different aggregate functions"""
        valid_agg_functions = ["sum", "avg", "count", "min", "max", "stddev", "variance"]

        for func in valid_agg_functions:
            with self.subTest(function=func):
                config = {
                    "chartType": "bar",
                    "computation_type": "aggregated",
                    "aggregate_func": func,
                    "aggregate_col": "value",
                }

                # Should not raise an exception
                self.chart_service._validate_chart_config(config)

        # Test invalid aggregate function
        config_invalid_func = {
            "chartType": "bar",
            "computation_type": "aggregated",
            "aggregate_func": "invalid_function",
        }

        with self.assertRaises(ValidationError):
            self.chart_service._validate_chart_config(config_invalid_func)

    def test_create_chart_with_long_strings(self):
        """Test chart creation with very long strings"""
        config = {
            "chartType": "bar",
            "computation_type": "raw",
            "xAxis": "category",
            "yAxis": "value",
        }

        # Test with very long title (should be truncated)
        long_title = "a" * 300
        long_description = "b" * 1500

        chart = self.chart_service.create_chart(
            title=long_title,
            description=long_description,
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            config=config,
        )

        # Should be truncated to max lengths
        self.assertEqual(len(chart.title), 255)
        self.assertEqual(len(chart.description), 1000)

    def test_create_chart_without_warehouse(self):
        """Test chart creation without warehouse configured"""
        # Remove warehouse
        self.chart_service.warehouse = None

        config = {
            "chartType": "bar",
            "computation_type": "raw",
            "xAxis": "category",
            "yAxis": "value",
        }

        with self.assertRaises(ValidationError) as cm:
            self.chart_service.create_chart(
                title="Test Chart",
                description="Test Description",
                chart_type="bar",
                schema_name="public",
                table_name="test_table",
                config=config,
            )

        self.assertIn("No warehouse configured", str(cm.exception))

    def test_generate_chart_data_with_sql_injection_attempts(self):
        """Test chart data generation with SQL injection attempts"""
        malicious_inputs = [
            ("'; DROP TABLE users; --", "value"),
            ("category", "'; DELETE FROM users; --"),
            ("category UNION SELECT * FROM users", "value"),
            ("category", "value OR 1=1"),
        ]

        for malicious_x, malicious_y in malicious_inputs:
            with self.subTest(x=malicious_x, y=malicious_y):
                with self.assertRaises(ValidationError):
                    self.chart_service.generate_chart_data(
                        chart_type="bar",
                        computation_type="raw",
                        schema_name="public",
                        table_name="test_table",
                        xaxis=malicious_x,
                        yaxis=malicious_y,
                    )

    def test_performance_with_large_datasets(self):
        """Test performance considerations with large datasets"""
        # Mock large dataset but apply limit in mock to simulate database behavior
        large_dataset = [
            {"x": f"item_{i}", "y": i} for i in range(100)
        ]  # Limit to 100 as database would
        self.chart_service.warehouse.execute.return_value = large_dataset

        # Test with limit
        chart_data = self.chart_service.generate_chart_data(
            chart_type="bar",
            computation_type="raw",
            schema_name="test_schema",
            table_name="test_table",
            xaxis="category",
            yaxis="value",
            limit=100,
        )

        # Should be limited to 100 items (database would apply this limit)
        self.assertLessEqual(len(chart_data["raw_data"]["data"]), 100)
