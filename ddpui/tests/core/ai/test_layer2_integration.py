"""
Integration tests for Layer 2: Natural Language Query Generation

Tests the complete flow from natural language question to executed query results.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from django.test import TestCase
from django.contrib.auth.models import User

from ddpui.core.ai.query_generator import (
    NaturalLanguageQueryService,
    QueryPlan,
    QueryValidationResult,
)
from ddpui.core.ai.query_executor import DynamicQueryExecutor, QueryExecutionResult
from ddpui.core.ai.data_intelligence import (
    DataIntelligenceService,
    DataCatalog,
    DataTableInfo,
    DataColumnInfo,
)
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.visualization import Chart


class TestLayer2Integration(TestCase):
    """Integration tests for Layer 2 components"""

    def setUp(self):
        """Set up test fixtures"""
        # Create test org
        self.org = Org.objects.create(name="Test Education Org", slug="test-edu-org")
        self.user = User.objects.create_user(
            username="testuser", email="testuser@example.com", password="password"
        )
        self.org_user = OrgUser.objects.create(user=self.user, org=self.org)

        # Create test warehouse
        self.warehouse = OrgWarehouse.objects.create(
            org=self.org,
            wtype="postgres",
            name="test_warehouse",
            credentials="{}",
        )

        # Create test charts
        self.chart1 = Chart.objects.create(
            org=self.org,
            created_by=self.org_user,
            title="Student Enrollment by State",
            chart_type="bar",
            schema_name="education",
            table_name="student_enrollment",
            extra_config={
                "dimension_column": "state",
                "metrics": [{"column": "student_count", "aggregation": "sum"}],
            },
        )

        self.chart2 = Chart.objects.create(
            org=self.org,
            created_by=self.org_user,
            title="School Performance",
            chart_type="line",
            schema_name="education",
            table_name="school_performance",
            extra_config={"dimension_column": "year", "y_axis_column": "performance_score"},
        )

    def _create_mock_data_catalog(self) -> DataCatalog:
        """Create a mock data catalog for testing"""
        catalog = DataCatalog(org_id=self.org.id)

        # Student enrollment table
        enrollment_table = DataTableInfo(
            schema_name="education",
            table_name="student_enrollment",
            row_count_estimate=50000,
            business_description="Contains student enrollment data by state and year",
        )

        enrollment_table.columns = [
            DataColumnInfo(
                name="state",
                data_type="varchar",
                nullable=False,
                sample_values=["Maharashtra", "Karnataka", "Uttarakhand", "Gujarat"],
                business_context="Geographic dimension - used for location-based analysis",
                is_dimension=True,
            ),
            DataColumnInfo(
                name="year",
                data_type="integer",
                nullable=False,
                sample_values=[2020, 2021, 2022, 2023],
                business_context="Temporal dimension - used for time-based analysis",
                is_dimension=True,
            ),
            DataColumnInfo(
                name="student_count",
                data_type="integer",
                nullable=False,
                sample_values=[15000, 18500, 22000, 19750],
                business_context="Numeric metric - represents counts or quantities",
                is_metric=True,
            ),
            DataColumnInfo(
                name="school_count",
                data_type="integer",
                nullable=False,
                sample_values=[150, 185, 220, 198],
                business_context="Numeric metric - represents counts or quantities",
                is_metric=True,
            ),
        ]

        # School performance table
        performance_table = DataTableInfo(
            schema_name="education",
            table_name="school_performance",
            row_count_estimate=25000,
            business_description="Contains school performance metrics by year and state",
        )

        performance_table.columns = [
            DataColumnInfo(
                name="year",
                data_type="integer",
                nullable=False,
                sample_values=[2020, 2021, 2022, 2023],
                business_context="Temporal dimension - used for time-based analysis",
                is_dimension=True,
            ),
            DataColumnInfo(
                name="state",
                data_type="varchar",
                nullable=False,
                sample_values=["Maharashtra", "Karnataka", "Uttarakhand"],
                business_context="Geographic dimension - used for location-based analysis",
                is_dimension=True,
            ),
            DataColumnInfo(
                name="performance_score",
                data_type="decimal",
                nullable=False,
                sample_values=[75.5, 82.3, 68.9, 90.1],
                business_context="Numeric data - can be used for calculations and aggregations",
                is_metric=True,
            ),
            DataColumnInfo(
                name="school_name",
                data_type="varchar",
                nullable=False,
                sample_values=["ABC School", "XYZ College", "State School"],
                business_context="Text dimension - descriptive information",
                is_dimension=True,
            ),
        ]

        catalog.tables = {
            "education.student_enrollment": enrollment_table,
            "education.school_performance": performance_table,
        }

        catalog.chart_table_mappings = {
            self.chart1.id: "education.student_enrollment",
            self.chart2.id: "education.school_performance",
        }

        return catalog

    @patch("ddpui.core.ai.query_generator.get_default_ai_provider")
    @patch("ddpui.datainsights.warehouse.warehouse_factory.WarehouseFactory.get_warehouse_client")
    def test_complete_query_generation_flow(self, mock_warehouse_factory, mock_ai_provider):
        """Test complete flow from question to query generation"""
        # Setup mocks
        mock_ai_response = Mock()
        mock_ai_response.content = """
        {
            "sql": "SELECT state, SUM(student_count) as total_students FROM education.student_enrollment WHERE year = 2021 GROUP BY state ORDER BY total_students DESC",
            "explanation": "This query calculates the total number of students by state for the year 2021",
            "confidence": 0.9,
            "reasoning": "The question asks for student counts in 2021, so I filtered by year and grouped by state",
            "expected_result": "aggregation"
        }
        """

        mock_ai_provider_instance = Mock()
        mock_ai_provider_instance.chat_completion.return_value = mock_ai_response
        mock_ai_provider.return_value = mock_ai_provider_instance

        # Mock warehouse client for validation
        mock_client = Mock()
        mock_client.get_table_columns.return_value = []
        mock_client.execute.return_value = []
        mock_warehouse_factory.return_value = mock_client

        # Create service with mocked data intelligence
        service = NaturalLanguageQueryService()

        with patch.object(service.data_intelligence, "get_org_data_catalog") as mock_catalog:
            mock_catalog.return_value = self._create_mock_data_catalog()

            # Test query generation
            question = "How many students attended school in 2021 by state?"
            query_plan = service.generate_query_from_question(question=question, org=self.org)

            # Verify query plan
            self.assertTrue(query_plan.requires_execution)
            self.assertIn("SELECT", query_plan.generated_sql.upper())
            self.assertIn("2021", query_plan.generated_sql)
            self.assertIn("student_count", query_plan.generated_sql)
            self.assertIn("state", query_plan.generated_sql)
            self.assertIn("LIMIT", query_plan.generated_sql.upper())
            self.assertEqual(query_plan.confidence_score, 0.9)
            self.assertEqual(query_plan.expected_result_type, "aggregation")

    @patch("ddpui.core.ai.query_generator.get_default_ai_provider")
    @patch("ddpui.datainsights.warehouse.warehouse_factory.WarehouseFactory.get_warehouse_client")
    def test_query_security_validation(self, mock_warehouse_factory, mock_ai_provider):
        """Test query security validation catches dangerous patterns"""
        # Setup AI to return dangerous query
        mock_ai_response = Mock()
        mock_ai_response.content = """
        {
            "sql": "SELECT * FROM education.student_enrollment; DROP TABLE education.student_enrollment; --",
            "explanation": "This is a malicious query",
            "confidence": 0.1,
            "reasoning": "Testing security",
            "expected_result": "table"
        }
        """

        mock_ai_provider_instance = Mock()
        mock_ai_provider_instance.chat_completion.return_value = mock_ai_response
        mock_ai_provider.return_value = mock_ai_provider_instance

        # Mock warehouse client
        mock_client = Mock()
        mock_warehouse_factory.return_value = mock_client

        service = NaturalLanguageQueryService()

        with patch.object(service.data_intelligence, "get_org_data_catalog") as mock_catalog:
            mock_catalog.return_value = self._create_mock_data_catalog()

            query_plan = service.generate_query_from_question(
                question="Show me all students", org=self.org
            )

            # Verify security validation caught the issue
            self.assertFalse(query_plan.requires_execution)
            self.assertTrue(query_plan.fallback_to_existing_data)
            self.assertIsNotNone(query_plan.validation_result)
            self.assertFalse(query_plan.validation_result.is_valid)

    @patch("ddpui.datainsights.warehouse.warehouse_factory.WarehouseFactory.get_warehouse_client")
    def test_complete_execution_flow(self, mock_warehouse_factory):
        """Test complete execution flow from question to results"""
        # Setup mock query results
        mock_query_results = [
            {"state": "Maharashtra", "total_students": 45000},
            {"state": "Karnataka", "total_students": 38000},
            {"state": "Uttarakhand", "total_students": 25000},
        ]

        mock_client = Mock()
        mock_client.execute.return_value = mock_query_results
        mock_warehouse_factory.return_value = mock_client

        # Create valid query plan
        query_plan = QueryPlan(
            original_question="How many students by state in 2021?",
            generated_sql="SELECT state, SUM(student_count) as total_students FROM education.student_enrollment WHERE year = 2021 GROUP BY state",
            explanation="Query to get student counts by state for 2021",
            confidence_score=0.8,
            requires_execution=True,
            validation_result=QueryValidationResult(
                is_valid=True,
                complexity_score=3,
                tables_accessed=["education.student_enrollment"],
                columns_accessed=["state", "student_count", "year"],
            ),
        )

        # Execute query
        executor = DynamicQueryExecutor()

        with patch.object(
            executor.query_generator, "generate_query_from_question"
        ) as mock_generator:
            mock_generator.return_value = query_plan

            result = executor.execute_natural_language_query(
                question="How many students by state in 2021?", org=self.org
            )

            # Verify execution results
            self.assertTrue(result.success)
            self.assertEqual(len(result.data), 3)
            self.assertEqual(result.data[0]["state"], "Maharashtra")
            self.assertEqual(result.data[0]["total_students"], 45000)
            self.assertGreater(result.execution_time_ms, 0)
            self.assertEqual(result.row_count, 3)

    def test_rate_limiting(self):
        """Test rate limiting functionality"""
        executor = DynamicQueryExecutor()

        # Set low limits for testing
        executor.safety_limits.max_queries_per_minute = 2
        executor.safety_limits.max_queries_per_hour = 5

        # First query should succeed
        can_execute, message = executor._check_rate_limits(self.org.id)
        self.assertTrue(can_execute)
        executor._update_rate_limit_tracker(self.org.id)

        # Second query should succeed
        can_execute, message = executor._check_rate_limits(self.org.id)
        self.assertTrue(can_execute)
        executor._update_rate_limit_tracker(self.org.id)

        # Third query should be rate limited
        can_execute, message = executor._check_rate_limits(self.org.id)
        self.assertFalse(can_execute)
        self.assertIn("Rate limit exceeded", message)

    def test_query_safety_modifications(self):
        """Test safety modifications applied to queries"""
        executor = DynamicQueryExecutor()

        # Test query without LIMIT
        query_without_limit = "SELECT * FROM education.student_enrollment"
        safe_query, modifications = executor._apply_safety_modifications(query_without_limit)

        self.assertIn("LIMIT 1000", safe_query)
        self.assertTrue(any("Added LIMIT" in mod for mod in modifications))

        # Test query with excessive LIMIT
        query_with_high_limit = "SELECT * FROM education.student_enrollment LIMIT 5000"
        safe_query, modifications = executor._apply_safety_modifications(query_with_high_limit)

        self.assertIn("LIMIT 1000", safe_query)
        self.assertNotIn("LIMIT 5000", safe_query)
        self.assertTrue(any("Reduced LIMIT" in mod for mod in modifications))

    @patch("ddpui.core.ai.query_generator.get_default_ai_provider")
    def test_table_relevance_scoring(self, mock_ai_provider):
        """Test that the system correctly identifies relevant tables for questions"""
        service = NaturalLanguageQueryService()
        catalog = self._create_mock_data_catalog()

        # Test question about student enrollment
        relevant_tables = service._identify_relevant_tables(
            "How many students in Maharashtra?", catalog
        )

        self.assertIn("education.student_enrollment", relevant_tables)

        # Test question about school performance
        relevant_tables = service._identify_relevant_tables(
            "What is the average performance score?", catalog
        )

        self.assertIn("education.school_performance", relevant_tables)

    def test_result_formatting(self):
        """Test result formatting handles different data types"""
        executor = DynamicQueryExecutor()

        # Test with dictionary results
        dict_results = [
            {"name": "Test School", "count": 100, "score": 85.5, "active": True},
            {"name": "Another School", "count": 150, "score": 92.3, "active": False},
        ]

        formatted_data, columns = executor._format_query_results(dict_results)

        self.assertEqual(len(formatted_data), 2)
        self.assertEqual(len(columns), 4)
        self.assertIn("name", columns)
        self.assertEqual(formatted_data[0]["count"], 100)
        self.assertEqual(formatted_data[1]["active"], False)

        # Test with tuple results
        tuple_results = [("Test School", 100, 85.5), ("Another School", 150, 92.3)]

        formatted_data, columns = executor._format_query_results(tuple_results)

        self.assertEqual(len(formatted_data), 2)
        self.assertEqual(len(columns), 3)
        self.assertTrue(all(col.startswith("column_") for col in columns))

    def test_execution_stats(self):
        """Test execution statistics tracking"""
        executor = DynamicQueryExecutor()

        # Simulate some executions
        mock_executions = [
            {
                "org_id": self.org.id,
                "execution_success": True,
                "execution_time_ms": 150,
                "result_row_count": 10,
                "ai_confidence": 0.8,
                "error_message": None,
            },
            {
                "org_id": self.org.id,
                "execution_success": False,
                "execution_time_ms": 50,
                "result_row_count": 0,
                "ai_confidence": 0.3,
                "error_message": "SQL syntax error",
            },
        ]

        executor._execution_history = mock_executions

        stats = executor.get_execution_stats(org_id=self.org.id)

        self.assertEqual(stats["total_executions"], 2)
        self.assertEqual(stats["successful_executions"], 1)
        self.assertEqual(stats["success_rate"], 0.5)
        self.assertEqual(stats["average_execution_time_ms"], 150.0)
        self.assertEqual(stats["average_ai_confidence"], 0.55)

    @patch("ddpui.core.ai.query_generator.get_default_ai_provider")
    def test_error_handling_and_fallback(self, mock_ai_provider):
        """Test error handling and fallback to existing data"""
        # Mock AI provider to raise an exception
        mock_ai_provider.side_effect = Exception("AI service unavailable")

        service = NaturalLanguageQueryService()

        with patch.object(service.data_intelligence, "get_org_data_catalog") as mock_catalog:
            mock_catalog.return_value = self._create_mock_data_catalog()

            query_plan = service.generate_query_from_question(
                question="How many students in 2021?", org=self.org
            )

            # Verify fallback behavior
            self.assertFalse(query_plan.requires_execution)
            self.assertTrue(query_plan.fallback_to_existing_data)
            self.assertIn("Failed to generate query", query_plan.explanation)
            self.assertEqual(query_plan.confidence_score, 0.0)

    @patch("ddpui.core.ai.query_generator.get_default_ai_provider")
    def test_empty_data_catalog_handling(self):
        """Test handling when no data catalog is available"""
        service = NaturalLanguageQueryService()

        # Create empty catalog
        empty_catalog = DataCatalog(org_id=self.org.id)

        with patch.object(service.data_intelligence, "get_org_data_catalog") as mock_catalog:
            mock_catalog.return_value = empty_catalog

            query_plan = service.generate_query_from_question(question="Show me data", org=self.org)

            # Verify graceful handling
            self.assertFalse(query_plan.requires_execution)
            self.assertTrue(query_plan.fallback_to_existing_data)
            self.assertIn("No data sources available", query_plan.explanation)
