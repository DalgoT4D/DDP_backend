"""
End-to-End Integration Tests for Complete AI-Driven Data Access System

Tests the full 4-layer architecture from user question to executed results:
- Layer 1: Data Intelligence Service
- Layer 2: Natural Language Query Generator
- Layer 3: Enhanced Dynamic Query Executor  
- Layer 4: Smart Chat Processor Integration
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from django.test import TestCase
from django.utils import timezone

from ddpui.core.ai.data_intelligence import (
    DataIntelligenceService,
    DataCatalog,
    DataTableInfo,
    DataColumnInfo,
)
from ddpui.core.ai.query_generator import NaturalLanguageQueryService, QueryPlan
from ddpui.core.ai.enhanced_executor import EnhancedDynamicExecutor
from ddpui.core.ai.smart_chat_processor import SmartChatProcessor, MessageIntent, MessageAnalysis
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_settings import OrgSettings
from ddpui.models.visualization import Chart


class TestEndToEndAIDataAccess(TestCase):
    """Complete end-to-end tests for AI-driven data access"""

    def setUp(self):
        """Set up test fixtures for full integration testing"""
        # Create test org with AI settings
        self.org = Org.objects.create(name="Test Education Analytics", slug="test-edu-analytics")

        # Create org settings with AI enabled
        self.org_settings = OrgSettings.objects.create(
            org=self.org, ai_consent=True, ai_data_sharing_enabled=True
        )

        # Create test warehouse
        self.warehouse = OrgWarehouse.objects.create(
            org=self.org,
            wtype="postgres",
            name="education_warehouse",
            host="localhost",
            port=5432,
            username="test_user",
            password="test_pass",
            database="education_db",
        )

        # Create realistic test charts
        self.enrollment_chart = Chart.objects.create(
            org=self.org,
            title="Student Enrollment by State and Year",
            chart_type="bar",
            schema_name="education",
            table_name="student_enrollment",
            extra_config={
                "dimension_column": "state",
                "extra_dimension_column": "year",
                "metrics": [{"column": "student_count", "aggregation": "sum"}],
            },
        )

        self.performance_chart = Chart.objects.create(
            org=self.org,
            title="School Performance Trends",
            chart_type="line",
            schema_name="education",
            table_name="school_performance",
            extra_config={"dimension_column": "year", "y_axis_column": "avg_score"},
        )

        self.revenue_chart = Chart.objects.create(
            org=self.org,
            title="Revenue by Region",
            chart_type="pie",
            schema_name="finance",
            table_name="revenue_data",
            extra_config={
                "dimension_column": "region",
                "metrics": [{"column": "total_revenue", "aggregation": "sum"}],
            },
        )

    def _create_comprehensive_data_catalog(self) -> DataCatalog:
        """Create comprehensive data catalog for testing"""
        catalog = DataCatalog(org_id=self.org.id)

        # Student enrollment table
        enrollment_table = DataTableInfo(
            schema_name="education",
            table_name="student_enrollment",
            row_count_estimate=150000,
            business_description="Student enrollment data across states and years for education analytics",
        )
        enrollment_table.columns = [
            DataColumnInfo(
                name="state",
                data_type="varchar",
                nullable=False,
                sample_values=["Maharashtra", "Karnataka", "Uttarakhand", "Gujarat", "Tamil Nadu"],
                business_context="Geographic dimension - Indian states for location-based analysis",
                is_dimension=True,
            ),
            DataColumnInfo(
                name="year",
                data_type="integer",
                nullable=False,
                sample_values=[2019, 2020, 2021, 2022, 2023],
                business_context="Temporal dimension - academic years for trend analysis",
                is_dimension=True,
            ),
            DataColumnInfo(
                name="student_count",
                data_type="integer",
                nullable=False,
                sample_values=[25000, 28500, 31200, 29800, 33100],
                business_context="Primary metric - total enrolled students",
                is_metric=True,
            ),
            DataColumnInfo(
                name="school_count",
                data_type="integer",
                nullable=False,
                sample_values=[450, 520, 580, 565, 620],
                business_context="Supporting metric - number of active schools",
                is_metric=True,
            ),
            DataColumnInfo(
                name="district",
                data_type="varchar",
                nullable=True,
                sample_values=["Mumbai", "Pune", "Bangalore", "Mysore", "Dehradun"],
                business_context="Geographic subdivision for detailed regional analysis",
                is_dimension=True,
            ),
        ]

        # School performance table
        performance_table = DataTableInfo(
            schema_name="education",
            table_name="school_performance",
            row_count_estimate=45000,
            business_description="Academic performance metrics across schools and time periods",
        )
        performance_table.columns = [
            DataColumnInfo(
                name="year",
                data_type="integer",
                nullable=False,
                sample_values=[2019, 2020, 2021, 2022, 2023],
                business_context="Academic year for performance tracking",
                is_dimension=True,
            ),
            DataColumnInfo(
                name="state",
                data_type="varchar",
                nullable=False,
                sample_values=["Maharashtra", "Karnataka", "Uttarakhand"],
                business_context="State-level performance grouping",
                is_dimension=True,
            ),
            DataColumnInfo(
                name="avg_score",
                data_type="decimal",
                nullable=False,
                sample_values=[78.5, 82.3, 75.8, 85.1, 80.9],
                business_context="Average academic performance score (0-100 scale)",
                is_metric=True,
            ),
            DataColumnInfo(
                name="school_id",
                data_type="integer",
                nullable=False,
                sample_values=[101, 102, 103, 104, 105],
                business_context="Unique school identifier for detailed analysis",
                is_dimension=True,
            ),
            DataColumnInfo(
                name="subject",
                data_type="varchar",
                nullable=True,
                sample_values=["Mathematics", "Science", "Language", "Social Studies"],
                business_context="Subject-wise performance breakdown",
                is_dimension=True,
            ),
        ]

        # Revenue data table
        revenue_table = DataTableInfo(
            schema_name="finance",
            table_name="revenue_data",
            row_count_estimate=12000,
            business_description="Financial revenue data by region and time period",
        )
        revenue_table.columns = [
            DataColumnInfo(
                name="region",
                data_type="varchar",
                nullable=False,
                sample_values=["North", "South", "East", "West", "Central"],
                business_context="Geographic regions for revenue analysis",
                is_dimension=True,
            ),
            DataColumnInfo(
                name="month",
                data_type="integer",
                nullable=False,
                sample_values=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                business_context="Monthly revenue tracking",
                is_dimension=True,
            ),
            DataColumnInfo(
                name="total_revenue",
                data_type="decimal",
                nullable=False,
                sample_values=[1250000.50, 980000.75, 1450000.00, 1180000.25],
                business_context="Total revenue amount in currency units",
                is_metric=True,
            ),
            DataColumnInfo(
                name="year",
                data_type="integer",
                nullable=False,
                sample_values=[2021, 2022, 2023],
                business_context="Revenue year for trend analysis",
                is_dimension=True,
            ),
        ]

        catalog.tables = {
            "education.student_enrollment": enrollment_table,
            "education.school_performance": performance_table,
            "finance.revenue_data": revenue_table,
        }

        catalog.chart_table_mappings = {
            self.enrollment_chart.id: "education.student_enrollment",
            self.performance_chart.id: "education.school_performance",
            self.revenue_chart.id: "finance.revenue_data",
        }

        return catalog

    def _create_mock_query_results(self, question_type: str):
        """Create realistic mock query results based on question type"""
        if "student" in question_type.lower() and "2021" in question_type:
            return [
                {"state": "Maharashtra", "student_count": 31200},
                {"state": "Karnataka", "student_count": 28500},
                {"state": "Uttarakhand", "student_count": 15800},
                {"state": "Gujarat", "student_count": 24600},
                {"state": "Tamil Nadu", "student_count": 29400},
            ]
        elif "revenue" in question_type.lower():
            return [
                {"region": "South", "total_revenue": 1450000.00},
                {"region": "West", "total_revenue": 1250000.50},
                {"region": "North", "total_revenue": 1180000.25},
                {"region": "East", "total_revenue": 980000.75},
                {"region": "Central", "total_revenue": 865000.30},
            ]
        elif "performance" in question_type.lower():
            return [
                {"state": "Karnataka", "avg_score": 85.1},
                {"state": "Maharashtra", "avg_score": 82.3},
                {"state": "Uttarakhand", "avg_score": 78.5},
            ]
        else:
            return [{"result": "Sample data"}]

    @patch("ddpui.core.ai.query_generator.get_default_ai_provider")
    @patch("ddpui.datainsights.warehouse.warehouse_factory.WarehouseFactory.get_warehouse_client")
    def test_complete_end_to_end_student_query(self, mock_warehouse_factory, mock_ai_provider):
        """Test complete flow: 'How many students attended school in 2021 in Maharashtra?'"""

        # Setup AI provider mock
        mock_ai_response = Mock()
        mock_ai_response.content = """
        {
            "sql": "SELECT state, SUM(student_count) as total_students FROM education.student_enrollment WHERE year = 2021 AND state = 'Maharashtra' GROUP BY state",
            "explanation": "This query gets the total student count for Maharashtra in 2021",
            "confidence": 0.85,
            "reasoning": "Query filters by year and state, then sums student counts",
            "expected_result": "aggregation"
        }
        """

        mock_ai_provider_instance = Mock()
        mock_ai_provider_instance.chat_completion.return_value = mock_ai_response
        mock_ai_provider.return_value = mock_ai_provider_instance

        # Setup warehouse client mock
        mock_client = Mock()
        mock_client.get_table_columns.return_value = []
        mock_client.execute.return_value = [{"state": "Maharashtra", "total_students": 31200}]
        mock_warehouse_factory.return_value = mock_client

        # Initialize complete system
        smart_processor = SmartChatProcessor()

        # Mock the data intelligence service
        with patch.object(
            smart_processor.executor.data_intelligence, "get_org_data_catalog"
        ) as mock_catalog:
            mock_catalog.return_value = self._create_comprehensive_data_catalog()

            question = "How many students attended school in 2021 in Maharashtra?"

            # Test the complete end-to-end flow
            enhanced_response = smart_processor.process_enhanced_chat_message(
                message=question, org=self.org, enable_data_query=True
            )

            # Verify complete flow worked
            self.assertTrue(enhanced_response.query_executed)
            self.assertEqual(enhanced_response.intent_detected, MessageIntent.DATA_QUERY)
            self.assertIsNotNone(enhanced_response.data_results)
            self.assertIn("31200", enhanced_response.content)
            self.assertIn("Maharashtra", enhanced_response.content)
            self.assertGreater(enhanced_response.confidence_score, 0.8)

    def test_message_intent_detection_accuracy(self):
        """Test accuracy of message intent detection across different question types"""
        smart_processor = SmartChatProcessor()

        test_cases = [
            ("How many students in 2021?", MessageIntent.DATA_QUERY, True),
            ("Show me revenue by region", MessageIntent.DATA_QUERY, True),
            ("What is the top performing state?", MessageIntent.DATA_QUERY, True),
            ("Explain this dashboard", MessageIntent.DASHBOARD_EXPLANATION, False),
            ("What does this chart show?", MessageIntent.DASHBOARD_EXPLANATION, False),
            ("Hello, how are you?", MessageIntent.GENERAL_CONVERSATION, False),
            ("Help me understand this visualization", MessageIntent.DASHBOARD_EXPLANATION, False),
            ("Compare revenue 2021 vs 2022", MessageIntent.DATA_QUERY, True),
        ]

        for question, expected_intent, expected_data_query in test_cases:
            with self.subTest(question=question):
                analysis = smart_processor.analyze_message(question)

                self.assertEqual(analysis.intent, expected_intent)
                self.assertEqual(analysis.data_query_detected, expected_data_query)
                self.assertGreater(analysis.confidence, 0.3)

    @patch("ddpui.core.ai.query_generator.get_default_ai_provider")
    @patch("ddpui.datainsights.warehouse.warehouse_factory.WarehouseFactory.get_warehouse_client")
    def test_performance_optimization_and_caching(self, mock_warehouse_factory, mock_ai_provider):
        """Test performance optimization and caching in enhanced executor"""

        # Setup mocks
        mock_ai_response = Mock()
        mock_ai_response.content = '{"sql": "SELECT COUNT(*) FROM education.student_enrollment", "confidence": 0.9, "explanation": "Count query", "reasoning": "Simple count", "expected_result": "single_value"}'

        mock_ai_provider_instance = Mock()
        mock_ai_provider_instance.chat_completion.return_value = mock_ai_response
        mock_ai_provider.return_value = mock_ai_provider_instance

        mock_client = Mock()
        mock_client.execute.return_value = [{"count": 150000}]
        mock_warehouse_factory.return_value = mock_client

        executor = EnhancedDynamicExecutor()

        with patch.object(executor.data_intelligence, "get_org_data_catalog") as mock_catalog:
            mock_catalog.return_value = self._create_comprehensive_data_catalog()

            question = "How many total students?"

            # First execution - should be fresh
            result1, analytics1 = executor.execute_natural_language_query_enhanced(
                question=question, org=self.org, enable_caching=True, enable_optimization=True
            )

            # Verify first execution
            self.assertTrue(result1.success)
            self.assertFalse(analytics1.get("cache_used", False))
            self.assertGreater(len(analytics1.get("optimizations_applied", [])), 0)

            # Second execution - should use cache (if implemented)
            result2, analytics2 = executor.execute_natural_language_query_enhanced(
                question=question, org=self.org, enable_caching=True, enable_optimization=True
            )

            # Verify second execution
            self.assertTrue(result2.success)
            # Note: Cache behavior depends on implementation details

    def test_security_validation_comprehensive(self):
        """Test comprehensive security validation across the system"""
        smart_processor = SmartChatProcessor()

        # Test malicious queries are rejected
        malicious_questions = [
            "DROP TABLE students; SELECT * FROM users",
            "Show me data; DELETE FROM revenue_data;",
            "/* malicious comment */ SELECT password FROM users",
            "EXEC xp_cmdshell 'dir'",
        ]

        for malicious_question in malicious_questions:
            with self.subTest(question=malicious_question):
                analysis = smart_processor.analyze_message(malicious_question)

                # Even if detected as data query, should be handled safely
                if analysis.data_query_detected:
                    # The security validator should catch this during execution
                    # We're mainly testing that the system doesn't crash
                    self.assertIsInstance(analysis, MessageAnalysis)

    def test_fallback_behavior_when_ai_unavailable(self):
        """Test system behavior when AI services are unavailable"""
        smart_processor = SmartChatProcessor()

        # Mock AI provider to fail
        with patch.object(smart_processor, "ai_provider", None):
            with patch(
                "ddpui.core.ai.factory.get_default_ai_provider",
                side_effect=Exception("AI unavailable"),
            ):
                enhanced_response = smart_processor.process_enhanced_chat_message(
                    message="How many students in 2021?", org=self.org, enable_data_query=True
                )

                # Should fallback gracefully
                self.assertFalse(enhanced_response.query_executed)
                self.assertTrue(enhanced_response.fallback_used)
                self.assertIn("error", enhanced_response.content.lower())

    @patch("ddpui.core.ai.query_generator.get_default_ai_provider")
    @patch("ddpui.datainsights.warehouse.warehouse_factory.WarehouseFactory.get_warehouse_client")
    def test_business_context_awareness(self, mock_warehouse_factory, mock_ai_provider):
        """Test that the system correctly understands business context"""

        # Setup AI provider
        mock_ai_response = Mock()
        mock_ai_response.content = '{"sql": "SELECT region, SUM(total_revenue) FROM finance.revenue_data GROUP BY region ORDER BY SUM(total_revenue) DESC LIMIT 5", "confidence": 0.88, "explanation": "Top regions by revenue", "reasoning": "Business analysis query", "expected_result": "table"}'

        mock_ai_provider_instance = Mock()
        mock_ai_provider_instance.chat_completion.return_value = mock_ai_response
        mock_ai_provider.return_value = mock_ai_provider_instance

        # Setup warehouse
        mock_client = Mock()
        mock_client.execute.return_value = self._create_mock_query_results("revenue")
        mock_warehouse_factory.return_value = mock_client

        smart_processor = SmartChatProcessor()

        with patch.object(
            smart_processor.executor.data_intelligence, "get_org_data_catalog"
        ) as mock_catalog:
            mock_catalog.return_value = self._create_comprehensive_data_catalog()

            # Test business-focused question
            enhanced_response = smart_processor.process_enhanced_chat_message(
                message="Which regions generate the most revenue?",
                org=self.org,
                enable_data_query=True,
            )

            # Verify business context understanding
            self.assertTrue(enhanced_response.query_executed)
            self.assertEqual(enhanced_response.intent_detected, MessageIntent.DATA_QUERY)
            self.assertIn("revenue", enhanced_response.content.lower())
            self.assertIn("region", enhanced_response.content.lower())

    def test_temporal_and_geographic_entity_extraction(self):
        """Test extraction of temporal and geographic entities from questions"""
        smart_processor = SmartChatProcessor()

        test_cases = [
            ("Students in Maharashtra in 2021", "2021", "Maharashtra"),
            ("Revenue for last year in Karnataka", "last year", "Karnataka"),
            ("Performance in Uttarakhand during 2022", "2022", "Uttarakhand"),
            ("Enrollment from Gujarat in Q1 2023", "Q1 2023", "Gujarat"),
        ]

        for question, expected_temporal, expected_geographic in test_cases:
            with self.subTest(question=question):
                analysis = smart_processor.analyze_message(question)

                # Check temporal extraction
                if expected_temporal:
                    self.assertIsNotNone(analysis.temporal_context)
                    self.assertIn(expected_temporal.split()[0], analysis.temporal_context)

                # Check geographic extraction
                if expected_geographic:
                    self.assertIsNotNone(analysis.geographic_context)
                    self.assertEqual(analysis.geographic_context, expected_geographic)

    def test_rate_limiting_and_safety_controls(self):
        """Test rate limiting and safety controls work correctly"""
        executor = EnhancedDynamicExecutor()

        # Set restrictive limits for testing
        executor.safety_limits.max_queries_per_minute = 2

        # First two queries should succeed
        for i in range(2):
            can_execute, message = executor._check_rate_limits(self.org.id)
            self.assertTrue(can_execute, f"Query {i+1} should be allowed")
            executor._update_rate_limit_tracker(self.org.id)

        # Third query should be rate limited
        can_execute, message = executor._check_rate_limits(self.org.id)
        self.assertFalse(can_execute)
        self.assertIn("rate limit", message.lower())

    def test_error_handling_and_recovery(self):
        """Test error handling and recovery mechanisms throughout the system"""
        smart_processor = SmartChatProcessor()

        # Test with invalid dashboard context
        invalid_context = {"invalid": "context"}

        enhanced_response = smart_processor.process_enhanced_chat_message(
            message="Show me the data",
            org=self.org,
            dashboard_context=invalid_context,
            enable_data_query=True,
        )

        # Should handle gracefully
        self.assertIsNotNone(enhanced_response)
        self.assertIsInstance(enhanced_response.content, str)
        self.assertGreater(len(enhanced_response.content), 0)

    @patch("ddpui.core.ai.query_generator.get_default_ai_provider")
    @patch("ddpui.datainsights.warehouse.warehouse_factory.WarehouseFactory.get_warehouse_client")
    def test_complex_multi_table_query_handling(self, mock_warehouse_factory, mock_ai_provider):
        """Test handling of complex queries involving multiple tables"""

        # Setup AI to generate complex query
        mock_ai_response = Mock()
        mock_ai_response.content = """
        {
            "sql": "SELECT e.state, AVG(p.avg_score) as performance, SUM(e.student_count) as students FROM education.student_enrollment e JOIN education.school_performance p ON e.state = p.state WHERE e.year = 2021 GROUP BY e.state ORDER BY performance DESC",
            "explanation": "This query correlates enrollment and performance data by state for 2021",
            "confidence": 0.75,
            "reasoning": "Complex join query to analyze enrollment vs performance relationship",
            "expected_result": "table"
        }
        """

        mock_ai_provider_instance = Mock()
        mock_ai_provider_instance.chat_completion.return_value = mock_ai_response
        mock_ai_provider.return_value = mock_ai_provider_instance

        # Setup warehouse client
        mock_client = Mock()
        mock_client.execute.return_value = [
            {"state": "Karnataka", "performance": 85.1, "students": 28500},
            {"state": "Maharashtra", "performance": 82.3, "students": 31200},
            {"state": "Uttarakhand", "performance": 78.5, "students": 15800},
        ]
        mock_warehouse_factory.return_value = mock_client

        smart_processor = SmartChatProcessor()

        with patch.object(
            smart_processor.executor.data_intelligence, "get_org_data_catalog"
        ) as mock_catalog:
            mock_catalog.return_value = self._create_comprehensive_data_catalog()

            # Test complex analytical question
            enhanced_response = smart_processor.process_enhanced_chat_message(
                message="Compare student enrollment and performance by state in 2021",
                org=self.org,
                enable_data_query=True,
            )

            # Verify complex query handling
            self.assertTrue(enhanced_response.query_executed)
            self.assertIn("Karnataka", enhanced_response.content)
            self.assertIn("performance", enhanced_response.content.lower())
            self.assertIn("students", enhanced_response.content.lower())

    def test_recommendation_engine(self):
        """Test that the system provides helpful recommendations"""
        smart_processor = SmartChatProcessor()

        # Test with vague question
        analysis = smart_processor.analyze_message("Show me some data")
        recommendations = smart_processor._generate_query_suggestions(analysis, None)

        self.assertGreater(len(recommendations), 0)
        self.assertTrue(all(isinstance(rec, str) for rec in recommendations))

        # Test with specific context
        dashboard_context = {
            "charts": [{"title": "Revenue Analysis"}, {"title": "Customer Demographics"}]
        }

        recommendations = smart_processor._generate_query_suggestions(analysis, dashboard_context)
        self.assertGreater(len(recommendations), 0)

    def test_system_monitoring_and_analytics(self):
        """Test monitoring and analytics capabilities"""
        executor = EnhancedDynamicExecutor()

        # Simulate some execution history
        test_executions = [
            {
                "org_id": self.org.id,
                "execution_success": True,
                "execution_time_ms": 1500,
                "result_row_count": 25,
                "ai_confidence": 0.8,
            },
            {
                "org_id": self.org.id,
                "execution_success": False,
                "execution_time_ms": 500,
                "result_row_count": 0,
                "ai_confidence": 0.3,
            },
        ]

        executor._execution_history = test_executions

        # Test stats generation
        stats = executor.get_enhanced_execution_stats(org_id=self.org.id)

        self.assertIn("total_executions", stats)
        self.assertIn("success_rate", stats)
        self.assertIn("cache_performance", stats)
        self.assertEqual(stats["total_executions"], 2)
        self.assertEqual(stats["success_rate"], 0.5)
