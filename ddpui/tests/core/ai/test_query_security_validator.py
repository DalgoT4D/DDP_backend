from unittest.mock import patch

from django.test import TestCase

from ddpui.core.ai.query_generator import QuerySecurityValidator, QueryValidationResult
from ddpui.models.org import Org, OrgWarehouse


class TestQuerySecurityValidator(TestCase):
    def setUp(self):
        self.org = Org.objects.create(name="Test Org", slug="test-org")
        self.warehouse = OrgWarehouse.objects.create(
            org=self.org,
            wtype="postgres",
            name="test_warehouse",
            credentials="{}",
        )

    @patch.object(QuerySecurityValidator, "_validate_sql_syntax")
    def test_valid_select_query_passes(self, mock_validate_sql_syntax):
        mock_validate_sql_syntax.return_value = QueryValidationResult(is_valid=True)
        validator = QuerySecurityValidator()

        query = (
            "SELECT state, COUNT(*) AS total_students "
            "FROM education.student_enrollment "
            "GROUP BY state LIMIT 10"
        )
        available_tables = {"education.student_enrollment"}

        result = validator.validate_query(query, available_tables, self.warehouse)

        self.assertTrue(result.is_valid)
        self.assertIn("education.student_enrollment", result.tables_accessed)
        self.assertIn("state", result.columns_accessed)

    @patch.object(QuerySecurityValidator, "_validate_sql_syntax")
    def test_unknown_table_blocked(self, mock_validate_sql_syntax):
        mock_validate_sql_syntax.return_value = QueryValidationResult(is_valid=True)
        validator = QuerySecurityValidator()

        query = "SELECT * FROM finance.revenue_data"
        available_tables = {"education.student_enrollment"}

        result = validator.validate_query(query, available_tables, self.warehouse)

        self.assertFalse(result.is_valid)
        self.assertIn("Access denied", result.error_message)

    @patch.object(QuerySecurityValidator, "_validate_sql_syntax")
    def test_dangerous_pattern_blocked(self, mock_validate_sql_syntax):
        mock_validate_sql_syntax.return_value = QueryValidationResult(is_valid=True)
        validator = QuerySecurityValidator()

        query = "SELECT * FROM education.student_enrollment; DROP TABLE users;"
        available_tables = {"education.student_enrollment"}

        result = validator.validate_query(query, available_tables, self.warehouse)

        self.assertFalse(result.is_valid)
        self.assertIn("Dangerous SQL pattern", result.error_message)
