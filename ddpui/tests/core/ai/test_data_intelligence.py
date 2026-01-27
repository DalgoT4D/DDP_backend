from types import SimpleNamespace
from unittest.mock import patch

from django.test import TestCase

from ddpui.core.ai.data_intelligence import (
    DataCatalog,
    DataColumnInfo,
    DataIntelligenceService,
    DataTableInfo,
)
from ddpui.models.org import Org


class TestDataIntelligenceService(TestCase):
    def setUp(self):
        self.org = Org.objects.create(name="Test Org", slug="test-org")
        self.service = DataIntelligenceService()

    def test_build_table_chart_mapping(self):
        charts = [
            SimpleNamespace(id=1, schema_name="education", table_name="students"),
            SimpleNamespace(id=2, schema_name="education", table_name="students"),
            SimpleNamespace(id=3, schema_name=None, table_name="ignored"),
        ]

        mapping = self.service._build_table_chart_mapping(charts)

        self.assertEqual(mapping["education.students"], [1, 2])

    def test_generate_column_business_context_patterns(self):
        revenue_column = DataColumnInfo(name="total_revenue", data_type="decimal", nullable=False)
        created_column = DataColumnInfo(name="created_at", data_type="timestamp", nullable=False)
        bool_column = DataColumnInfo(name="is_active", data_type="bool", nullable=False)

        revenue_context = self.service._generate_column_business_context(revenue_column)
        created_context = self.service._generate_column_business_context(created_column)
        bool_context = self.service._generate_column_business_context(bool_column)

        self.assertIn("Financial metric", revenue_context)
        self.assertIn("Temporal dimension", created_context)
        self.assertIn("Boolean flag", bool_context)

    def test_build_ai_data_context_focus_tables(self):
        table_info = DataTableInfo(
            schema_name="education",
            table_name="students",
            columns=[
                DataColumnInfo(
                    name="state", data_type="varchar", nullable=False, is_dimension=True
                ),
                DataColumnInfo(
                    name="student_count", data_type="integer", nullable=False, is_metric=True
                ),
            ],
            row_count_estimate=5000,
            business_description="Student enrollment by state",
        )
        other_table = DataTableInfo(
            schema_name="finance",
            table_name="revenue",
            columns=[
                DataColumnInfo(
                    name="region", data_type="varchar", nullable=False, is_dimension=True
                ),
                DataColumnInfo(
                    name="total_revenue", data_type="decimal", nullable=False, is_metric=True
                ),
            ],
            row_count_estimate=1200,
            business_description="Revenue by region",
        )

        catalog = DataCatalog(
            org_id=self.org.id,
            tables={
                "education.students": table_info,
                "finance.revenue": other_table,
            },
            total_charts=2,
            total_tables=2,
        )

        with patch.object(self.service, "get_org_data_catalog", return_value=catalog):
            context = self.service.build_ai_data_context(
                org=self.org, focus_tables=["education.students"]
            )

        self.assertIn("TABLE: education.students", context)
        self.assertIn("QUERY GUIDELINES", context)
        self.assertNotIn("TABLE: finance.revenue", context)
