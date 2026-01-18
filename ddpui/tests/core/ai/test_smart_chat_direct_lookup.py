from django.test import TestCase

from ddpui.core.ai.smart_chat_processor import SmartChatProcessor
from ddpui.models.org import Org


class TestSmartChatDirectLookup(TestCase):
    def setUp(self):
        self.org = Org.objects.create(name="Test Org", slug="test-org")
        self.processor = SmartChatProcessor()

    def test_direct_lookup_aggregates_matching_rows(self):
        dashboard_context = {
            "charts": [
                {
                    "title": "Population by State",
                    "sample_data": {
                        "columns": ["state", "population"],
                        "rows": [
                            {"state": "Karnataka", "population": 300},
                            {"state": "Karnataka", "population": 200},
                            {"state": "Gujarat", "population": 150},
                        ],
                    },
                    "schema": {"schema_name": "public", "table_name": "population"},
                }
            ]
        }

        response = self.processor.process_enhanced_chat_message(
            message="What is the population of Karnataka?",
            org=self.org,
            dashboard_context=dashboard_context,
            enable_data_query=True,
        )

        self.assertFalse(response.query_executed)
        self.assertIn("Karnataka", response.content)
        self.assertIn("500", response.content)
        self.assertEqual(response.data_results.get("source"), "direct_context_lookup_aggregate")
        self.assertEqual(response.data_results.get("row_count"), 2)

    def test_direct_lookup_matches_chart_title(self):
        dashboard_context = {
            "charts": [
                {
                    "title": "Karnataka Population",
                    "sample_data": {
                        "columns": ["population"],
                        "rows": [{"population": 750}],
                    },
                    "schema": {"schema_name": "public", "table_name": "population"},
                }
            ]
        }

        response = self.processor.process_enhanced_chat_message(
            message="population of Karnataka",
            org=self.org,
            dashboard_context=dashboard_context,
            enable_data_query=True,
        )

        self.assertFalse(response.query_executed)
        self.assertIn("Karnataka", response.content)
        self.assertIn("750", response.content)
        self.assertEqual(response.data_results.get("source"), "direct_context_lookup_title")
        self.assertEqual(response.data_results.get("row_count"), 1)
