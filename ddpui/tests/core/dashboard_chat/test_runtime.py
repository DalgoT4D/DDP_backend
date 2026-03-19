"""Tests for dashboard chat LangGraph runtime, allowlist, and SQL guard."""

import os

import django
import pytest
from django.contrib.auth.models import User
from django.db import transaction

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.dashboard_chat.allowlist import DashboardChatAllowlist, DashboardChatAllowlistBuilder
from ddpui.core.dashboard_chat.config import DashboardChatRuntimeConfig
from ddpui.core.dashboard_chat.runtime import DashboardChatRuntime
from ddpui.core.dashboard_chat.runtime_types import (
    DashboardChatConversationMessage,
    DashboardChatIntent,
    DashboardChatPlanMode,
    DashboardChatQueryPlan,
    DashboardChatSqlDraft,
    DashboardChatTextFilterPlan,
)
from ddpui.core.dashboard_chat.sql_guard import DashboardChatSqlGuard
from ddpui.core.dashboard_chat.vector_store import DashboardChatVectorQueryResult
from ddpui.models.dashboard import Dashboard
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.visualization import Chart
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db(transaction=True)


class FakeVectorStore:
    """Deterministic vector store used by runtime tests."""

    def __init__(self, rows):
        self.rows = list(rows)
        self.calls = []

    def query(self, org_id, query_text, n_results=5, source_types=None, dashboard_id=None):
        self.calls.append(
            {
                "org_id": org_id,
                "query_text": query_text,
                "n_results": n_results,
                "source_types": list(source_types) if source_types else [],
                "dashboard_id": dashboard_id,
            }
        )
        results = []
        for row in self.rows:
            if source_types and row.metadata.get("source_type") not in source_types:
                continue
            if dashboard_id is not None and row.metadata.get("dashboard_id") != dashboard_id:
                continue
            results.append(row)
        return results[:n_results]


class ContextOnlyLlm:
    """Minimal LLM stub for context-answer tests."""

    def plan_query(self, *args, **kwargs):
        raise AssertionError("plan_query should not be called for context-only heuristics")

    def classify_intent(self, *args, **kwargs):
        raise AssertionError("classify_intent should not be called for context-only heuristics")

    def generate_sql(self, *args, **kwargs):
        raise AssertionError("generate_sql should not be called for context-only heuristics")

    def compose_answer(
        self,
        user_query,
        dashboard_summary,
        retrieved_documents,
        sql,
        sql_results,
        warnings,
        related_dashboard_titles,
    ):
        assert sql is None
        assert sql_results is None
        return "The reach metric shows how many beneficiaries were served over time."


class FakeWarehouseTools:
    """Warehouse stub that records schema, distinct, and execution calls."""

    def __init__(self):
        self.schema_requests = []
        self.distinct_requests = []
        self.executed_sql = []

    def get_schema_snippets(self, tables):
        self.schema_requests.append(list(tables))
        return {
            "analytics.program_reach": self._schema_snippet(
                "analytics.program_reach",
                [
                    {"name": "program_name", "data_type": "text", "nullable": False},
                    {"name": "beneficiaries", "data_type": "integer", "nullable": False},
                ],
            )
        }

    def get_distinct_values(self, table_name, column_name, limit=50):
        self.distinct_requests.append((table_name, column_name, limit))
        return ["Education", "Health"]

    def execute_sql(self, sql):
        self.executed_sql.append(sql)
        return [{"program_name": "Education", "beneficiary_count": 120}]

    @staticmethod
    def _schema_snippet(table_name, columns):
        from ddpui.core.dashboard_chat.runtime_types import DashboardChatSchemaSnippet

        return DashboardChatSchemaSnippet(table_name=table_name, columns=columns)


class SqlPathLlm:
    """LLM stub that forces the runtime through planning, distinct lookup, and SQL execution."""

    def classify_intent(self, *args, **kwargs):
        raise AssertionError("Heuristic data routing should handle this test case")

    def plan_query(
        self,
        user_query,
        conversation_history,
        dashboard_summary,
        retrieved_documents,
        schema_prompt,
        allowlisted_tables,
    ):
        assert "analytics.program_reach" in allowlisted_tables
        assert "program_name" in schema_prompt
        return DashboardChatQueryPlan(
            mode=DashboardChatPlanMode.SQL,
            reason="Needs aggregate data",
            relevant_tables=["analytics.program_reach"],
            schema_lookup_tables=["analytics.program_reach"],
            text_filters=[
                DashboardChatTextFilterPlan(
                    table_name="analytics.program_reach",
                    column_name="program_name",
                    requested_value="Education",
                )
            ],
        )

    def generate_sql(
        self,
        user_query,
        dashboard_summary,
        query_plan,
        schema_prompt,
        distinct_values,
        allowlisted_tables,
    ):
        assert distinct_values["analytics.program_reach.program_name"] == ["Education", "Health"]
        return DashboardChatSqlDraft(
            sql=(
                "SELECT program_name, COUNT(*) AS beneficiary_count "
                "FROM analytics.program_reach "
                "WHERE program_name = 'Education' "
                "GROUP BY program_name "
                "LIMIT 50"
            ),
            reason="Uses the allowlisted chart table with an exact filter value.",
        )

    def compose_answer(
        self,
        user_query,
        dashboard_summary,
        retrieved_documents,
        sql,
        sql_results,
        warnings,
        related_dashboard_titles,
    ):
        assert sql is not None
        assert sql_results == [{"program_name": "Education", "beneficiary_count": 120}]
        return "Education has 120 beneficiaries on the current dashboard."


@pytest.fixture
def org():
    organization = Org.objects.create(
        name="Dashboard Chat Org",
        slug="dashchat",
        airbyte_workspace_id="workspace-1",
    )
    yield organization
    organization.delete()


@pytest.fixture
def orguser(org, seed_db):
    user = User.objects.create(
        username="dashchat-user",
        email="dashchat-user@test.com",
        password="testpassword",
    )
    org_user = OrgUser.objects.create(
        user=user,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield org_user
    org_user.delete()
    user.delete()


@pytest.fixture
def primary_chart(org, orguser):
    chart = Chart.objects.create(
        title="Program Reach",
        description="Monthly reach",
        chart_type="line",
        schema_name="analytics",
        table_name="program_reach",
        created_by=orguser,
        last_modified_by=orguser,
        org=org,
    )
    yield chart
    chart.delete()


@pytest.fixture
def related_chart(org, orguser):
    chart = Chart.objects.create(
        title="Funding by Donor",
        description="Donor funding mix",
        chart_type="bar",
        schema_name="analytics",
        table_name="donor_funding",
        created_by=orguser,
        last_modified_by=orguser,
        org=org,
    )
    yield chart
    chart.delete()


@pytest.fixture
def primary_dashboard(org, orguser, primary_chart):
    dashboard = Dashboard.objects.create(
        title="Impact Overview",
        description="Program KPIs and reach",
        dashboard_type="native",
        components={
            "chart-1": {
                "id": "chart-1",
                "type": "chart",
                "config": {"chartId": primary_chart.id},
            }
        },
        created_by=orguser,
        last_modified_by=orguser,
        org=org,
    )
    yield dashboard
    dashboard.delete()


@pytest.fixture
def related_dashboard(org, orguser, related_chart):
    dashboard = Dashboard.objects.create(
        title="Funding Overview",
        description="Funding KPIs and donor mix",
        dashboard_type="native",
        components={
            "chart-2": {
                "id": "chart-2",
                "type": "chart",
                "config": {"chartId": related_chart.id},
            }
        },
        created_by=orguser,
        last_modified_by=orguser,
        org=org,
    )
    yield dashboard
    dashboard.delete()


def test_allowlist_adds_upstream_dbt_tables():
    """Allowlist should include chart tables and their upstream dbt lineage."""
    export_payload = {
        "dashboard": {"title": "Impact Overview"},
        "charts": [{"id": 1, "schema_name": "analytics", "table_name": "fact_reach"}],
    }
    manifest_json = {
        "nodes": {
            "model.dalgo.fact_reach": {
                "resource_type": "model",
                "schema": "analytics",
                "name": "fact_reach",
                "depends_on": {"nodes": ["model.dalgo.dim_program", "source.dalgo.raw_students"]},
            },
            "model.dalgo.dim_program": {
                "resource_type": "model",
                "schema": "analytics",
                "name": "dim_program",
                "depends_on": {"nodes": []},
            },
        },
        "sources": {
            "source.dalgo.raw_students": {
                "resource_type": "source",
                "schema": "raw",
                "name": "students",
            }
        },
    }

    allowlist = DashboardChatAllowlistBuilder.build(export_payload, manifest_json=manifest_json)

    assert allowlist.chart_tables == {"analytics.fact_reach"}
    assert "analytics.dim_program" in allowlist.upstream_tables
    assert "raw.students" in allowlist.allowed_tables
    assert allowlist.is_allowed("analytics.fact_reach") is True
    assert allowlist.is_unique_id_allowed("model.dalgo.dim_program") is True


def test_sql_guard_enforces_single_statement_allowlist_and_limit():
    """SQL guard should block unsafe queries and add a row limit when absent."""
    allowlist = DashboardChatAllowlist(allowed_tables={"analytics.program_reach"})
    guard = DashboardChatSqlGuard(allowlist=allowlist, max_rows=200)

    multi_statement = guard.validate(
        "SELECT * FROM analytics.program_reach; DELETE FROM analytics.program_reach"
    )
    assert multi_statement.is_valid is False
    assert multi_statement.errors == ["Multiple statements are not allowed"]

    disallowed_table = guard.validate("SELECT * FROM analytics.other_table")
    assert disallowed_table.is_valid is False
    assert any("not accessible" in error for error in disallowed_table.errors)

    allowed_query = guard.validate("SELECT COUNT(*) AS beneficiary_count FROM analytics.program_reach")
    assert allowed_query.is_valid is True
    assert allowed_query.sanitized_sql.endswith("LIMIT 200")
    assert any("No LIMIT clause found" in warning for warning in allowed_query.warnings)


def test_runtime_context_query_returns_citations_and_related_dashboards(
    org,
    primary_dashboard,
    related_dashboard,
):
    """Context questions should return citations and cross-dashboard suggestions."""
    transaction.commit()
    vector_store = FakeVectorStore(
        [
            DashboardChatVectorQueryResult(
                document_id="doc-dashboard-context",
                content="This dashboard tracks monthly reach across programs.",
                metadata={
                    "source_type": "dashboard_context",
                    "source_identifier": f"dashboard:{primary_dashboard.id}:context",
                    "dashboard_id": primary_dashboard.id,
                },
                distance=0.02,
            ),
            DashboardChatVectorQueryResult(
                document_id="doc-org-context",
                content="Dalgo supports NGO dashboards and program reporting.",
                metadata={
                    "source_type": "org_context",
                    "source_identifier": f"org:{org.id}:context",
                },
                distance=0.04,
            ),
            DashboardChatVectorQueryResult(
                document_id="doc-related-dashboard",
                content="This dashboard shows donor-wise funding and cashflow trends.",
                metadata={
                    "source_type": "dashboard_export",
                    "source_identifier": f"dashboard:{related_dashboard.id}:summary",
                    "dashboard_id": related_dashboard.id,
                },
                distance=0.05,
            ),
        ]
    )

    runtime = DashboardChatRuntime(
        vector_store=vector_store,
        llm_client=ContextOnlyLlm(),
        runtime_config=DashboardChatRuntimeConfig(
            retrieval_limit=6,
            related_dashboard_limit=2,
            max_query_rows=200,
            max_distinct_values=20,
            max_schema_tables=4,
        ),
    )

    response = runtime.run(
        org=org,
        dashboard_id=primary_dashboard.id,
        user_query="Explain the reach metric",
    )

    assert response.intent == DashboardChatIntent.CONTEXT_QUERY
    assert response.sql is None
    assert response.metadata["query_plan_mode"] == "context"
    assert len(response.citations) >= 2
    assert response.citations[0].source_type == "dashboard_context"
    assert response.related_dashboards[0].dashboard_id == related_dashboard.id
    assert response.related_dashboards[0].title == "Funding Overview"


def test_runtime_data_query_uses_distinct_values_before_sql_execution(
    org,
    primary_dashboard,
):
    """Data questions should fetch distinct values before generating and executing SQL."""
    transaction.commit()
    vector_store = FakeVectorStore(
        [
            DashboardChatVectorQueryResult(
                document_id="doc-dashboard-export",
                content="Chart id: 1. Data source: analytics.program_reach.",
                metadata={
                    "source_type": "dashboard_export",
                    "source_identifier": f"dashboard:{primary_dashboard.id}:chart:1",
                    "dashboard_id": primary_dashboard.id,
                },
                distance=0.01,
            )
        ]
    )
    fake_warehouse = FakeWarehouseTools()

    runtime = DashboardChatRuntime(
        vector_store=vector_store,
        llm_client=SqlPathLlm(),
        warehouse_tools_factory=lambda org: fake_warehouse,
        runtime_config=DashboardChatRuntimeConfig(
            retrieval_limit=6,
            related_dashboard_limit=2,
            max_query_rows=200,
            max_distinct_values=20,
            max_schema_tables=4,
        ),
    )

    response = runtime.run(
        org=org,
        dashboard_id=primary_dashboard.id,
        user_query="How many beneficiaries are in Education?",
        conversation_history=[
            DashboardChatConversationMessage(role="user", content="Show me beneficiary data")
        ],
    )

    assert fake_warehouse.distinct_requests == [("analytics.program_reach", "program_name", 20)]
    assert len(fake_warehouse.executed_sql) == 1
    assert "WHERE program_name = 'Education'" in fake_warehouse.executed_sql[0]
    assert response.intent == DashboardChatIntent.DATA_QUERY
    assert response.sql is not None
    assert response.metadata["query_plan_mode"] == "sql"
    assert any(citation.source_type == "warehouse_table" for citation in response.citations)
