"""Tests for the prototype-faithful dashboard chat runtime."""

from decimal import Decimal

import pytest
from django.contrib.auth.models import User
from django.core.cache import cache

from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.dashboard_chat.allowlist import (
    DashboardChatAllowlist,
    DashboardChatAllowlistBuilder,
)
from ddpui.core.dashboard_chat.config import DashboardChatRuntimeConfig, DashboardChatSourceConfig
from ddpui.core.dashboard_chat.runtime import DashboardChatRuntime
from ddpui.core.dashboard_chat.runtime_types import (
    DashboardChatConversationContext,
    DashboardChatConversationMessage,
    DashboardChatFollowUpContext,
    DashboardChatIntent,
    DashboardChatIntentDecision,
    DashboardChatRetrievedDocument,
    DashboardChatResponse,
)
from ddpui.core.dashboard_chat.sql_guard import DashboardChatSqlGuard
from ddpui.core.dashboard_chat.vector_documents import DashboardChatSourceType
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
        self.embed_query_calls = []

    def embed_query(self, query_text):
        self.embed_query_calls.append(query_text)
        return [0.1, 0.2, 0.3]

    def query(
        self,
        org_id,
        query_text,
        n_results=5,
        source_types=None,
        dashboard_id=None,
        query_embedding=None,
        collection_name=None,
    ):
        self.calls.append(
            {
                "org_id": org_id,
                "query_text": query_text,
                "n_results": n_results,
                "source_types": list(source_types) if source_types else [],
                "dashboard_id": dashboard_id,
                "query_embedding": query_embedding,
                "collection_name": collection_name,
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

    def usage_summary(self):
        return {}


class FakeWarehouseTools:
    """Warehouse stub that records schema, distinct, and execution calls."""

    def __init__(self):
        self.schema_requests = []
        self.distinct_requests = []
        self.executed_sql = []
        self.schemas = {
            "analytics.program_reach": self._schema_snippet(
                "analytics.program_reach",
                [
                    {"name": "program_name", "data_type": "text", "nullable": False},
                    {"name": "beneficiaries", "data_type": "integer", "nullable": False},
                ],
            ),
            "analytics.stg_program_reach": self._schema_snippet(
                "analytics.stg_program_reach",
                [
                    {"name": "program_name", "data_type": "text", "nullable": False},
                    {"name": "donor_type", "data_type": "text", "nullable": False},
                    {"name": "beneficiaries", "data_type": "integer", "nullable": False},
                ],
            ),
            "analytics.donor_funding_quarterly": self._schema_snippet(
                "analytics.donor_funding_quarterly",
                [
                    {"name": "quarter_label", "data_type": "text", "nullable": False},
                    {
                        "name": "total_realized_funding_usd",
                        "data_type": "numeric",
                        "nullable": False,
                    },
                    {"name": "donor_count", "data_type": "integer", "nullable": False},
                ],
            ),
            "analytics.stg_donor_funding_clean": self._schema_snippet(
                "analytics.stg_donor_funding_clean",
                [
                    {"name": "quarter_label", "data_type": "text", "nullable": False},
                    {"name": "donor_type", "data_type": "text", "nullable": False},
                    {"name": "realized_amount_usd", "data_type": "numeric", "nullable": False},
                    {"name": "donation_id", "data_type": "text", "nullable": False},
                    {"name": "is_realized", "data_type": "boolean", "nullable": False},
                ],
            ),
            "analytics.facilitator_effectiveness_quarterly": self._schema_snippet(
                "analytics.facilitator_effectiveness_quarterly",
                [
                    {"name": "quarter_label", "data_type": "text", "nullable": False},
                    {"name": "facilitator_name", "data_type": "text", "nullable": False},
                    {"name": "district_name", "data_type": "text", "nullable": False},
                    {"name": "program_area", "data_type": "text", "nullable": False},
                    {
                        "name": "cost_per_improved_outcome_usd",
                        "data_type": "numeric",
                        "nullable": False,
                    },
                ],
            ),
            "analytics.district_funding_efficiency_quarterly": self._schema_snippet(
                "analytics.district_funding_efficiency_quarterly",
                [
                    {"name": "quarter_label", "data_type": "text", "nullable": False},
                    {"name": "district_name", "data_type": "text", "nullable": False},
                    {"name": "program_area", "data_type": "text", "nullable": False},
                    {
                        "name": "spend_per_student_usd",
                        "data_type": "numeric",
                        "nullable": False,
                    },
                ],
            ),
        }

    def get_schema_snippets(self, tables):
        self.schema_requests.append(list(tables))
        return {
            table_name: self.schemas[table_name]
            for table_name in tables
            if table_name in self.schemas
        }

    def get_distinct_values(self, table_name, column_name, limit=50):
        self.distinct_requests.append((table_name, column_name, limit))
        if table_name == "analytics.program_reach" and column_name == "program_name":
            return ["Education", "Health"]
        if table_name == "analytics.stg_program_reach" and column_name == "donor_type":
            return ["Grant", "Corporate"]
        return []

    def execute_sql(self, sql):
        self.executed_sql.append(sql)
        if "SELECT email" in sql:
            raise AssertionError("PII queries should not reach warehouse execution")
        if (
            "analytics.stg_program_reach" in sql
            and "GROUP BY donor_type" in sql
            and "beneficiaries" in sql
        ):
            raise Exception(
                'column "analytics.stg_program_reach.beneficiaries" must appear in the GROUP BY clause or be used in an aggregate function'
            )
        if "analytics.stg_program_reach" in sql:
            return [
                {"donor_type": "Grant", "beneficiary_count": 80},
                {"donor_type": "Corporate", "beneficiary_count": 40},
            ]
        if "analytics.stg_donor_funding_clean" in sql and "GROUP BY quarter_label, donor_type" in sql:
            return [
                {
                    "quarter_label": "2025 Q1",
                    "donor_type": "Grant",
                    "total_realized_funding_usd": 258000,
                    "donor_count": 3,
                },
                {
                    "quarter_label": "2025 Q1",
                    "donor_type": "Corporate",
                    "total_realized_funding_usd": 35000,
                    "donor_count": 1,
                },
                {
                    "quarter_label": "2025 Q2",
                    "donor_type": "Grant",
                    "total_realized_funding_usd": 46000,
                    "donor_count": 2,
                },
                {
                    "quarter_label": "2025 Q2",
                    "donor_type": "Corporate",
                    "total_realized_funding_usd": 59000,
                    "donor_count": 2,
                },
            ]
        if (
            "analytics.facilitator_effectiveness_quarterly f" in sql
            and "analytics.district_funding_efficiency_quarterly d" in sql
        ):
            return [
                {
                    "facilitator_name": "Farah Ali",
                    "district_name": "South",
                    "program_area": "Literacy Boost",
                    "cost_per_improved_outcome_usd": 740.25,
                    "spend_per_student_usd": 158.4,
                }
            ]
        if "analytics.program_reach" in sql and "program_name = 'Education'" in sql:
            return [{"program_name": "Education", "beneficiary_count": 120}]
        if "COUNT(*) AS row_count" in sql:
            return [{"row_count": 42}]
        return [{"beneficiary_count": 120}]

    @staticmethod
    def _schema_snippet(table_name, columns):
        from ddpui.core.dashboard_chat.runtime_types import DashboardChatSchemaSnippet

        return DashboardChatSchemaSnippet(table_name=table_name, columns=columns)


class PrototypeLlmBase:
    """Base LLM stub implementing the runtime contract needed by the tool loop."""

    def __init__(self):
        self.turn = 0

    def get_prompt(self, prompt_key):
        return f"prompt:{prompt_key}"

    def usage_summary(self):
        return {}

    def compose_small_talk(self, user_query):
        return "Hi! I can help with your program data and metrics. What would you like to know?"


class ContextToolLoopLlm(PrototypeLlmBase):
    """LLM stub for a context-only question that still uses retrieval."""

    def classify_intent(self, *args, **kwargs):
        return DashboardChatIntentDecision(
            intent=DashboardChatIntent.QUERY_WITHOUT_SQL,
            confidence=0.9,
            reason="Needs metadata/context, not SQL",
        )

    def run_tool_loop_turn(self, *, messages, tools, tool_choice, operation):
        if self.turn == 0:
            self.turn += 1
            return {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call-1",
                        "name": "retrieve_docs",
                        "args": {"query": "Explain the reach metric", "types": ["context"]},
                    }
                ],
            }

        tool_messages = [message for message in messages if message["role"] == "tool"]
        assert any("doc-dashboard-context" in message["content"] for message in tool_messages)
        return {
            "content": "The reach metric shows how many beneficiaries were served over time.",
            "tool_calls": [],
        }


class SqlToolLoopLlm(PrototypeLlmBase):
    """LLM stub for a fresh SQL-backed question."""

    def classify_intent(self, *args, **kwargs):
        return DashboardChatIntentDecision(
            intent=DashboardChatIntent.QUERY_WITH_SQL,
            confidence=0.92,
            reason="Needs data analysis",
            force_tool_usage=True,
        )

    def run_tool_loop_turn(self, *, messages, tools, tool_choice, operation):
        responses = [
            {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call-1",
                        "name": "retrieve_docs",
                        "args": {"query": "How many beneficiaries are in Education?", "types": ["chart"]},
                    }
                ],
            },
            {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call-2",
                        "name": "get_schema_snippets",
                        "args": {"tables": ["analytics.program_reach"]},
                    }
                ],
            },
            {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call-3",
                        "name": "get_distinct_values",
                        "args": {
                            "table": "analytics.program_reach",
                            "column": "program_name",
                            "limit": 20,
                        },
                    }
                ],
            },
            {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call-4",
                        "name": "run_sql_query",
                        "args": {
                            "sql": (
                                "SELECT program_name, COUNT(*) AS beneficiary_count "
                                "FROM analytics.program_reach "
                                "WHERE program_name = 'Education' "
                                "GROUP BY program_name"
                            )
                        },
                    }
                ],
            },
        ]
        response = responses[self.turn]
        self.turn += 1
        return response


class FollowUpCorrectionLlm(PrototypeLlmBase):
    """LLM stub that corrects itself after the runtime rejects the wrong follow-up table/column choice."""

    def classify_intent(self, *args, **kwargs):
        return DashboardChatIntentDecision(
            intent=DashboardChatIntent.FOLLOW_UP_SQL,
            confidence=0.95,
            reason="User is modifying the previous SQL result",
            force_tool_usage=True,
            follow_up_context=DashboardChatFollowUpContext(
                is_follow_up=True,
                follow_up_type="add_dimension",
                reusable_elements={
                    "previous_sql": "SELECT COUNT(*) FROM analytics.program_reach",
                    "previous_tables": ["analytics.program_reach"],
                },
                modification_instruction="Split the previous result by donor_type",
            ),
        )

    def run_tool_loop_turn(self, *, messages, tools, tool_choice, operation):
        if self.turn == 0:
            self.turn += 1
            return {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call-1",
                        "name": "get_schema_snippets",
                        "args": {"tables": ["analytics.program_reach"]},
                    }
                ],
            }
        if self.turn == 1:
            self.turn += 1
            return {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call-2",
                        "name": "run_sql_query",
                        "args": {
                            "sql": (
                                "SELECT donor_type, COUNT(*) AS beneficiary_count "
                                "FROM analytics.program_reach "
                                "GROUP BY donor_type"
                            )
                        },
                    }
                ],
            }
        if self.turn == 2:
            tool_messages = [message for message in messages if message["role"] == "tool"]
            assert any("column_not_in_table" in message["content"] for message in tool_messages)
            assert any("analytics.stg_program_reach" in message["content"] for message in tool_messages)
            self.turn += 1
            return {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call-3",
                        "name": "run_sql_query",
                        "args": {
                            "sql": (
                                "SELECT donor_type, COUNT(*) AS beneficiary_count "
                                "FROM analytics.stg_program_reach "
                                "GROUP BY donor_type"
                            )
                        },
                    },
                ],
            }
        raise AssertionError("Follow-up correction LLM exceeded expected turns")


class FollowUpDimensionGuardLlm(PrototypeLlmBase):
    """LLM stub that first ignores the requested dimension, then corrects after the guard fires."""

    def classify_intent(self, *args, **kwargs):
        return DashboardChatIntentDecision(
            intent=DashboardChatIntent.FOLLOW_UP_SQL,
            confidence=0.95,
            reason="User is modifying the previous SQL result",
            force_tool_usage=True,
            follow_up_context=DashboardChatFollowUpContext(
                is_follow_up=True,
                follow_up_type="add_dimension",
                reusable_elements={
                    "previous_sql": "SELECT quarter_label, total_realized_funding_usd FROM analytics.donor_funding_quarterly",
                    "previous_tables": ["analytics.donor_funding_quarterly"],
                },
                modification_instruction="split by donor_type",
            ),
        )

    def run_tool_loop_turn(self, *, messages, tools, tool_choice, operation):
        if self.turn == 0:
            self.turn += 1
            return {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call-1",
                        "name": "get_schema_snippets",
                        "args": {"tables": ["analytics.donor_funding_quarterly"]},
                    }
                ],
            }
        if self.turn == 1:
            self.turn += 1
            return {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call-2",
                        "name": "run_sql_query",
                        "args": {
                            "sql": (
                                "SELECT quarter_label, SUM(total_realized_funding_usd) AS total_realized_funding_usd "
                                "FROM analytics.donor_funding_quarterly "
                                "WHERE quarter_label IN ('2025 Q1', '2025 Q2') "
                                "GROUP BY quarter_label ORDER BY quarter_label"
                            )
                        },
                    }
                ],
            }
        if self.turn == 2:
            tool_messages = [message for message in messages if message["role"] == "tool"]
            assert any("requested_dimension_missing" in message["content"] for message in tool_messages)
            self.turn += 1
            return {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call-3",
                        "name": "list_tables_by_keyword",
                        "args": {"keyword": "donor_funding", "limit": 10},
                    }
                ],
            }
        if self.turn == 3:
            self.turn += 1
            return {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call-4",
                        "name": "get_schema_snippets",
                        "args": {"tables": ["analytics.stg_donor_funding_clean"]},
                    }
                ],
            }
        if self.turn == 4:
            self.turn += 1
            return {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call-5",
                        "name": "run_sql_query",
                        "args": {
                            "sql": (
                                "SELECT quarter_label, donor_type, SUM(realized_amount_usd) AS total_realized_funding_usd, "
                                "COUNT(DISTINCT donation_id) AS donor_count "
                                "FROM analytics.stg_donor_funding_clean "
                                "WHERE quarter_label IN ('2025 Q1', '2025 Q2') AND is_realized = TRUE "
                                "GROUP BY quarter_label, donor_type ORDER BY quarter_label, donor_type"
                            )
                        },
                    }
                ],
            }
        raise AssertionError("Follow-up dimension guard LLM exceeded expected turns")


class PiiToolLoopLlm(PrototypeLlmBase):
    """LLM stub that needs a safe failure response after SQL guard rejection."""

    def classify_intent(self, *args, **kwargs):
        return DashboardChatIntentDecision(
            intent=DashboardChatIntent.QUERY_WITH_SQL,
            confidence=0.9,
            reason="Needs data analysis",
            force_tool_usage=True,
        )

    def run_tool_loop_turn(self, *, messages, tools, tool_choice, operation):
        if self.turn == 0:
            self.turn += 1
            return {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call-1",
                        "name": "run_sql_query",
                        "args": {"sql": "SELECT email FROM analytics.program_reach LIMIT 25"},
                    }
                ],
            }

        tool_messages = [message for message in messages if message["role"] == "tool"]
        assert any("aggregate the results or rephrase" in message["content"] for message in tool_messages)
        return {
            "content": "I couldn't answer that safely. Please aggregate the results or rephrase.",
            "tool_calls": [],
        }


class SmallTalkLlm(PrototypeLlmBase):
    """LLM stub for prototype-style small talk."""

    def classify_intent(self, *args, **kwargs):
        return DashboardChatIntentDecision(
            intent=DashboardChatIntent.SMALL_TALK,
            confidence=0.97,
            reason="Greeting or pleasantry",
        )

    def run_tool_loop_turn(self, *, messages, tools, tool_choice, operation):
        raise AssertionError("Small talk should not enter the tool loop")


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

def test_extract_conversation_context_reads_previous_sql_payload():
    """Follow-up routing should recover prior SQL context from assistant payloads."""
    conversation_context = DashboardChatRuntime._extract_conversation_context(
        [
            DashboardChatConversationMessage(role="user", content="How many beneficiaries do we have?"),
            DashboardChatConversationMessage(
                role="assistant",
                content="There are 120 beneficiaries.",
                payload={
                    "intent": "query_with_sql",
                    "sql": "SELECT COUNT(*) FROM analytics.program_reach",
                    "metadata": {"query_plan_tables": ["analytics.program_reach"]},
                    "citations": [
                        {
                            "source_type": "warehouse_table",
                            "table_name": "analytics.program_reach",
                        }
                    ],
                },
            ),
        ]
    )

    assert conversation_context.last_sql_query == "SELECT COUNT(*) FROM analytics.program_reach"
    assert conversation_context.last_tables_used == ["analytics.program_reach"]
    assert conversation_context.last_response_type == "sql_result"
    assert conversation_context.last_intent == "query_with_sql"


def test_seed_distinct_cache_reuses_previous_text_filters(primary_dashboard):
    """Follow-up turns should reuse text-filter validations from the previous successful SQL."""
    runtime = DashboardChatRuntime(
        vector_store=FakeVectorStore([]),
        llm_client=SmallTalkLlm(),
    )
    state = {
        "dashboard_id": primary_dashboard.id,
        "conversation_context": DashboardChatRuntime._extract_conversation_context(
            [
                DashboardChatConversationMessage(
                    role="assistant",
                    content="Previous answer",
                    payload={
                        "intent": "query_with_sql",
                        "sql": (
                            "SELECT quarter_label, SUM(total_realized_funding_usd) "
                            "FROM analytics.donor_funding_quarterly "
                            "WHERE quarter_label IN ('2025 Q1', '2025 Q2') "
                            "GROUP BY quarter_label"
                        ),
                    },
                )
            ]
        ),
    }
    execution_context = {"distinct_cache": set()}

    runtime._seed_distinct_cache_from_previous_sql(state, execution_context)

    assert (
        "analytics.donor_funding_quarterly",
        "quarter_label",
        "2025 q1",
    ) in execution_context["distinct_cache"]
    assert ("*", "quarter_label", "2025 q2") in execution_context["distinct_cache"]


def test_missing_distinct_accepts_previous_filter_validation_on_upstream_table(primary_dashboard):
    """Follow-up SQL should reuse validated text filters even after moving to an upstream table."""
    runtime = DashboardChatRuntime(
        vector_store=FakeVectorStore([]),
        llm_client=SmallTalkLlm(),
    )
    state = {
        "allowlist": DashboardChatAllowlist(
            allowed_tables={
                "analytics.donor_funding_quarterly",
                "analytics.stg_donor_funding_clean",
            }
        ),
        "conversation_context": DashboardChatRuntime._extract_conversation_context(
            [
                DashboardChatConversationMessage(
                    role="assistant",
                    content="Previous answer",
                    payload={
                        "intent": "query_with_sql",
                        "sql": (
                            "SELECT quarter_label, total_realized_funding_usd "
                            "FROM analytics.donor_funding_quarterly "
                            "WHERE quarter_label IN ('2025 Q1', '2025 Q2') "
                            "ORDER BY quarter_label"
                        ),
                    },
                )
            ]
        ),
        "org": primary_dashboard.org,
    }
    execution_context = {
        "distinct_cache": set(),
        "schema_cache": {
            "analytics.stg_donor_funding_clean": FakeWarehouseTools._schema_snippet(
                "analytics.stg_donor_funding_clean",
                [
                    {"name": "quarter_label", "data_type": "text", "nullable": False},
                    {"name": "donor_type", "data_type": "text", "nullable": False},
                    {"name": "realized_amount_usd", "data_type": "numeric", "nullable": False},
                    {"name": "is_realized", "data_type": "boolean", "nullable": False},
                ],
            )
        },
        "warehouse_tools": None,
    }

    runtime._seed_distinct_cache_from_previous_sql(state, execution_context)
    missing = runtime._missing_distinct(
        (
            "SELECT quarter_label, donor_type, SUM(realized_amount_usd) AS total_realized_funding_usd "
            "FROM analytics.stg_donor_funding_clean "
            "WHERE quarter_label IN ('2025 Q1', '2025 Q2') "
            "AND is_realized = TRUE "
            "GROUP BY quarter_label, donor_type"
        ),
        state,
        execution_context,
    )

    assert missing == []


def test_get_distinct_values_returns_column_correction_for_wrong_table(primary_dashboard):
    """Follow-up correction should surface candidate tables when a distinct lookup targets the wrong table."""
    runtime = DashboardChatRuntime(
        vector_store=FakeVectorStore([]),
        llm_client=SmallTalkLlm(),
    )
    state = {
        "allowlist": DashboardChatAllowlist(
            allowed_tables={
                "analytics.donor_funding_quarterly",
                "analytics.stg_donor_funding_clean",
            }
        ),
        "org": primary_dashboard.org,
    }
    execution_context = {
        "schema_cache": {
            "analytics.donor_funding_quarterly": FakeWarehouseTools._schema_snippet(
                "analytics.donor_funding_quarterly",
                [
                    {"name": "quarter_label", "data_type": "text", "nullable": False},
                    {"name": "total_realized_funding_usd", "data_type": "numeric", "nullable": False},
                ],
            ),
            "analytics.stg_donor_funding_clean": FakeWarehouseTools._schema_snippet(
                "analytics.stg_donor_funding_clean",
                [
                    {"name": "quarter_label", "data_type": "text", "nullable": False},
                    {"name": "donor_type", "data_type": "text", "nullable": False},
                    {"name": "realized_amount_usd", "data_type": "numeric", "nullable": False},
                ],
            ),
        },
        "warehouse_tools": None,
    }

    result = runtime._tool_get_distinct_values(
        {
            "table": "analytics.donor_funding_quarterly",
            "column": "donor_type",
            "limit": 50,
        },
        state,
        execution_context,
    )

    assert result["error"] == "column_not_in_table"
    assert result["table"] == "analytics.donor_funding_quarterly"
    assert result["column"] == "donor_type"
    assert "analytics.stg_donor_funding_clean" in result["candidates"]


def test_missing_columns_check_ignores_boolean_literals(primary_dashboard):
    """Boolean literals in WHERE clauses should not be misread as missing columns."""
    runtime = DashboardChatRuntime(
        vector_store=FakeVectorStore([]),
        llm_client=SmallTalkLlm(),
    )
    state = {
        "allowlist": DashboardChatAllowlist(
            allowed_tables={"analytics.stg_donor_funding_clean"}
        ),
        "org": primary_dashboard.org,
    }
    execution_context = {
        "schema_cache": {
            "analytics.stg_donor_funding_clean": FakeWarehouseTools._schema_snippet(
                "analytics.stg_donor_funding_clean",
                [
                    {"name": "quarter_label", "data_type": "text", "nullable": False},
                    {"name": "donor_type", "data_type": "text", "nullable": False},
                    {"name": "realized_amount_usd", "data_type": "numeric", "nullable": False},
                    {"name": "donation_id", "data_type": "text", "nullable": False},
                    {"name": "is_realized", "data_type": "boolean", "nullable": False},
                ],
            )
        },
        "warehouse_tools": None,
    }

    missing = runtime._missing_columns_in_primary_table(
        sql=(
            "SELECT quarter_label, donor_type, SUM(realized_amount_usd) AS total_realized_funding_usd, "
            "COUNT(DISTINCT donation_id) AS donor_count "
            "FROM analytics.stg_donor_funding_clean "
            "WHERE quarter_label IN ('2025 Q1', '2025 Q2') AND is_realized = TRUE "
            "GROUP BY quarter_label, donor_type ORDER BY quarter_label, donor_type LIMIT 200"
        ),
        state=state,
        execution_context=execution_context,
    )

    assert missing is None


def test_run_sql_keeps_join_tables_intact(primary_dashboard):
    """Join queries should execute the model's SQL as written and let the tool loop correct errors."""
    runtime = DashboardChatRuntime(
        vector_store=FakeVectorStore([]),
        llm_client=SmallTalkLlm(),
        warehouse_tools_factory=lambda org: FakeWarehouseTools(),
    )
    state = {
        "allowlist": DashboardChatAllowlist(
            allowed_tables={
                "analytics.facilitator_effectiveness_quarterly",
                "analytics.district_funding_efficiency_quarterly",
            }
        ),
        "org": primary_dashboard.org,
        "intent_decision": DashboardChatIntentDecision(
            intent=DashboardChatIntent.QUERY_WITH_SQL,
            confidence=0.9,
            reason="Join-heavy data analysis",
            force_tool_usage=True,
        ),
        "user_query": "Join facilitator outcomes to district funding efficiency.",
    }
    execution_context = {
        "schema_cache": {
            "analytics.facilitator_effectiveness_quarterly": FakeWarehouseTools().schemas[
                "analytics.facilitator_effectiveness_quarterly"
            ],
            "analytics.district_funding_efficiency_quarterly": FakeWarehouseTools().schemas[
                "analytics.district_funding_efficiency_quarterly"
            ],
        },
        "warehouse_tools": FakeWarehouseTools(),
        "distinct_cache": {("*", "quarter_label")},
        "last_sql": None,
        "last_sql_results": None,
        "last_sql_validation": None,
        "warnings": [],
    }

    result = runtime._run_sql_with_distinct_guard(
        {
            "sql": (
                "SELECT "
                "f.facilitator_name, f.district_name, f.program_area, "
                "f.cost_per_improved_outcome_usd, d.spend_per_student_usd "
                "FROM analytics.facilitator_effectiveness_quarterly f "
                "JOIN analytics.district_funding_efficiency_quarterly d "
                "ON f.quarter_label = d.quarter_label "
                "AND f.district_name = d.district_name "
                "AND f.program_area = d.program_area "
                "WHERE f.quarter_label = '2025 Q2' "
                "ORDER BY f.cost_per_improved_outcome_usd ASC"
            )
        },
        state,
        execution_context,
    )

    assert result["success"] is True
    assert "analytics.facilitator_effectiveness_quarterly f" in result["sql_used"]
    assert "analytics.district_funding_efficiency_quarterly d" in result["sql_used"]


def test_missing_distinct_resolves_join_filter_to_qualified_table(primary_dashboard):
    """Distinct validation should inspect the joined table referenced by a qualified WHERE filter."""
    runtime = DashboardChatRuntime(
        vector_store=FakeVectorStore([]),
        llm_client=SmallTalkLlm(),
    )
    state = {
        "allowlist": DashboardChatAllowlist(
            allowed_tables={
                "analytics.facilitator_effectiveness_quarterly",
                "analytics.district_funding_efficiency_quarterly",
            }
        ),
        "org": primary_dashboard.org,
    }
    execution_context = {
        "schema_cache": {
            "analytics.facilitator_effectiveness_quarterly": FakeWarehouseTools().schemas[
                "analytics.facilitator_effectiveness_quarterly"
            ],
            "analytics.district_funding_efficiency_quarterly": FakeWarehouseTools().schemas[
                "analytics.district_funding_efficiency_quarterly"
            ],
        },
        "warehouse_tools": None,
        "distinct_cache": set(),
    }

    missing = runtime._missing_distinct(
        (
            "SELECT f.facilitator_name, d.spend_per_student_usd "
            "FROM analytics.facilitator_effectiveness_quarterly f "
            "JOIN analytics.district_funding_efficiency_quarterly d "
            "ON f.quarter_label = d.quarter_label "
            "AND f.district_name = d.district_name "
            "AND f.program_area = d.program_area "
            "WHERE d.program_area = 'Literacy'"
        ),
        state,
        execution_context,
    )

    assert missing == [
        {
            "table": "analytics.district_funding_efficiency_quarterly",
            "column": "program_area",
            "value": "Literacy",
        }
    ]


def test_missing_columns_check_is_join_aware_for_qualified_columns(primary_dashboard):
    """Qualified join columns should be validated against the referenced joined table."""
    runtime = DashboardChatRuntime(
        vector_store=FakeVectorStore([]),
        llm_client=SmallTalkLlm(),
    )
    state = {
        "allowlist": DashboardChatAllowlist(
            allowed_tables={
                "analytics.facilitator_effectiveness_quarterly",
                "analytics.district_funding_efficiency_quarterly",
            }
        ),
        "org": primary_dashboard.org,
    }
    execution_context = {
        "schema_cache": {
            "analytics.facilitator_effectiveness_quarterly": FakeWarehouseTools().schemas[
                "analytics.facilitator_effectiveness_quarterly"
            ],
            "analytics.district_funding_efficiency_quarterly": FakeWarehouseTools().schemas[
                "analytics.district_funding_efficiency_quarterly"
            ],
        },
        "warehouse_tools": None,
    }

    missing = runtime._missing_columns_in_primary_table(
        sql=(
            "SELECT f.facilitator_name, d.fake_dimension "
            "FROM analytics.facilitator_effectiveness_quarterly f "
            "JOIN analytics.district_funding_efficiency_quarterly d "
            "ON f.quarter_label = d.quarter_label "
            "AND f.district_name = d.district_name "
            "AND f.program_area = d.program_area "
            "WHERE f.quarter_label = '2025 Q2'"
        ),
        state=state,
        execution_context=execution_context,
    )

    assert missing["error"] == "column_not_in_table"
    assert missing["table"] == "analytics.district_funding_efficiency_quarterly"
    assert missing["column"] == "fake_dimension"


def test_missing_columns_check_ignores_order_by_select_alias(primary_dashboard):
    """ORDER BY aliases from the SELECT clause should not be treated as missing physical columns."""
    runtime = DashboardChatRuntime(
        vector_store=FakeVectorStore([]),
        llm_client=SmallTalkLlm(),
    )
    state = {
        "allowlist": DashboardChatAllowlist(
            allowed_tables={"analytics.facilitator_effectiveness_quarterly"}
        ),
        "org": primary_dashboard.org,
    }
    execution_context = {
        "schema_cache": {
            "analytics.facilitator_effectiveness_quarterly": FakeWarehouseTools._schema_snippet(
                "analytics.facilitator_effectiveness_quarterly",
                [
                    {"name": "quarter_label", "data_type": "text", "nullable": False},
                    {"name": "facilitator_name", "data_type": "text", "nullable": False},
                    {
                        "name": "cost_per_improved_outcome_usd",
                        "data_type": "numeric",
                        "nullable": False,
                    },
                ],
            )
        },
        "warehouse_tools": None,
    }

    missing = runtime._missing_columns_in_primary_table(
        sql=(
            "SELECT facilitator_name, AVG(cost_per_improved_outcome_usd) AS avg_cost_per_improved_outcome "
            "FROM analytics.facilitator_effectiveness_quarterly "
            "WHERE quarter_label = '2025 Q2' "
            "GROUP BY facilitator_name "
            "ORDER BY avg_cost_per_improved_outcome ASC "
            "LIMIT 1"
        ),
        state=state,
        execution_context=execution_context,
    )

    assert missing is None


def test_small_talk_turn_returns_without_citations(primary_dashboard):
    """Greeting turns should skip retrieval and finalize cleanly."""
    runtime = DashboardChatRuntime(
        vector_store=FakeVectorStore([]),
        llm_client=SmallTalkLlm(),
    )

    response = runtime.run(
        org=primary_dashboard.org,
        dashboard_id=primary_dashboard.id,
        user_query="hello",
    )

    assert response.intent == DashboardChatIntent.SMALL_TALK
    assert "this dashboard" in response.answer_text
    assert response.citations == []
    assert response.warnings == []
    assert response.metadata["allowlisted_tables"] == ["analytics.program_reach"]


def test_runtime_query_without_sql_returns_dashboard_scoped_citations(
    org,
    primary_dashboard,
):
    """Context questions should use retrieval without suggesting other dashboards."""
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
        ]
    )

    runtime = DashboardChatRuntime(
        vector_store=vector_store,
        llm_client=ContextToolLoopLlm(),
        runtime_config=DashboardChatRuntimeConfig(
            retrieval_limit=6,
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

    assert response.intent == DashboardChatIntent.QUERY_WITHOUT_SQL
    assert response.sql is None
    assert len(response.citations) >= 2
    assert response.citations[0].source_type in {"dashboard_context", "org_context"}
    assert response.tool_calls[0]["name"] == "retrieve_docs"


def test_runtime_prompt_messages_do_not_inline_raw_human_context(primary_dashboard):
    """Raw org/dashboard markdown should reach the model through retrieval, not prompt duplication."""
    runtime = DashboardChatRuntime(
        vector_store=FakeVectorStore([]),
        llm_client=SmallTalkLlm(),
    )

    new_query_messages = runtime._build_new_query_messages(
        {
            "user_query": "Explain the reach metric",
            "human_context": "Organization context: duplicated markdown",
        }
    )
    follow_up_messages = runtime._build_follow_up_messages(
        {
            "user_query": "Explain that metric",
            "human_context": "Organization context: duplicated markdown",
            "conversation_context": DashboardChatRuntime._extract_conversation_context([]),
        }
    )

    assert new_query_messages[0]["content"] == "prompt:new_query_system"
    assert "Human context" not in follow_up_messages[0]["content"]


def test_runtime_query_with_sql_uses_distinct_values_before_sql_execution(
    org,
    primary_dashboard,
):
    """Data questions should fetch distinct values before executing SQL."""
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
        llm_client=SqlToolLoopLlm(),
        warehouse_tools_factory=lambda org: fake_warehouse,
        runtime_config=DashboardChatRuntimeConfig(
            retrieval_limit=6,
            max_query_rows=200,
            max_distinct_values=20,
            max_schema_tables=4,
        ),
    )

    response = runtime.run(
        org=org,
        dashboard_id=primary_dashboard.id,
        user_query="How many beneficiaries are in Education?",
    )

    assert fake_warehouse.distinct_requests == [("analytics.program_reach", "program_name", 20)]


def test_runtime_reuses_session_snapshot_across_turns(org, primary_dashboard):
    """Session snapshots should freeze dashboard context and reuse schema within one chat."""
    cache.clear()
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
    def build_runtime():
        return DashboardChatRuntime(
            vector_store=vector_store,
            llm_client=SqlToolLoopLlm(),
            warehouse_tools_factory=lambda org: fake_warehouse,
            runtime_config=DashboardChatRuntimeConfig(
                retrieval_limit=6,
                max_query_rows=200,
                max_distinct_values=20,
                max_schema_tables=4,
            ),
        )

    first_response = build_runtime().run(
        org=org,
        dashboard_id=primary_dashboard.id,
        session_id="session-cache-test",
        user_query="How many beneficiaries are in Education?",
    )
    second_response = build_runtime().run(
        org=org,
        dashboard_id=primary_dashboard.id,
        session_id="session-cache-test",
        user_query="How many beneficiaries are in Education?",
    )

    assert first_response.intent == DashboardChatIntent.QUERY_WITH_SQL
    assert second_response.intent == DashboardChatIntent.QUERY_WITH_SQL
    assert fake_warehouse.schema_requests == [["analytics.program_reach"]]
    assert "WHERE program_name = 'Education'" in fake_warehouse.executed_sql[0]
    assert first_response.sql is not None
    assert second_response.sql is not None
    assert '"beneficiary_count": 120' in first_response.answer_text
    assert any(citation.source_type == "warehouse_table" for citation in first_response.citations)
    assert [call["name"] for call in first_response.tool_calls] == [
        "retrieve_docs",
        "get_schema_snippets",
        "get_distinct_values",
        "run_sql_query",
    ]


def test_runtime_persists_distinct_validations_in_session_snapshot(org, primary_dashboard):
    """Validated text filter values should survive across turns in the same chat session."""
    cache.clear()
    runtime = DashboardChatRuntime(
        vector_store=FakeVectorStore([]),
        llm_client=SmallTalkLlm(),
    )
    session_id = "session-distinct-cache-test"
    snapshot_state = {
        "org": org,
        "dashboard_id": primary_dashboard.id,
        "session_id": session_id,
    }

    snapshot = runtime._load_session_snapshot(snapshot_state)
    state = {
        "org": org,
        "dashboard_id": primary_dashboard.id,
        "session_id": session_id,
        "allowlist": snapshot["allowlist"],
        "session_distinct_cache": snapshot["distinct_cache"],
    }
    execution_context = {"distinct_cache": set(snapshot["distinct_cache"])}

    runtime._record_validated_distinct_values(
        state=state,
        execution_context=execution_context,
        table_name="analytics.program_reach",
        column_name="program_name",
        values=["Education"],
    )

    reloaded_snapshot = runtime._load_session_snapshot(snapshot_state)
    missing = runtime._missing_distinct(
        "SELECT COUNT(*) FROM analytics.program_reach WHERE program_name = 'Education'",
        {
            "allowlist": snapshot["allowlist"],
            "org": org,
        },
        {
            "distinct_cache": set(reloaded_snapshot["distinct_cache"]),
            "schema_cache": {
                "analytics.program_reach": FakeWarehouseTools._schema_snippet(
                    "analytics.program_reach",
                    [
                        {"name": "program_name", "data_type": "text", "nullable": False},
                        {"name": "beneficiaries", "data_type": "integer", "nullable": False},
                    ],
                )
            },
            "warehouse_tools": None,
        },
    )

    assert (
        "analytics.program_reach",
        "program_name",
        "education",
    ) in reloaded_snapshot["distinct_cache"]
    assert missing == []


def test_runtime_follow_up_sql_corrects_after_failed_sql_attempt(
    monkeypatch,
    org,
    primary_dashboard,
):
    """Follow-up SQL turns should self-correct within the prototype tool loop."""
    vector_store = FakeVectorStore([])
    fake_warehouse = FakeWarehouseTools()

    manifest_json = {
        "nodes": {
            "model.dalgo.program_reach": {
                "resource_type": "model",
                "schema": "analytics",
                "name": "program_reach",
                "depends_on": {"nodes": ["model.dalgo.stg_program_reach"]},
            },
            "model.dalgo.stg_program_reach": {
                "resource_type": "model",
                "schema": "analytics",
                "name": "stg_program_reach",
                "depends_on": {"nodes": []},
            },
        },
        "sources": {},
    }
    monkeypatch.setattr(
        DashboardChatAllowlistBuilder,
        "load_manifest_json",
        staticmethod(lambda orgdbt: manifest_json),
    )

    runtime = DashboardChatRuntime(
        vector_store=vector_store,
        llm_client=FollowUpCorrectionLlm(),
        warehouse_tools_factory=lambda org: fake_warehouse,
    )

    response = runtime.run(
        org=org,
        dashboard_id=primary_dashboard.id,
        user_query="Now split that by donor type.",
        conversation_history=[
            DashboardChatConversationMessage(role="user", content="How many beneficiaries do we have?"),
            DashboardChatConversationMessage(
                role="assistant",
                content="There are 120 beneficiaries.",
                payload={
                    "intent": "query_with_sql",
                    "sql": "SELECT COUNT(*) FROM analytics.program_reach",
                    "metadata": {"query_plan_tables": ["analytics.program_reach"]},
                },
            ),
        ],
    )

    assert response.intent == DashboardChatIntent.FOLLOW_UP_SQL
    assert response.sql is not None
    assert "analytics.stg_program_reach" in response.sql
    assert len(fake_warehouse.executed_sql) == 1
    run_sql_calls = [
        tool_call for tool_call in response.tool_calls if tool_call["name"] == "run_sql_query"
    ]
    assert run_sql_calls[0]["success"] is False
    assert run_sql_calls[-1]["success"] is True
    assert '"donor_type": "Grant"' in response.answer_text


def test_runtime_dbt_tools_use_compact_allowlisted_index():
    """Deterministic dbt tools should run from the compact allowlisted index, not a full manifest blob."""
    export_payload = {
        "dashboard": {"title": "Impact Overview"},
        "charts": [{"id": 1, "schema_name": "analytics", "table_name": "program_reach"}],
    }
    manifest_json = {
        "nodes": {
            "model.dalgo.program_reach": {
                "resource_type": "model",
                "schema": "analytics",
                "name": "program_reach",
                "description": "Program-level reach fact table",
                "columns": {
                    "program_name": {
                        "name": "program_name",
                        "description": "Program dimension",
                        "data_type": "text",
                    }
                },
                "depends_on": {"nodes": ["model.dalgo.stg_program_reach"]},
            },
            "model.dalgo.stg_program_reach": {
                "resource_type": "model",
                "schema": "analytics",
                "name": "stg_program_reach",
                "description": "Staging model for program reach",
                "columns": {},
                "depends_on": {"nodes": []},
            },
        },
        "sources": {},
        "parent_map": {
            "model.dalgo.program_reach": ["model.dalgo.stg_program_reach"],
            "model.dalgo.stg_program_reach": [],
        },
        "child_map": {
            "model.dalgo.program_reach": [],
            "model.dalgo.stg_program_reach": ["model.dalgo.program_reach"],
        },
    }
    allowlist = DashboardChatAllowlistBuilder.build(export_payload, manifest_json=manifest_json)
    dbt_index = DashboardChatAllowlistBuilder.build_dbt_index(manifest_json, allowlist)
    runtime = DashboardChatRuntime(
        vector_store=FakeVectorStore([]),
        llm_client=SmallTalkLlm(),
    )
    state = {
        "allowlist": allowlist,
        "dbt_index": dbt_index,
    }

    search_result = runtime._tool_search_dbt_models(
        {"query": "program reach", "limit": 5},
        state,
        {},
    )
    info_result = runtime._tool_get_dbt_model_info(
        {"model_name": "analytics.program_reach"},
        state,
        {},
    )

    assert search_result["count"] >= 1
    assert {
        model["table"] for model in search_result["models"]
    } <= {"analytics.program_reach", "analytics.stg_program_reach"}
    assert info_result["model"] == "program_reach"
    assert info_result["upstream"] == ["analytics.stg_program_reach"]


def test_runtime_follow_up_sql_rejects_query_that_ignores_requested_dimension(
    monkeypatch,
    org,
    primary_dashboard,
):
    """Follow-up add-dimension turns should not succeed without using the requested dimension."""
    vector_store = FakeVectorStore([])
    fake_warehouse = FakeWarehouseTools()

    manifest_json = {
        "nodes": {
            "model.dalgo.donor_funding_quarterly": {
                "resource_type": "model",
                "schema": "analytics",
                "name": "donor_funding_quarterly",
                "depends_on": {"nodes": ["model.dalgo.stg_donor_funding_clean"]},
            },
            "model.dalgo.stg_donor_funding_clean": {
                "resource_type": "model",
                "schema": "analytics",
                "name": "stg_donor_funding_clean",
                "depends_on": {"nodes": []},
            },
        },
        "sources": {},
    }
    monkeypatch.setattr(
        DashboardChatAllowlistBuilder,
        "load_manifest_json",
        staticmethod(lambda orgdbt: manifest_json),
    )
    monkeypatch.setattr(
        DashboardChatAllowlistBuilder,
        "build",
        staticmethod(
            lambda dashboard_export, manifest_json: DashboardChatAllowlist(
                allowed_tables={
                    "analytics.donor_funding_quarterly",
                    "analytics.stg_donor_funding_clean",
                }
            )
        ),
    )

    runtime = DashboardChatRuntime(
        vector_store=vector_store,
        llm_client=FollowUpDimensionGuardLlm(),
        warehouse_tools_factory=lambda org: fake_warehouse,
    )

    response = runtime.run(
        org=org,
        dashboard_id=primary_dashboard.id,
        user_query="Now split that by donor type.",
        conversation_history=[
            DashboardChatConversationMessage(role="user", content="How many beneficiaries do we have?"),
            DashboardChatConversationMessage(
                role="assistant",
                content="There are 120 beneficiaries.",
                payload={
                    "intent": "query_with_sql",
                    "sql": (
                        "SELECT quarter_label, total_realized_funding_usd "
                        "FROM analytics.donor_funding_quarterly "
                        "WHERE quarter_label IN ('2025 Q1', '2025 Q2') "
                        "ORDER BY quarter_label"
                    ),
                    "metadata": {
                        "query_plan_tables": ["analytics.donor_funding_quarterly"]
                    },
                },
            ),
        ],
    )

    assert response.intent == DashboardChatIntent.FOLLOW_UP_SQL
    assert response.sql is not None
    assert "analytics.stg_donor_funding_clean" in response.sql
    assert any(call.get("error") == "requested_dimension_missing" for call in response.tool_calls)
    assert '"Grant"' in response.answer_text


def test_follow_up_dimension_validation_accepts_structural_granularity_change(primary_dashboard):
    """Follow-up add-dimension validation should accept structural SQL rewrites, not only exact token reuse."""
    runtime = DashboardChatRuntime(
        vector_store=FakeVectorStore([]),
        llm_client=SmallTalkLlm(),
    )
    state = {
        "intent_decision": DashboardChatIntentDecision(
            intent=DashboardChatIntent.FOLLOW_UP_SQL,
            confidence=0.9,
            reason="Follow-up SQL",
            force_tool_usage=True,
            follow_up_context=DashboardChatFollowUpContext(
                is_follow_up=True,
                follow_up_type="add_dimension",
                modification_instruction="Now split that by donor type.",
            ),
        ),
        "conversation_context": DashboardChatConversationContext(
            last_sql_query=(
                "SELECT quarter_label, SUM(realized_amount_usd) AS total_realized_funding_usd "
                "FROM analytics.stg_donor_funding_clean "
                "WHERE quarter_label IN ('2025 Q1', '2025 Q2') "
                "GROUP BY quarter_label"
            ),
        ),
        "user_query": "Now split that by donor type.",
        "allowlist": DashboardChatAllowlist(
            allowed_tables={"analytics.stg_donor_funding_clean"}
        ),
        "org": primary_dashboard.org,
    }
    execution_context = {
        "schema_cache": {
            "analytics.stg_donor_funding_clean": FakeWarehouseTools._schema_snippet(
                "analytics.stg_donor_funding_clean",
                [
                    {"name": "quarter_label", "data_type": "text", "nullable": False},
                    {"name": "donor_type", "data_type": "text", "nullable": False},
                    {"name": "realized_amount_usd", "data_type": "numeric", "nullable": False},
                ],
            )
        },
        "warehouse_tools": None,
    }

    validation = runtime._validate_follow_up_dimension_usage(
        sql=(
            "SELECT quarter_label, COALESCE(donor_type, 'Unknown') AS donor_type, "
            "SUM(realized_amount_usd) AS total_realized_funding_usd "
            "FROM analytics.stg_donor_funding_clean "
            "WHERE quarter_label IN ('2025 Q1', '2025 Q2') "
            "GROUP BY quarter_label, COALESCE(donor_type, 'Unknown')"
        ),
        state=state,
        execution_context=execution_context,
    )

    assert validation is None


def test_runtime_rejects_row_level_pii_queries_before_execution(org, primary_dashboard):
    """Unsafe PII SQL should be rejected by the SQL guard before warehouse execution."""
    fake_warehouse = FakeWarehouseTools()
    runtime = DashboardChatRuntime(
        vector_store=FakeVectorStore([]),
        llm_client=PiiToolLoopLlm(),
        warehouse_tools_factory=lambda org: fake_warehouse,
    )

    response = runtime.run(
        org=org,
        dashboard_id=primary_dashboard.id,
        user_query="List email addresses for this dashboard",
    )

    assert fake_warehouse.executed_sql == []
    assert response.sql is None
    assert response.sql_results is None
    assert "aggregate the results or rephrase" in response.answer_text
    assert response.metadata["sql_guard_errors"] == [
        "Queries returning row-level sensitive data are not allowed. Please aggregate the results or rephrase."
    ]


def test_runtime_skips_disabled_source_types_during_retrieval(org, primary_dashboard):
    """Disabled source types should not be queried by the retrieve_docs tool."""
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
        ]
    )

    runtime = DashboardChatRuntime(
        vector_store=vector_store,
        llm_client=ContextToolLoopLlm(),
        source_config=DashboardChatSourceConfig(
            enabled_source_types=(
                "dashboard_context",
                "dashboard_export",
            )
        ),
    )

    runtime.run(
        org=org,
        dashboard_id=primary_dashboard.id,
        user_query="Explain the reach metric",
    )

    queried_source_groups = [tuple(call["source_types"]) for call in vector_store.calls]
    assert all("org_context" not in source_group for source_group in queried_source_groups)


def test_list_tables_by_keyword_matches_allowlisted_table_names_without_schema_lookup(org):
    """Keyword table lookup should work even when schema snippets are not yet cached."""
    fake_warehouse = FakeWarehouseTools()
    runtime = DashboardChatRuntime(
        vector_store=FakeVectorStore([]),
        llm_client=ContextToolLoopLlm(),
        warehouse_tools_factory=lambda org: fake_warehouse,
    )
    state = {
        "org": org,
        "allowlist": DashboardChatAllowlist(
            allowed_tables={
                "analytics.district_funding_efficiency_quarterly",
                "analytics.facilitator_effectiveness_quarterly",
            }
        ),
    }
    execution_context = {"schema_cache": {}, "warnings": []}

    result = runtime._tool_list_tables_by_keyword(
        {"keyword": "district_funding_efficiency_quarterly", "limit": 10},
        state,
        execution_context,
    )

    assert result["tables"][0]["table"] == "analytics.district_funding_efficiency_quarterly"


def test_dashboard_chat_response_to_dict_serializes_decimal_sql_results():
    """Final response payloads must be JSON-safe before they are persisted."""
    response = DashboardChatResponse(
        answer_text="Answer",
        intent=DashboardChatIntent.QUERY_WITH_SQL,
        sql_results=[{"quarter": "2025 Q2", "funding": Decimal("105000.00")}],
    )

    payload = response.to_dict()

    assert payload["sql_results"] == [{"quarter": "2025 Q2", "funding": "105000.00"}]


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


def test_tool_document_payload_exposes_structured_chart_metadata():
    """Chart retrieval payloads should surface exact table, metric, and dimension hints."""
    runtime = DashboardChatRuntime(
        vector_store=FakeVectorStore([]),
        llm_client=SmallTalkLlm(),
    )

    payload = runtime._tool_document_payload(
        DashboardChatRetrievedDocument(
            document_id="doc-chart",
            source_type=DashboardChatSourceType.DASHBOARD_EXPORT.value,
            source_identifier="dashboard:6:chart:7",
            content="Facilitator outcomes chart",
            dashboard_id=6,
            distance=0.02,
        ),
        DashboardChatAllowlist(
            allowed_tables={"analytics.facilitator_effectiveness_quarterly"}
        ),
        {
            "dashboard": {"title": "Facilitator Effectiveness Studio"},
            "charts": [
                {
                    "id": 7,
                    "title": "Facilitator Outcomes",
                    "chart_type": "bar",
                    "schema_name": "analytics",
                    "table_name": "facilitator_effectiveness_quarterly",
                    "extra_config": {
                        "dimension_col": "quarter_label",
                        "extra_dimension": "facilitator_name",
                        "metrics": [
                            {"column": "cost_per_improved_outcome_usd"},
                            {"column": "improved_literacy_students"},
                        ],
                    },
                }
            ],
        },
    )

    assert payload["metadata"]["preferred_table"] == "analytics.facilitator_effectiveness_quarterly"
    assert payload["metadata"]["metric_columns"] == [
        "cost_per_improved_outcome_usd",
        "improved_literacy_students",
    ]
    assert payload["metadata"]["dimension_columns"] == [
        "quarter_label",
        "facilitator_name",
    ]
    assert payload["metadata"]["time_column"] == "quarter_label"


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

    allowed_query = guard.validate(
        "SELECT COUNT(*) AS beneficiary_count FROM analytics.program_reach"
    )
    assert allowed_query.is_valid is True
    assert allowed_query.sanitized_sql.endswith("LIMIT 200")
    assert any("No LIMIT clause found" in warning for warning in allowed_query.warnings)


def test_sql_guard_rejects_row_level_pii_queries():
    """SQL guard should reject row-level projections of sensitive fields."""
    allowlist = DashboardChatAllowlist(allowed_tables={"analytics.program_reach"})
    guard = DashboardChatSqlGuard(allowlist=allowlist, max_rows=200)

    pii_query = guard.validate(
        "SELECT email, COUNT(*) AS beneficiary_count "
        "FROM analytics.program_reach "
        "GROUP BY email "
        "LIMIT 50"
    )

    assert pii_query.is_valid is False
    assert pii_query.sanitized_sql is None
    assert pii_query.errors == [
        "Queries returning row-level sensitive data are not allowed. Please aggregate the results or rephrase."
    ]


def test_sql_guard_rejects_select_into_queries():
    """SQL guard should reject SELECT ... INTO statements."""
    allowlist = DashboardChatAllowlist(allowed_tables={"analytics.program_reach"})
    guard = DashboardChatSqlGuard(allowlist=allowlist, max_rows=200)

    select_into_query = guard.validate(
        "SELECT program_name INTO temp_programs FROM analytics.program_reach LIMIT 50"
    )

    assert select_into_query.is_valid is False
    assert select_into_query.sanitized_sql is None
    assert "SELECT INTO is not allowed" in select_into_query.errors
