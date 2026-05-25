"""Tests for the dashboard chat runtime."""

from decimal import Decimal
from dataclasses import dataclass

import pytest
from django.contrib.auth.models import User

from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import (
    DashboardChatAllowlist,
    DashboardChatAllowlistBuilder,
)
from ddpui.core.dashboard_chat.config import DashboardChatRuntimeConfig
from ddpui.core.dashboard_chat.orchestration.conversation_context import extract_conversation_context
from ddpui.core.dashboard_chat.orchestration.tool_loop_message_builder import (
    build_follow_up_messages,
    build_new_query_messages,
)
from ddpui.core.dashboard_chat.orchestration.nodes.load_context import load_context_node
from ddpui.core.dashboard_chat.orchestration.orchestrator import DashboardChatRuntime
from ddpui.core.dashboard_chat.orchestration.response_composer import (
    compose_final_answer_text,
    determine_response_format,
)
from ddpui.core.dashboard_chat.orchestration.retrieval_support import build_tool_document_payload
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.schema_tools import (
    handle_get_distinct_values_tool,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_corrections import (
    missing_columns_in_primary_table,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_execution_tools import (
    handle_run_sql_query_tool,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_validation import (
    find_missing_distinct_filters,
    validate_follow_up_dimension_usage,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.runtime.turn_context import (
    DashboardChatTurnContext,
    seed_validated_distinct_values_from_previous_sql,
)
from ddpui.core.dashboard_chat.contracts.conversation_contracts import (
    DashboardChatConversationContext,
    DashboardChatConversationMessage,
)
from ddpui.core.dashboard_chat.contracts.intent_contracts import (
    DashboardChatFollowUpContext,
    DashboardChatIntent,
    DashboardChatIntentDecision,
)
from ddpui.core.dashboard_chat.contracts.response_contracts import DashboardChatResponse
from ddpui.core.dashboard_chat.contracts.retrieval_contracts import (
    DashboardChatRetrievedDocument,
    DashboardChatSourceType,
)
from ddpui.core.dashboard_chat.warehouse.sql_guard import DashboardChatSqlGuard
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import (
    DashboardChatMetadataArtifact,
    DashboardChatMetadataArtifactStatus,
)
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.visualization import Chart
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db(transaction=True)


def build_runtime_state(
    *,
    org: Org | None = None,
    allowlist: DashboardChatAllowlist | None = None,
    conversation_context: DashboardChatConversationContext | None = None,
    intent_decision: DashboardChatIntentDecision | None = None,
    **extra,
):
    """Build a runtime-state payload that matches the post-refactor graph contract."""
    state = dict(extra)
    if org is not None:
        state["org_id"] = org.id
    if allowlist is not None:
        state["allowlist_payload"] = allowlist.model_dump(mode="json")
    if conversation_context is not None:
        state["conversation_context"] = conversation_context.model_dump(mode="json")
    if intent_decision is not None:
        state["intent_decision"] = intent_decision.model_dump(mode="json")
    return state


def build_turn_context(
    *,
    schema_snippets_by_table=None,
    validated_distinct_values=None,
    warehouse_tools=None,
    warnings=None,
    last_sql=None,
    last_sql_results=None,
    last_sql_validation=None,
):
    """Build the explicit per-turn execution context used by tool helpers."""
    return DashboardChatTurnContext(
        validated_distinct_values=set(validated_distinct_values or set()),
        schema_snippets_by_table=dict(schema_snippets_by_table or {}),
        warnings=list(warnings or []),
        warehouse_tools=warehouse_tools,
        last_sql=last_sql,
        last_sql_results=last_sql_results,
        last_sql_validation=last_sql_validation,
        timing_breakdown={"tool_calls_ms": []},
    )


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
        if (
            "analytics.stg_donor_funding_clean" in sql
            and "GROUP BY quarter_label, donor_type" in sql
        ):
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
        from ddpui.core.dashboard_chat.contracts.retrieval_contracts import DashboardChatSchemaSnippet

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
            assert any(
                "analytics.stg_program_reach" in message["content"] for message in tool_messages
            )
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


class SmallTalkLlm(PrototypeLlmBase):
    """LLM stub for small-talk turns."""

    def classify_intent(self, *args, **kwargs):
        return DashboardChatIntentDecision(
            intent=DashboardChatIntent.SMALL_TALK,
            confidence=0.97,
            reason="Greeting or pleasantry",
        )

    def run_tool_loop_turn(self, *, messages, tools, tool_choice, operation):
        raise AssertionError("Small talk should not enter the tool loop")


class FastPathOnlySmallTalkLlm(PrototypeLlmBase):
    """LLM stub that fails if the runtime does not short-circuit obvious small talk."""

    def classify_intent(self, *args, **kwargs):
        raise AssertionError("Fast-path small talk should skip LLM classification")

    def compose_small_talk(self, user_query):
        raise AssertionError("Fast-path small talk should use deterministic response")

    def run_tool_loop_turn(self, *, messages, tools, tool_choice, operation):
        raise AssertionError("Small talk should not enter the tool loop")


class FinalAnswerComposerLlm(PrototypeLlmBase):
    """LLM stub that only composes the final user-facing answer."""

    def __init__(self):
        super().__init__()
        self.compose_calls = []

    def classify_intent(self, *args, **kwargs):
        raise AssertionError("This stub is only for direct final-answer composition tests")

    def run_tool_loop_turn(self, *, messages, tools, tool_choice, operation):
        raise AssertionError("This stub should not enter the tool loop")

    def compose_final_answer(
        self,
        *,
        user_query,
        intent,
        response_format,
        draft_answer,
        retrieved_documents,
        sql,
        sql_results,
        warnings,
    ):
        self.compose_calls.append(
            {
                "user_query": user_query,
                "intent": intent,
                "response_format": response_format,
                "draft_answer": draft_answer,
                "retrieved_documents": retrieved_documents,
                "sql": sql,
                "sql_results": sql_results,
                "warnings": warnings,
            }
        )
        return "## District-wise pass rates\nSee the table below for the breakdown."


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


@pytest.fixture
def ready_metadata_artifact(primary_dashboard):
    artifact = DashboardChatMetadataArtifact.objects.create(
        dashboard=primary_dashboard,
        status=DashboardChatMetadataArtifactStatus.READY,
        artifact_json={
            "schema_version": 2,
            "dashboard_id": primary_dashboard.id,
            "org_id": primary_dashboard.org_id,
            "dashboard_title": primary_dashboard.title,
            "dashboard_description": primary_dashboard.description or "",
            "allowlisted_tables": ["analytics.program_reach"],
            "chart_table_map": {"analytics.program_reach": ["1"]},
            "tables": [],
            "join_paths": [],
            "entity_index": {},
            "measure_index": {},
            "column_index": {},
        },
    )
    yield artifact
    artifact.delete()


def test_load_context_downgrades_unsupported_ready_metadata_artifact_to_stale(primary_dashboard):
    DashboardChatMetadataArtifact.objects.create(
        dashboard=primary_dashboard,
        status=DashboardChatMetadataArtifactStatus.READY,
        schema_version=1,
        artifact_json={
            "schema_version": 1,
            "dashboard_id": primary_dashboard.id,
            "org_id": primary_dashboard.org_id,
            "dashboard_title": primary_dashboard.title,
            "dashboard_description": primary_dashboard.description or "",
            "allowlisted_tables": ["analytics.program_reach"],
            "chart_table_map": {"analytics.program_reach": ["1"]},
            "tables": [],
            "join_paths": [],
            "entity_index": {},
            "measure_index": {},
            "column_index": {},
        },
    )

    loaded = load_context_node(
        {
            "org_id": primary_dashboard.org_id,
            "dashboard_id": primary_dashboard.id,
            "schema_snippet_payloads": {},
            "validated_distinct_payloads": {},
        }
    )

    assert loaded["metadata_artifact_payload"] is None
    assert loaded["metadata_artifact_status"] == "stale"


def test_extract_conversation_context_reads_previous_sql_payload():
    """Follow-up routing should recover prior SQL context from assistant payloads."""
    conversation_context = extract_conversation_context(
        [
            DashboardChatConversationMessage(
                role="user", content="How many beneficiaries do we have?"
            ),
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


def test_seed_validated_distinct_values_reuses_previous_text_filters(primary_dashboard):
    """Follow-up turns should reuse text-filter validations from the previous successful SQL."""
    state = build_runtime_state(
        dashboard_id=primary_dashboard.id,
        conversation_context=extract_conversation_context(
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
    )
    turn_context = build_turn_context(validated_distinct_values=set())

    seed_validated_distinct_values_from_previous_sql(state, turn_context)

    assert (
        "analytics.donor_funding_quarterly",
        "quarter_label",
        "2025 q1",
    ) in turn_context.validated_distinct_values
    assert ("*", "quarter_label", "2025 q2") in turn_context.validated_distinct_values


def test_missing_distinct_accepts_previous_filter_validation_on_upstream_table(primary_dashboard):
    """Follow-up SQL should reuse validated text filters even after moving to an upstream table."""
    state = build_runtime_state(
        org=primary_dashboard.org,
        allowlist=DashboardChatAllowlist(
            allowed_tables={
                "analytics.donor_funding_quarterly",
                "analytics.stg_donor_funding_clean",
            }
        ),
        conversation_context=extract_conversation_context(
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
    )
    turn_context = build_turn_context(
        validated_distinct_values=set(),
        schema_snippets_by_table={
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
    )

    seed_validated_distinct_values_from_previous_sql(state, turn_context)
    missing = find_missing_distinct_filters(
        lambda org: FakeWarehouseTools(),
        (
            "SELECT quarter_label, donor_type, SUM(realized_amount_usd) AS total_realized_funding_usd "
            "FROM analytics.stg_donor_funding_clean "
            "WHERE quarter_label IN ('2025 Q1', '2025 Q2') "
            "AND is_realized = TRUE "
            "GROUP BY quarter_label, donor_type"
        ),
        state,
        turn_context,
    )

    assert missing == []


def test_get_distinct_values_returns_column_correction_for_wrong_table(primary_dashboard):
    """Follow-up correction should surface candidate tables when a distinct lookup targets the wrong table."""
    state = build_runtime_state(
        org=primary_dashboard.org,
        allowlist=DashboardChatAllowlist(
            allowed_tables={
                "analytics.donor_funding_quarterly",
                "analytics.stg_donor_funding_clean",
            }
        ),
    )
    turn_context = build_turn_context(
        schema_snippets_by_table={
            "analytics.donor_funding_quarterly": FakeWarehouseTools._schema_snippet(
                "analytics.donor_funding_quarterly",
                [
                    {"name": "quarter_label", "data_type": "text", "nullable": False},
                    {
                        "name": "total_realized_funding_usd",
                        "data_type": "numeric",
                        "nullable": False,
                    },
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
    )

    result = handle_get_distinct_values_tool(
        lambda org: FakeWarehouseTools(),
        {
            "table": "analytics.donor_funding_quarterly",
            "column": "donor_type",
            "limit": 50,
        },
        state,
        turn_context,
    )

    assert result["error"] == "column_not_in_table"
    assert result["table"] == "analytics.donor_funding_quarterly"
    assert result["column"] == "donor_type"
    assert "analytics.stg_donor_funding_clean" in result["candidates"]


def test_missing_columns_check_ignores_boolean_literals(primary_dashboard):
    """Boolean literals in WHERE clauses should not be misread as missing columns."""
    state = build_runtime_state(
        org=primary_dashboard.org,
        allowlist=DashboardChatAllowlist(allowed_tables={"analytics.stg_donor_funding_clean"}),
    )
    turn_context = build_turn_context(
        schema_snippets_by_table={
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
    )

    missing = missing_columns_in_primary_table(
        lambda org: FakeWarehouseTools(),
        sql=(
            "SELECT quarter_label, donor_type, SUM(realized_amount_usd) AS total_realized_funding_usd, "
            "COUNT(DISTINCT donation_id) AS donor_count "
            "FROM analytics.stg_donor_funding_clean "
            "WHERE quarter_label IN ('2025 Q1', '2025 Q2') AND is_realized = TRUE "
            "GROUP BY quarter_label, donor_type ORDER BY quarter_label, donor_type LIMIT 200"
        ),
        state=state,
        turn_context=turn_context,
    )

    assert missing is None


def test_run_sql_keeps_join_tables_intact(primary_dashboard):
    """Join queries should execute the model's SQL as written and let the tool loop correct errors."""
    fake_warehouse = FakeWarehouseTools()
    state = build_runtime_state(
        org=primary_dashboard.org,
        allowlist=DashboardChatAllowlist(
            allowed_tables={
                "analytics.facilitator_effectiveness_quarterly",
                "analytics.district_funding_efficiency_quarterly",
            }
        ),
        intent_decision=DashboardChatIntentDecision(
            intent=DashboardChatIntent.QUERY_WITH_SQL,
            confidence=0.9,
            reason="Join-heavy data analysis",
            force_tool_usage=True,
        ),
        user_query="Join facilitator outcomes to district funding efficiency.",
    )
    turn_context = build_turn_context(
        schema_snippets_by_table={
            "analytics.facilitator_effectiveness_quarterly": FakeWarehouseTools().schemas[
                "analytics.facilitator_effectiveness_quarterly"
            ],
            "analytics.district_funding_efficiency_quarterly": FakeWarehouseTools().schemas[
                "analytics.district_funding_efficiency_quarterly"
            ],
        },
        warehouse_tools=fake_warehouse,
        validated_distinct_values={("*", "quarter_label")},
        warnings=[],
    )

    result = handle_run_sql_query_tool(
        lambda org: fake_warehouse,
        DashboardChatRuntimeConfig(
            retrieval_limit=6,
            max_query_rows=200,
            max_distinct_values=20,
            max_schema_tables=4,
        ),
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
        turn_context,
    )

    assert result["success"] is True
    assert "analytics.facilitator_effectiveness_quarterly f" in result["sql_used"]
    assert "analytics.district_funding_efficiency_quarterly d" in result["sql_used"]


def test_missing_distinct_resolves_join_filter_to_qualified_table(primary_dashboard):
    """Distinct validation should inspect the joined table referenced by a qualified WHERE filter."""
    state = build_runtime_state(
        org=primary_dashboard.org,
        allowlist=DashboardChatAllowlist(
            allowed_tables={
                "analytics.facilitator_effectiveness_quarterly",
                "analytics.district_funding_efficiency_quarterly",
            }
        ),
    )
    turn_context = build_turn_context(
        schema_snippets_by_table={
            "analytics.facilitator_effectiveness_quarterly": FakeWarehouseTools().schemas[
                "analytics.facilitator_effectiveness_quarterly"
            ],
            "analytics.district_funding_efficiency_quarterly": FakeWarehouseTools().schemas[
                "analytics.district_funding_efficiency_quarterly"
            ],
        },
        validated_distinct_values=set(),
    )

    missing = find_missing_distinct_filters(
        lambda org: FakeWarehouseTools(),
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
        turn_context,
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
    state = build_runtime_state(
        org=primary_dashboard.org,
        allowlist=DashboardChatAllowlist(
            allowed_tables={
                "analytics.facilitator_effectiveness_quarterly",
                "analytics.district_funding_efficiency_quarterly",
            }
        ),
    )
    turn_context = build_turn_context(
        schema_snippets_by_table={
            "analytics.facilitator_effectiveness_quarterly": FakeWarehouseTools().schemas[
                "analytics.facilitator_effectiveness_quarterly"
            ],
            "analytics.district_funding_efficiency_quarterly": FakeWarehouseTools().schemas[
                "analytics.district_funding_efficiency_quarterly"
            ],
        },
    )

    missing = missing_columns_in_primary_table(
        lambda org: FakeWarehouseTools(),
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
        turn_context=turn_context,
    )

    assert missing["error"] == "column_not_in_table"
    assert missing["table"] == "analytics.district_funding_efficiency_quarterly"
    assert missing["column"] == "fake_dimension"


def test_missing_columns_check_ignores_order_by_select_alias(primary_dashboard):
    """ORDER BY aliases from the SELECT clause should not be treated as missing physical columns."""
    state = build_runtime_state(
        org=primary_dashboard.org,
        allowlist=DashboardChatAllowlist(
            allowed_tables={"analytics.facilitator_effectiveness_quarterly"}
        ),
    )
    turn_context = build_turn_context(
        schema_snippets_by_table={
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
    )

    missing = missing_columns_in_primary_table(
        lambda org: FakeWarehouseTools(),
        sql=(
            "SELECT facilitator_name, AVG(cost_per_improved_outcome_usd) AS avg_cost_per_improved_outcome "
            "FROM analytics.facilitator_effectiveness_quarterly "
            "WHERE quarter_label = '2025 Q2' "
            "GROUP BY facilitator_name "
            "ORDER BY avg_cost_per_improved_outcome ASC "
            "LIMIT 1"
        ),
        state=state,
        turn_context=turn_context,
    )

    assert missing is None


def test_small_talk_turn_returns_without_citations(primary_dashboard, ready_metadata_artifact):
    """Greeting turns should skip retrieval and finalize cleanly."""
    runtime = DashboardChatRuntime(llm_client=SmallTalkLlm())

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
    assert response.metadata["timing_breakdown"]["runtime_total_ms"] >= 0
    assert "load_context" in response.metadata["timing_breakdown"]["graph_nodes_ms"]
    assert "route_intent" in response.metadata["timing_breakdown"]["graph_nodes_ms"]
    assert "handle_small_talk" in response.metadata["timing_breakdown"]["graph_nodes_ms"]
    assert "finalize" in response.metadata["timing_breakdown"]["graph_nodes_ms"]


@pytest.mark.parametrize(
    ("user_query", "expected_text"),
    [
        (
            "what can you do?",
            "I can explain this dashboard",
        ),
        (
            "who are you?",
            "I'm the dashboard chat assistant",
        ),
    ],
)
def test_small_talk_fast_path_handles_capability_prompts(
    primary_dashboard,
    ready_metadata_artifact,
    user_query,
    expected_text,
):
    """Obvious capability/identity prompts should short-circuit before LLM classification."""
    runtime = DashboardChatRuntime(llm_client=FastPathOnlySmallTalkLlm())

    response = runtime.run(
        org=primary_dashboard.org,
        dashboard_id=primary_dashboard.id,
        user_query=user_query,
    )

    assert response.intent == DashboardChatIntent.SMALL_TALK
    assert expected_text in response.answer_text
    assert response.citations == []
    assert response.warnings == []
    assert response.metadata["timing_breakdown"]["runtime_total_ms"] >= 0


def test_runtime_prompt_messages_do_not_inline_raw_human_context(primary_dashboard):
    """Raw org/dashboard markdown should reach the model through retrieval, not prompt duplication."""
    runtime = DashboardChatRuntime(llm_client=SmallTalkLlm())

    new_query_messages = build_new_query_messages(
        runtime.llm_client,
        build_runtime_state(
            user_query="Explain the reach metric",
            human_context="Organization context: duplicated markdown",
        ),
    )
    follow_up_messages = build_follow_up_messages(
        runtime.llm_client,
        build_runtime_state(
            user_query="Explain that metric",
            human_context="Organization context: duplicated markdown",
            conversation_context=extract_conversation_context([]),
        ),
    )

    assert new_query_messages[0]["content"] == "prompt:new_query_system"
    assert all("Human context" not in message["content"] for message in follow_up_messages)


def test_follow_up_dimension_validation_accepts_structural_granularity_change(primary_dashboard):
    """Follow-up add-dimension validation should accept structural SQL rewrites, not only exact token reuse."""
    state = build_runtime_state(
        org=primary_dashboard.org,
        allowlist=DashboardChatAllowlist(allowed_tables={"analytics.stg_donor_funding_clean"}),
        intent_decision=DashboardChatIntentDecision(
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
        conversation_context=DashboardChatConversationContext(
            last_sql_query=(
                "SELECT quarter_label, SUM(realized_amount_usd) AS total_realized_funding_usd "
                "FROM analytics.stg_donor_funding_clean "
                "WHERE quarter_label IN ('2025 Q1', '2025 Q2') "
                "GROUP BY quarter_label"
            ),
        ),
        user_query="Now split that by donor type.",
    )
    turn_context = build_turn_context(
        schema_snippets_by_table={
            "analytics.stg_donor_funding_clean": FakeWarehouseTools._schema_snippet(
                "analytics.stg_donor_funding_clean",
                [
                    {"name": "quarter_label", "data_type": "text", "nullable": False},
                    {"name": "donor_type", "data_type": "text", "nullable": False},
                    {"name": "realized_amount_usd", "data_type": "numeric", "nullable": False},
                ],
            )
        },
    )

    validation = validate_follow_up_dimension_usage(
        lambda org: FakeWarehouseTools(),
        sql=(
            "SELECT quarter_label, COALESCE(donor_type, 'Unknown') AS donor_type, "
            "SUM(realized_amount_usd) AS total_realized_funding_usd "
            "FROM analytics.stg_donor_funding_clean "
            "WHERE quarter_label IN ('2025 Q1', '2025 Q2') "
            "GROUP BY quarter_label, COALESCE(donor_type, 'Unknown')"
        ),
        state=state,
        turn_context=turn_context,
    )

    assert validation is None


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
    assert "raw.students" not in allowlist.allowed_tables
    assert allowlist.is_allowed("analytics.fact_reach") is True
    assert allowlist.is_unique_id_allowed("model.dalgo.dim_program") is True


def test_tool_document_payload_exposes_structured_chart_metadata():
    """Chart retrieval payloads should surface exact table, metric, and dimension hints."""
    payload = build_tool_document_payload(
        DashboardChatRetrievedDocument(
            document_id="doc-chart",
            source_type=DashboardChatSourceType.DASHBOARD_EXPORT.value,
            source_identifier="dashboard:6:chart:7",
            content="Facilitator outcomes chart",
            dashboard_id=6,
            distance=0.02,
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


def test_compose_final_answer_text_uses_llm_and_normalizes_rate_values():
    """Final answer composition should send normalized values and table hints to the composer."""
    llm = FinalAnswerComposerLlm()
    state = build_runtime_state(
        user_query="Give me a district wise pass rate breakdown",
        intent_decision=DashboardChatIntentDecision(
            intent=DashboardChatIntent.QUERY_WITH_SQL,
            confidence=0.9,
            reason="Needs grouped results",
            force_tool_usage=True,
        ),
    )
    execution_result = {
        "answer_text": "",
        "retrieved_documents": [
            DashboardChatRetrievedDocument(
                document_id="doc-chart",
                source_type=DashboardChatSourceType.DASHBOARD_EXPORT.value,
                source_identifier="dashboard:1:chart:2",
                content="District pass-rate chart",
            )
        ],
        "sql": (
            "SELECT district_name, avg_literacy_pass_rate, avg_numeracy_pass_rate "
            "FROM analytics.district_program_performance_quarterly"
        ),
        "sql_results": [
            {
                "district_name": "East",
                "avg_literacy_pass_rate": Decimal("0E-20"),
                "avg_numeracy_pass_rate": Decimal("0.25000000000000000000"),
            },
            {
                "district_name": "South",
                "avg_literacy_pass_rate": Decimal("0.25000000000000000000"),
                "avg_numeracy_pass_rate": Decimal("0E-20"),
            },
        ],
        "warnings": [],
    }

    answer = compose_final_answer_text(
        llm,
        state,
        execution_result,
        response_format="text_with_table",
    )

    assert answer == "## District-wise pass rates\nSee the table below for the breakdown."
    assert llm.compose_calls[0]["response_format"] == "text_with_table"
    assert llm.compose_calls[0]["sql_results"] == [
        {
            "district_name": "East",
            "avg_literacy_pass_rate": "0%",
            "avg_numeracy_pass_rate": "25%",
        },
        {
            "district_name": "South",
            "avg_literacy_pass_rate": "25%",
            "avg_numeracy_pass_rate": "0%",
        },
    ]


def test_determine_response_format_prefers_table_for_grouped_breakdowns():
    """Grouped breakdowns should tell the frontend to render a structured table."""
    response_format = determine_response_format(
        user_query="Give me a district wise pass rate breakdown",
        sql_results=[
            {
                "district_name": "North",
                "avg_literacy_pass_rate": "25%",
                "avg_numeracy_pass_rate": "50%",
            },
            {
                "district_name": "South",
                "avg_literacy_pass_rate": "25%",
                "avg_numeracy_pass_rate": "0%",
            },
        ],
    )

    assert response_format == "text_with_table"
