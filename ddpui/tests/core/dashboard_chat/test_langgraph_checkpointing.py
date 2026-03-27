"""Focused tests for dashboard chat LangGraph checkpoint bootstrap/persistence."""

from unittest.mock import patch
from uuid import uuid4

import pytest
from django.contrib.auth.models import User
from django.db import connection

from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.orchestration.checkpoints import get_dashboard_chat_checkpointer
from ddpui.core.dashboard_chat.orchestration.nodes.load_context import load_context_node
from ddpui.core.dashboard_chat.orchestration.orchestrator import DashboardChatRuntime
from ddpui.core.dashboard_chat.orchestration.state.payload_codec import (
    serialize_sql_validation_result,
)
from ddpui.core.dashboard_chat.contracts import (
    DashboardChatFollowUpContext,
    DashboardChatIntent,
    DashboardChatIntentDecision,
    DashboardChatSqlValidationResult,
)
from ddpui.models.dashboard import Dashboard
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.visualization import Chart
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db(transaction=True)


class MinimalVectorStore:
    """Minimal vector store for checkpointing tests that do not use retrieval."""

    def usage_summary(self):
        return {}


class MinimalLlmClient:
    """Minimal LLM client used for fast-path small-talk runtime turns."""

    def usage_summary(self):
        return {}


class QuerySecondTurnLlmClient:
    """LLM stub that keeps first-turn small talk fast path and classifies later queries."""

    def classify_intent(self, user_query, conversation_context):
        return DashboardChatIntentDecision(
            intent=DashboardChatIntent.QUERY_WITH_SQL,
            confidence=0.95,
            reason="Test SQL route",
            force_tool_usage=True,
            follow_up_context=DashboardChatFollowUpContext(is_follow_up=False),
        )

    def compose_small_talk(self, user_query):
        return "Hi. Ask me anything about this dashboard or the data behind it."

    def get_prompt(self, prompt_key):
        return ""

    def reset_usage(self):
        return None

    def run_tool_loop_turn(self, *, messages, tools, tool_choice, operation):
        raise AssertionError("tool loop should be patched in this regression test")

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
        return f"Computed answer for {user_query}"

    def usage_summary(self):
        return {}


@pytest.fixture
def org():
    organization = Org.objects.create(
        name="Dashboard Chat Org",
        slug=f"dashchat-{uuid4().hex[:8]}",
        airbyte_workspace_id="workspace-1",
    )
    yield organization
    organization.delete()


@pytest.fixture
def orguser(org, seed_db):
    user = User.objects.create(
        username=f"dashchat-user-{uuid4().hex[:8]}",
        email=f"dashchat-user-{uuid4().hex[:8]}@test.com",
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


def test_load_context_node_bootstraps_checkpoint_payloads(primary_dashboard):
    """First-turn context loading should return checkpoint-safe dashboard payloads."""
    export_payload = {"dashboard": {"title": "Impact Overview"}, "charts": []}
    allowlist = DashboardChatAllowlist(allowed_tables={"analytics.program_reach"})
    manifest_json = {"nodes": {}, "sources": {}, "parent_map": {}, "child_map": {}}

    with patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardService.export_dashboard_context",
        return_value=export_payload,
    ) as export_mock, patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardChatAllowlistBuilder.load_manifest_json",
        return_value=manifest_json,
    ) as manifest_mock, patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardChatAllowlistBuilder.build",
        return_value=allowlist,
    ) as build_mock, patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardChatAllowlistBuilder.build_dbt_index",
        return_value={"resources_by_unique_id": {}},
    ) as index_mock:
        updates = load_context_node(
            {
                "org_id": primary_dashboard.org_id,
                "dashboard_id": primary_dashboard.id,
                "session_id": str(uuid4()),
            }
        )

    assert updates["dashboard_export_payload"] == export_payload
    assert updates["allowlist_payload"]["allowed_tables"] == ["analytics.program_reach"]
    assert updates["dbt_index"] == {"resources_by_unique_id": {}}
    assert updates["schema_snippet_payloads"] == {}
    assert updates["validated_distinct_payloads"] == {}
    export_mock.assert_called_once()
    manifest_mock.assert_called_once()
    build_mock.assert_called_once_with(export_payload, manifest_json=manifest_json)
    index_mock.assert_called_once_with(manifest_json, allowlist)


def test_runtime_session_turn_writes_langgraph_checkpoints(primary_dashboard):
    """Session-backed runtime turns should persist LangGraph checkpoints in Postgres."""
    export_payload = {"dashboard": {"title": "Impact Overview"}, "charts": []}
    allowlist = DashboardChatAllowlist(allowed_tables={"analytics.program_reach"})
    manifest_json = {"nodes": {}, "sources": {}, "parent_map": {}, "child_map": {}}
    session_id = str(uuid4())

    runtime = DashboardChatRuntime(
        vector_store=MinimalVectorStore(),
        llm_client=MinimalLlmClient(),
        checkpointer=get_dashboard_chat_checkpointer().saver,
    )

    with patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardService.export_dashboard_context",
        return_value=export_payload,
    ), patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardChatAllowlistBuilder.load_manifest_json",
        return_value=manifest_json,
    ), patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardChatAllowlistBuilder.build",
        return_value=allowlist,
    ), patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardChatAllowlistBuilder.build_dbt_index",
        return_value={"resources_by_unique_id": {}},
    ):
        response = runtime.run(
            org=primary_dashboard.org,
            dashboard_id=primary_dashboard.id,
            user_query="hi",
            session_id=session_id,
            vector_collection_name=None,
            conversation_history=[],
        )

    assert response.answer_text.startswith("Hi.")
    with connection.cursor() as cursor:
        cursor.execute("select count(*) from checkpoints where thread_id = %s", [session_id])
        checkpoint_count = cursor.fetchone()[0]
    assert checkpoint_count > 0


def test_runtime_reuses_checkpointed_context_across_turns(primary_dashboard):
    """Second turns in the same session should reuse durable checkpointed context."""
    export_payload = {"dashboard": {"title": "Impact Overview"}, "charts": []}
    allowlist = DashboardChatAllowlist(allowed_tables={"analytics.program_reach"})
    manifest_json = {"nodes": {}, "sources": {}, "parent_map": {}, "child_map": {}}
    session_id = str(uuid4())

    runtime = DashboardChatRuntime(
        vector_store=MinimalVectorStore(),
        llm_client=MinimalLlmClient(),
        checkpointer=get_dashboard_chat_checkpointer().saver,
    )

    with patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardService.export_dashboard_context",
        return_value=export_payload,
    ) as export_mock, patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardChatAllowlistBuilder.load_manifest_json",
        return_value=manifest_json,
    ) as manifest_mock, patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardChatAllowlistBuilder.build",
        return_value=allowlist,
    ) as build_mock, patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardChatAllowlistBuilder.build_dbt_index",
        return_value={"resources_by_unique_id": {}},
    ) as index_mock:
        first_response = runtime.run(
            org=primary_dashboard.org,
            dashboard_id=primary_dashboard.id,
            user_query="hi",
            session_id=session_id,
            vector_collection_name=None,
            conversation_history=[],
        )
        second_response = runtime.run(
            org=primary_dashboard.org,
            dashboard_id=primary_dashboard.id,
            user_query="who are you",
            session_id=session_id,
            vector_collection_name=None,
            conversation_history=[],
        )

    assert first_response.answer_text.startswith("Hi.")
    assert second_response.answer_text.startswith("I'm the dashboard chat assistant")
    export_mock.assert_called_once()
    manifest_mock.assert_called_once()
    build_mock.assert_called_once()
    index_mock.assert_called_once()


def test_runtime_rebuilds_response_for_non_small_talk_turns_in_same_session(primary_dashboard):
    """A later SQL turn must not reuse the prior checkpointed small-talk response."""
    export_payload = {"dashboard": {"title": "Impact Overview"}, "charts": []}
    allowlist = DashboardChatAllowlist(allowed_tables={"analytics.program_reach"})
    manifest_json = {"nodes": {}, "sources": {}, "parent_map": {}, "child_map": {}}
    session_id = str(uuid4())

    runtime = DashboardChatRuntime(
        vector_store=MinimalVectorStore(),
        llm_client=QuerySecondTurnLlmClient(),
        checkpointer=get_dashboard_chat_checkpointer().saver,
    )

    query_with_sql_updates = {
        "retrieved_documents": [],
        "tool_calls": [{"name": "run_sql_query"}],
        "draft_answer_text": "",
        "sql": "SELECT facilitator_name FROM analytics.facilitator_effectiveness_quarterly LIMIT 5",
        "sql_validation": serialize_sql_validation_result(
            DashboardChatSqlValidationResult(
                is_valid=True,
                sanitized_sql="SELECT facilitator_name FROM analytics.facilitator_effectiveness_quarterly LIMIT 5",
                tables=["analytics.facilitator_effectiveness_quarterly"],
                warnings=[],
                errors=[],
            )
        ),
        "sql_results": [{"facilitator_name": "Asha Menon"}],
        "warnings": [],
        "timing_breakdown": {"tool_calls_ms": [], "tool_loop_ms": 0},
        "schema_snippet_payloads": {},
        "validated_distinct_payloads": {},
    }

    with patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardService.export_dashboard_context",
        return_value=export_payload,
    ), patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardChatAllowlistBuilder.load_manifest_json",
        return_value=manifest_json,
    ), patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardChatAllowlistBuilder.build",
        return_value=allowlist,
    ), patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardChatAllowlistBuilder.build_dbt_index",
        return_value={"resources_by_unique_id": {}},
    ), patch(
        "ddpui.core.dashboard_chat.orchestration.orchestrator.handle_query_with_sql_node",
        return_value=query_with_sql_updates,
    ) as query_mock:
        first_response = runtime.run(
            org=primary_dashboard.org,
            dashboard_id=primary_dashboard.id,
            user_query="hi",
            session_id=session_id,
            vector_collection_name=None,
            conversation_history=[],
        )
        second_response = runtime.run(
            org=primary_dashboard.org,
            dashboard_id=primary_dashboard.id,
            user_query="top 5 facilitators by outcomes in q2",
            session_id=session_id,
            vector_collection_name=None,
            conversation_history=[],
        )

    assert first_response.answer_text.startswith("Hi.")
    assert second_response.answer_text == "Computed answer for top 5 facilitators by outcomes in q2"
    assert second_response.intent == DashboardChatIntent.QUERY_WITH_SQL
    assert second_response.sql == query_with_sql_updates["sql"]
    assert second_response.sql_results == query_with_sql_updates["sql_results"]
    query_mock.assert_called_once()


def test_runtime_resume_completes_interrupted_session(primary_dashboard):
    """Interrupted checkpointed runs should resume cleanly from the persisted session thread."""
    export_payload = {"dashboard": {"title": "Impact Overview"}, "charts": []}
    allowlist = DashboardChatAllowlist(allowed_tables={"analytics.program_reach"})
    manifest_json = {"nodes": {}, "sources": {}, "parent_map": {}, "child_map": {}}
    session_id = str(uuid4())

    runtime = DashboardChatRuntime(
        vector_store=MinimalVectorStore(),
        llm_client=MinimalLlmClient(),
        checkpointer=get_dashboard_chat_checkpointer().saver,
    )

    with patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardService.export_dashboard_context",
        return_value=export_payload,
    ), patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardChatAllowlistBuilder.load_manifest_json",
        return_value=manifest_json,
    ), patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardChatAllowlistBuilder.build",
        return_value=allowlist,
    ), patch(
        "ddpui.core.dashboard_chat.orchestration.nodes.load_context.DashboardChatAllowlistBuilder.build_dbt_index",
        return_value={"resources_by_unique_id": {}},
    ):
        with pytest.raises(RuntimeError, match="interrupted before a final response"):
            runtime.run(
                org=primary_dashboard.org,
                dashboard_id=primary_dashboard.id,
                user_query="hi",
                session_id=session_id,
                vector_collection_name=None,
                conversation_history=[],
                interrupt_before=["handle_small_talk"],
            )

    interrupted_state = runtime.get_state_snapshot(session_id)
    assert interrupted_state is not None
    assert interrupted_state.next == ("handle_small_talk",)

    resumed_response = runtime.resume(session_id)
    assert resumed_response.answer_text.startswith("Hi.")

    completed_state = runtime.get_state_snapshot(session_id)
    assert completed_state is not None
    assert completed_state.next == ()
