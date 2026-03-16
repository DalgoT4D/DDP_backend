"""Tests for AI chat model extensions."""

import os
import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from django.utils import timezone

from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.models.dashboard import Dashboard
from ddpui.models.llm import DashboardChatSession, DashboardChatSessionStatus, LlmSessionStatus
from ddpui.models.org import Org, OrgDbt
from ddpui.models.org_preferences import OrgPreferences, default_ai_chat_source_config
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.visualization import Chart
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="aichatmodeluser", email="aichatmodeluser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="AI Chat Test Org",
        slug="ai-chat-test-org",
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


@pytest.fixture
def orguser(authuser, org):
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


def test_org_preferences_to_json_preserves_legacy_and_new_ai_fields(org, orguser, seed_db):
    """OrgPreferences.to_json should keep legacy fields and expose the new AI chat state."""
    consented_at = timezone.now()
    updated_at = timezone.now()
    preferences = OrgPreferences.objects.create(
        org=org,
        llm_optin=True,
        llm_optin_approved_by=orguser,
        llm_optin_date=consented_at,
        ai_data_sharing_enabled=True,
        ai_data_sharing_consented_by=orguser,
        ai_data_sharing_consented_at=consented_at,
        ai_org_context_markdown="# Org context",
        ai_org_context_updated_by=orguser,
        ai_org_context_updated_at=updated_at,
        ai_chat_source_config={
            "org_context": True,
            "dashboard_context": False,
            "dbt_manifest": True,
            "dbt_catalog": False,
        },
    )

    payload = preferences.to_json()

    assert payload["llm_optin"] is True
    assert payload["llm_optin_approved_by"] == orguser.user.email
    assert payload["ai_data_sharing_enabled"] is True
    assert payload["ai_data_sharing_consented_by"] == orguser.user.email
    assert payload["ai_org_context_markdown"] == "# Org context"
    assert payload["ai_org_context_updated_by"] == orguser.user.email
    assert payload["ai_chat_source_config"]["dashboard_context"] is False


def test_default_ai_chat_source_config_is_all_enabled():
    """The default source config should enable all supported retrieval sources."""
    assert default_ai_chat_source_config() == {
        "org_context": True,
        "dashboard_context": True,
        "dbt_manifest": True,
        "dbt_catalog": True,
    }


def test_dashboard_to_json_includes_ai_context_fields(org, orguser, seed_db):
    """Dashboard JSON should expose AI context metadata without breaking existing fields."""
    updated_at = timezone.now()
    dashboard = Dashboard.objects.create(
        title="AI Context Dashboard",
        dashboard_type="native",
        ai_context_markdown="## Dashboard context",
        ai_context_updated_by=orguser,
        ai_context_updated_at=updated_at,
        created_by=orguser,
        org=org,
    )

    payload = dashboard.to_json()

    assert payload["title"] == "AI Context Dashboard"
    assert payload["ai_context_markdown"] == "## Dashboard context"
    assert payload["ai_context_updated_by"] == orguser.user.email
    assert payload["ai_context_updated_at"] == updated_at.isoformat()


def test_orgdbt_supports_ai_artifact_metadata(seed_db):
    """OrgDbt should persist artifact freshness fields needed by chat ingestion."""
    generated_at = timezone.now()
    ingested_at = timezone.now()
    orgdbt = OrgDbt.objects.create(
        project_dir="/tmp/dbtrepo",
        target_type="postgres",
        default_schema="analytics",
        ai_manifest_sha256="a" * 64,
        ai_catalog_sha256="b" * 64,
        ai_docs_generated_at=generated_at,
        ai_vector_last_ingested_at=ingested_at,
    )

    assert orgdbt.ai_manifest_sha256 == "a" * 64
    assert orgdbt.ai_catalog_sha256 == "b" * 64
    assert orgdbt.ai_docs_generated_at == generated_at
    assert orgdbt.ai_vector_last_ingested_at == ingested_at


def test_legacy_llm_session_status_enum_remains_unchanged():
    """Legacy LlmSessionStatus should keep the existing summarization lifecycle only."""
    assert ("running", "RUNNING") in LlmSessionStatus.choices()
    assert ("completed", "COMPLETED") in LlmSessionStatus.choices()
    assert ("failed", "FAILED") in LlmSessionStatus.choices()
    assert ("queued", "QUEUED") not in LlmSessionStatus.choices()
    assert ("cancelled", "CANCELLED") not in LlmSessionStatus.choices()


def test_dashboard_chat_session_persists_chat_specific_fields(org, orguser, seed_db):
    """DashboardChatSession should persist chat state independently of legacy LlmSession rows."""
    dashboard = Dashboard.objects.create(
        title="Session Dashboard",
        dashboard_type="native",
        created_by=orguser,
        org=org,
    )
    chart = Chart.objects.create(
        title="Session Chart",
        description="Chart used by chat session",
        chart_type="bar",
        schema_name="analytics",
        table_name="facts",
        extra_config={},
        created_by=orguser,
        last_modified_by=orguser,
        org=org,
    )

    session = DashboardChatSession.objects.create(
        org=org,
        orguser=orguser,
        dashboard=dashboard,
        selected_chart=chart,
        status=DashboardChatSessionStatus.QUEUED,
        messages=[{"role": "user", "content": "How many beneficiaries are there?"}],
        latest_response={"answer_text": "There are 10 beneficiaries."},
        request_meta={"selected_chart_id": chart.id},
        response_meta={"graph_trace": []},
    )

    assert session.status == DashboardChatSessionStatus.QUEUED
    assert session.dashboard_id == dashboard.id
    assert session.selected_chart_id == chart.id
    assert session.messages[0]["role"] == "user"
    assert session.latest_response["answer_text"] == "There are 10 beneficiaries."
    assert session.request_meta["selected_chart_id"] == chart.id
    assert session.response_meta["graph_trace"] == []
    assert session.session_id is not None


def test_dashboard_chat_session_status_enum_covers_chat_lifecycle():
    """DashboardChatSessionStatus should expose the richer chat lifecycle states."""
    assert ("queued", "QUEUED") in DashboardChatSessionStatus.choices()
    assert ("running", "RUNNING") in DashboardChatSessionStatus.choices()
    assert ("completed", "COMPLETED") in DashboardChatSessionStatus.choices()
    assert ("failed", "FAILED") in DashboardChatSessionStatus.choices()
    assert ("cancelled", "CANCELLED") in DashboardChatSessionStatus.choices()
