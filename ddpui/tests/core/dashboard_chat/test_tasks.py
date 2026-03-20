import os
from unittest.mock import Mock, patch

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from django.utils import timezone

from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.celeryworkers.tasks import (
    build_dashboard_chat_context_for_org,
    run_dashboard_chat_turn,
    schedule_dashboard_chat_context_builds,
)
from ddpui.core.dashboard_chat.ingestion import DashboardChatIngestionResult
from ddpui.core.dashboard_chat.runtime_types import DashboardChatIntent, DashboardChatResponse
from ddpui.models.org import Org, OrgDbt
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import DashboardChatMessage, DashboardChatSession
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.tests.api_tests.test_user_org_api import seed_db
from ddpui.utils.feature_flags import enable_feature_flag

pytestmark = pytest.mark.django_db


@pytest.fixture
def orguser(seed_db):
    org = Org.objects.create(
        name="Dashboard Chat Org",
        slug="dashchat",
        airbyte_workspace_id="workspace-id",
    )
    user = User.objects.create(
        username="dashchat-task-user",
        email="dashchat-task-user@test.com",
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
    org.delete()


def _create_org_dbt(org: Org) -> OrgDbt:
    dbt = OrgDbt.objects.create(
        project_dir=f"{org.slug}/dbtrepo",
        dbt_venv="dbt-1.8.7",
        target_type="postgres",
        default_schema="analytics",
    )
    org.dbt = dbt
    org.save(update_fields=["dbt"])
    return dbt


def _create_dashboard(orguser: OrgUser) -> Dashboard:
    return Dashboard.objects.create(
        title="Chat Dashboard",
        dashboard_type="native",
        created_by=orguser,
        last_modified_by=orguser,
        org=orguser.org,
    )


def test_schedule_dashboard_chat_context_builds_enqueues_only_eligible_orgs(orguser):
    eligible_org = orguser.org
    _create_org_dbt(eligible_org)
    OrgPreferences.objects.create(org=eligible_org, ai_data_sharing_enabled=True)
    enable_feature_flag("AI_DASHBOARD_CHAT", org=eligible_org)

    missing_flag_org = Org.objects.create(
        name="Missing Flag",
        slug="missing-flag",
        airbyte_workspace_id="ws-2",
    )
    _create_org_dbt(missing_flag_org)
    OrgPreferences.objects.create(org=missing_flag_org, ai_data_sharing_enabled=True)

    missing_consent_org = Org.objects.create(
        name="Missing Consent",
        slug="missing-consent",
        airbyte_workspace_id="ws-3",
    )
    _create_org_dbt(missing_consent_org)
    OrgPreferences.objects.create(org=missing_consent_org, ai_data_sharing_enabled=False)
    enable_feature_flag("AI_DASHBOARD_CHAT", org=missing_consent_org)

    with patch(
        "ddpui.celeryworkers.tasks.build_dashboard_chat_context_for_org.delay"
    ) as delay_mock:
        result = schedule_dashboard_chat_context_builds()

    delay_mock.assert_called_once_with(eligible_org.id)
    assert result == {"enqueued_org_ids": [eligible_org.id]}


def test_build_dashboard_chat_context_for_org_skips_when_locked(orguser):
    org = orguser.org
    _create_org_dbt(org)
    OrgPreferences.objects.create(org=org, ai_data_sharing_enabled=True)
    enable_feature_flag("AI_DASHBOARD_CHAT", org=org)

    redis_lock = Mock()
    redis_lock.acquire.return_value = False
    redis_client = Mock()
    redis_client.lock.return_value = redis_lock

    with patch("ddpui.celeryworkers.tasks.RedisClient.get_instance", return_value=redis_client), patch(
        "ddpui.celeryworkers.tasks.DashboardChatIngestionService"
    ) as ingestion_service:
        result = build_dashboard_chat_context_for_org.run(org.id)

    assert result == {"status": "skipped_locked", "org_id": org.id}
    ingestion_service.assert_not_called()


def test_build_dashboard_chat_context_for_org_runs_ingestion(orguser):
    org = orguser.org
    _create_org_dbt(org)
    OrgPreferences.objects.create(org=org, ai_data_sharing_enabled=True)
    enable_feature_flag("AI_DASHBOARD_CHAT", org=org)

    redis_lock = Mock()
    redis_lock.acquire.return_value = True
    redis_lock.owned.return_value = True
    redis_client = Mock()
    redis_client.lock.return_value = redis_lock

    result_payload = DashboardChatIngestionResult(
        org_id=org.id,
        docs_generated_at=timezone.now(),
        vector_ingested_at=timezone.now(),
        source_document_counts={"dashboard_export": 2},
        upserted_document_ids=["abc"],
        deleted_document_ids=[],
    )
    ingestion_service = Mock()
    ingestion_service.ingest_org.return_value = result_payload

    with patch("ddpui.celeryworkers.tasks.RedisClient.get_instance", return_value=redis_client), patch(
        "ddpui.celeryworkers.tasks.DashboardChatIngestionService",
        return_value=ingestion_service,
    ):
        result = build_dashboard_chat_context_for_org.run(org.id)

    assert result["status"] == "completed"
    assert result["org_id"] == org.id
    assert result["source_document_counts"] == {"dashboard_export": 2}
    ingestion_service.ingest_org.assert_called_once()
    redis_lock.release.assert_called_once()


@patch("ddpui.celeryworkers.tasks.publish_dashboard_chat_event")
@patch("ddpui.celeryworkers.tasks.DashboardChatRuntime")
def test_run_dashboard_chat_turn_persists_assistant_message_and_publishes_event(
    runtime_class,
    publish_event,
    orguser,
):
    _create_org_dbt(orguser.org)
    dashboard = _create_dashboard(orguser)
    session = DashboardChatSession.objects.create(
        org=orguser.org,
        orguser=orguser,
        dashboard=dashboard,
    )
    user_message = DashboardChatMessage.objects.create(
        session=session,
        sequence_number=1,
        role="user",
        content="Why did funding drop?",
    )
    runtime_class.return_value.run.return_value = DashboardChatResponse(
        answer_text="Funding dropped because donor inflows slowed this quarter.",
        intent=DashboardChatIntent.DATA_QUERY,
        warnings=["Example warning"],
        sql="SELECT 1",
        sql_results=[{"value": 1}],
    )

    result = run_dashboard_chat_turn(str(session.session_id), user_message.id)

    assistant_message = DashboardChatMessage.objects.get(session=session, role="assistant")
    assert assistant_message.sequence_number == 2
    assert assistant_message.content == "Funding dropped because donor inflows slowed this quarter."
    assert assistant_message.payload["sql"] == "SELECT 1"
    assert result["status"] == "completed"
    publish_event.assert_called_once()


@patch("ddpui.celeryworkers.tasks.publish_dashboard_chat_event")
@patch("ddpui.celeryworkers.tasks.DashboardChatRuntime")
def test_run_dashboard_chat_turn_publishes_error_when_runtime_fails(
    runtime_class,
    publish_event,
    orguser,
):
    _create_org_dbt(orguser.org)
    dashboard = _create_dashboard(orguser)
    session = DashboardChatSession.objects.create(
        org=orguser.org,
        orguser=orguser,
        dashboard=dashboard,
    )
    user_message = DashboardChatMessage.objects.create(
        session=session,
        sequence_number=1,
        role="user",
        content="Why did funding drop?",
    )
    runtime_class.return_value.run.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        run_dashboard_chat_turn(str(session.session_id), user_message.id)

    assert DashboardChatMessage.objects.filter(session=session, role="assistant").count() == 0
    publish_event.assert_called_once()
