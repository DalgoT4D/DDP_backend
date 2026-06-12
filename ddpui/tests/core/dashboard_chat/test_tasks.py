from unittest.mock import Mock, patch

import pytest

from django.contrib.auth.models import User
from django.utils import timezone

from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.celeryworkers.tasks import (
    build_dashboard_chat_context_for_org,
    schedule_dashboard_chat_context_builds,
)
from ddpui.core.dashboard_chat.vector.org_vector_context_build_service import OrgVectorBuildResult
from ddpui.models.org import Org, OrgDbt
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


def test_build_dashboard_chat_context_for_org_skips_when_org_is_missing():
    result = build_dashboard_chat_context_for_org.run(999999)

    assert result == {"status": "skipped_missing_org", "org_id": 999999}


def test_build_dashboard_chat_context_for_org_skips_when_org_is_ineligible(orguser):
    org = orguser.org
    _create_org_dbt(org)
    OrgPreferences.objects.create(org=org, ai_data_sharing_enabled=False)

    with patch("ddpui.celeryworkers.tasks.OrgVectorBuildService") as vector_build_service:
        result = build_dashboard_chat_context_for_org.run(org.id)

    assert result == {"status": "skipped_ineligible", "org_id": org.id}
    vector_build_service.assert_not_called()


def test_build_dashboard_chat_context_for_org_runs_vector_build(orguser):
    org = orguser.org
    _create_org_dbt(org)
    OrgPreferences.objects.create(org=org, ai_data_sharing_enabled=True)
    enable_feature_flag("AI_DASHBOARD_CHAT", org=org)

    result_payload = OrgVectorBuildResult(
        org_id=org.id,
        docs_generated_at=timezone.now(),
        vector_ingested_at=timezone.now(),
        source_document_counts={"dashboard_export": 2},
        upserted_document_ids=["abc"],
        deleted_document_ids=[],
    )
    vector_build_service = Mock()
    vector_build_service.build_org_vector_context.return_value = result_payload

    with patch("ddpui.celeryworkers.tasks.OrgVectorBuildService", return_value=vector_build_service):
        result = build_dashboard_chat_context_for_org.run(org.id)

    assert result["status"] == "completed"
    assert result["org_id"] == org.id
    assert result["source_document_counts"] == {"dashboard_export": 2}
    vector_build_service.build_org_vector_context.assert_called_once()
