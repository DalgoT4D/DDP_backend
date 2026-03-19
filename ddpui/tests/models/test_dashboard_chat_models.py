import os
from datetime import timedelta

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from django.db import IntegrityError
from django.db import transaction
from django.utils import timezone

from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import (
    DashboardAIContext,
    DashboardChatMessage,
    DashboardChatSession,
    OrgAIContext,
)
from ddpui.models.org import Org, OrgDbt
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="dashchatmodeluser",
        email="dashchatmodeluser@test.com",
        password="testpassword",
    )
    yield user
    user.delete()


@pytest.fixture
def org_dbt():
    dbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/example/dbt.git",
        project_dir="/tmp/dbt",
        target_type="bigquery",
        default_schema="analytics",
    )
    yield dbt
    dbt.delete()


@pytest.fixture
def org(org_dbt):
    org = Org.objects.create(
        name="Dashboard Chat Org",
        slug="dash-chat-org",
        airbyte_workspace_id="workspace-id",
        dbt=org_dbt,
    )
    yield org
    org.delete()


@pytest.fixture
def orguser(authuser, org, seed_db):
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def dashboard(org, orguser):
    dashboard = Dashboard.objects.create(
        title="Chat Dashboard",
        dashboard_type="native",
        created_by=orguser,
        last_modified_by=orguser,
        org=org,
    )
    yield dashboard
    dashboard.delete()


def test_org_preferences_ai_consent_defaults(org):
    preferences = OrgPreferences.objects.create(org=org)

    assert preferences.ai_data_sharing_enabled is False
    assert preferences.ai_data_sharing_consented_by is None
    assert preferences.ai_data_sharing_consented_at is None


def test_org_preferences_ai_consent_persists(org, orguser):
    consented_at = timezone.now()
    preferences = OrgPreferences.objects.create(
        org=org,
        ai_data_sharing_enabled=True,
        ai_data_sharing_consented_by=orguser,
        ai_data_sharing_consented_at=consented_at,
    )

    assert preferences.ai_data_sharing_enabled is True
    assert preferences.ai_data_sharing_consented_by == orguser
    assert preferences.ai_data_sharing_consented_at == consented_at


def test_org_dbt_ai_freshness_fields(org_dbt):
    generated_at = timezone.now()
    ingested_at = generated_at + timedelta(minutes=5)

    org_dbt.docs_generated_at = generated_at
    org_dbt.vector_last_ingested_at = ingested_at
    org_dbt.save()

    org_dbt.refresh_from_db()
    assert org_dbt.docs_generated_at == generated_at
    assert org_dbt.vector_last_ingested_at == ingested_at


def test_org_ai_context_one_to_one(org, orguser):
    context = OrgAIContext.objects.create(
        org=org,
        markdown="## Org context",
        updated_by=orguser,
        updated_at=timezone.now(),
    )

    assert context.org == org
    assert context.markdown == "## Org context"
    assert org.ai_context == context


def test_dashboard_ai_context_one_to_one(dashboard, orguser):
    context = DashboardAIContext.objects.create(
        dashboard=dashboard,
        markdown="## Dashboard context",
        updated_by=orguser,
        updated_at=timezone.now(),
    )

    assert context.dashboard == dashboard
    assert context.markdown == "## Dashboard context"
    assert dashboard.ai_context == context


def test_dashboard_chat_session_defaults(org, orguser, dashboard):
    session = DashboardChatSession.objects.create(
        org=org,
        orguser=orguser,
        dashboard=dashboard,
    )

    assert session.session_id is not None
    assert session.org == org
    assert session.orguser == orguser
    assert session.dashboard == dashboard


def test_dashboard_chat_message_sequence_uniqueness(org, orguser, dashboard):
    session = DashboardChatSession.objects.create(
        org=org,
        orguser=orguser,
        dashboard=dashboard,
    )
    DashboardChatMessage.objects.create(
        session=session,
        sequence_number=1,
        role="user",
        content="What changed this quarter?",
    )

    with pytest.raises(IntegrityError):
        with transaction.atomic():
            DashboardChatMessage.objects.create(
                session=session,
                sequence_number=1,
                role="assistant",
                content="Here is the answer.",
            )


def test_dashboard_chat_message_payload(org, orguser, dashboard):
    session = DashboardChatSession.objects.create(
        org=org,
        orguser=orguser,
        dashboard=dashboard,
    )
    message = DashboardChatMessage.objects.create(
        session=session,
        sequence_number=2,
        role="assistant",
        content="Funding dropped because Q4 grants closed early.",
        payload={
            "citations": [{"source_type": "dashboard_export", "title": "Funding by quarter"}],
            "warnings": [],
            "sql": "SELECT quarter, SUM(amount) FROM funding GROUP BY 1",
        },
    )

    assert message.payload["citations"][0]["source_type"] == "dashboard_export"
    assert message.payload["sql"].startswith("SELECT")
