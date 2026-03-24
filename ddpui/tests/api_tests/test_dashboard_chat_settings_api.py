import os
import django

import pytest
from ninja.errors import HttpError
from django.contrib.auth.models import User
from django.utils import timezone

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.api.org_preferences_api import (
    get_ai_dashboard_chat_settings,
    update_ai_dashboard_chat_settings,
    get_ai_dashboard_chat_status,
)
from ddpui.api.dashboard_native_api import (
    get_dashboard_ai_context,
    update_dashboard_ai_context,
)
from ddpui.auth import ACCOUNT_MANAGER_ROLE, GUEST_ROLE
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import DashboardAIContext, OrgAIContext
from ddpui.models.org import Org, OrgDbt
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.schemas.dashboard_schema import UpdateDashboardAIContextSchema
from ddpui.schemas.org_preferences_schema import UpdateOrgAIDashboardChatSchema
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request
from ddpui.utils.feature_flags import enable_feature_flag

pytestmark = pytest.mark.django_db


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="chatsettingsuser",
        email="chatsettingsuser@test.com",
        password="testpassword",
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Dashboard Chat Settings Org",
        slug="chat-settings-org",
        airbyte_workspace_id="workspace-id",
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
def guest_orguser(org, seed_db):
    guest_user = User.objects.create(
        username="chatsettingsguest",
        email="chatsettingsguest@test.com",
        password="testpassword",
    )
    orguser = OrgUser.objects.create(
        user=guest_user,
        org=org,
        new_role=Role.objects.filter(slug=GUEST_ROLE).first(),
    )
    yield orguser
    orguser.delete()
    guest_user.delete()


@pytest.fixture
def other_org_dashboard(seed_db):
    other_org = Org.objects.create(
        name="Other Dashboard Chat Org",
        slug="other-chat-org",
        airbyte_workspace_id="other-workspace-id",
    )
    other_user = User.objects.create(
        username="otherchatsettingsuser",
        email="otherchatsettingsuser@test.com",
        password="testpassword",
    )
    other_orguser = OrgUser.objects.create(
        user=other_user,
        org=other_org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    dashboard = Dashboard.objects.create(
        title="Other Org Dashboard",
        description="Should not be accessible",
        created_by=other_orguser,
        org=other_org,
    )
    yield dashboard
    dashboard.delete()
    other_orguser.delete()
    other_user.delete()
    other_org.delete()


@pytest.fixture
def dashboard(orguser, org):
    dashboard = Dashboard.objects.create(
        title="Donor Report",
        description="Dashboard description",
        created_by=orguser,
        org=org,
    )
    yield dashboard
    dashboard.delete()


def test_get_ai_dashboard_chat_settings_returns_enveloped_response(orguser, seed_db):
    request = mock_request(orguser)
    enable_feature_flag("AI_DASHBOARD_CHAT", org=orguser.org)

    response = get_ai_dashboard_chat_settings(request)

    assert response["success"] is True
    assert response["res"]["feature_flag_enabled"] is True
    assert response["res"]["ai_data_sharing_enabled"] is False
    assert response["res"]["org_context_markdown"] == ""
    assert response["res"]["dbt_configured"] is False

    preferences = OrgPreferences.objects.get(org=orguser.org)
    context = OrgAIContext.objects.get(org=orguser.org)
    assert preferences.ai_data_sharing_enabled is False
    assert context.markdown == ""


def test_update_ai_dashboard_chat_settings_stamps_consent_and_context(orguser, seed_db):
    request = mock_request(orguser)
    enable_feature_flag("AI_DASHBOARD_CHAT", org=orguser.org)
    payload = UpdateOrgAIDashboardChatSchema(
        ai_data_sharing_enabled=True,
        org_context_markdown="## Org context",
    )

    response = update_ai_dashboard_chat_settings(request, payload)

    assert response["success"] is True
    assert response["res"]["ai_data_sharing_enabled"] is True
    assert response["res"]["ai_data_sharing_consented_by"] == orguser.user.email
    assert response["res"]["org_context_markdown"] == "## Org context"
    assert response["res"]["org_context_updated_by"] == orguser.user.email

    preferences = OrgPreferences.objects.get(org=orguser.org)
    context = OrgAIContext.objects.get(org=orguser.org)
    assert preferences.ai_data_sharing_enabled is True
    assert preferences.ai_data_sharing_consented_by == orguser
    assert preferences.ai_data_sharing_consented_at is not None
    assert context.markdown == "## Org context"
    assert context.updated_by == orguser
    assert context.updated_at is not None


def test_get_ai_dashboard_chat_status_reports_chat_available(orguser, seed_db):
    request = mock_request(orguser)
    enable_feature_flag("AI_DASHBOARD_CHAT", org=orguser.org)

    generated_at = timezone.now()
    ingested_at = timezone.now()
    org_dbt = OrgDbt.objects.create(
        project_dir="dbt/project",
        target_type="postgres",
        default_schema="analytics",
        docs_generated_at=generated_at,
        vector_last_ingested_at=ingested_at,
    )
    org = orguser.org
    org.dbt = org_dbt
    org.save(update_fields=["dbt"])

    OrgPreferences.objects.create(
        org=org,
        ai_data_sharing_enabled=True,
        ai_data_sharing_consented_by=orguser,
        ai_data_sharing_consented_at=timezone.now(),
    )

    response = get_ai_dashboard_chat_status(request)

    assert response["success"] is True
    assert response["res"]["feature_flag_enabled"] is True
    assert response["res"]["ai_data_sharing_enabled"] is True
    assert response["res"]["dbt_configured"] is True
    assert response["res"]["chat_available"] is True
    assert response["res"]["docs_generated_at"] == generated_at
    assert response["res"]["vector_last_ingested_at"] == ingested_at


def test_get_ai_dashboard_chat_settings_requires_permission(guest_orguser, seed_db):
    request = mock_request(guest_orguser)

    with pytest.raises(HttpError) as excinfo:
        get_ai_dashboard_chat_settings(request)

    assert excinfo.value.status_code == 404
    assert str(excinfo.value) == "unauthorized"


def test_get_dashboard_ai_context_returns_direct_payload(orguser, dashboard, seed_db):
    request = mock_request(orguser)
    enable_feature_flag("AI_DASHBOARD_CHAT", org=orguser.org)

    response = get_dashboard_ai_context(request, dashboard.id)

    assert response.dashboard_id == dashboard.id
    assert response.dashboard_title == dashboard.title
    assert response.dashboard_context_markdown == ""
    assert response.dashboard_context_updated_by is None
    assert response.vector_last_ingested_at is None


def test_update_dashboard_ai_context_persists_context(orguser, dashboard, seed_db):
    request = mock_request(orguser)
    enable_feature_flag("AI_DASHBOARD_CHAT", org=orguser.org)
    OrgPreferences.objects.create(
        org=orguser.org,
        ai_data_sharing_enabled=True,
        ai_data_sharing_consented_by=orguser,
        ai_data_sharing_consented_at=timezone.now(),
    )
    payload = UpdateDashboardAIContextSchema(dashboard_context_markdown="## Dashboard context")

    response = update_dashboard_ai_context(request, dashboard.id, payload)

    assert response.dashboard_id == dashboard.id
    assert response.dashboard_context_markdown == "## Dashboard context"
    assert response.dashboard_context_updated_by == orguser.user.email

    context = DashboardAIContext.objects.get(dashboard=dashboard)
    assert context.markdown == "## Dashboard context"
    assert context.updated_by == orguser
    assert context.updated_at is not None


def test_get_dashboard_ai_context_requires_permission(guest_orguser, dashboard, seed_db):
    request = mock_request(guest_orguser)

    with pytest.raises(HttpError) as excinfo:
        get_dashboard_ai_context(request, dashboard.id)

    assert excinfo.value.status_code == 404
    assert str(excinfo.value) == "unauthorized"


def test_get_dashboard_ai_context_is_org_scoped(orguser, other_org_dashboard, seed_db):
    request = mock_request(orguser)
    enable_feature_flag("AI_DASHBOARD_CHAT", org=orguser.org)

    with pytest.raises(HttpError) as excinfo:
        get_dashboard_ai_context(request, other_org_dashboard.id)

    assert excinfo.value.status_code == 404
    assert str(excinfo.value) == "Dashboard not found"


def test_dashboard_chat_settings_are_hidden_when_feature_flag_is_off(orguser, seed_db):
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_ai_dashboard_chat_settings(request)

    assert excinfo.value.status_code == 404
    assert str(excinfo.value) == "Chat with dashboards is not enabled for this organization"


def test_update_ai_dashboard_chat_settings_rejects_context_without_consent(orguser, seed_db):
    request = mock_request(orguser)
    enable_feature_flag("AI_DASHBOARD_CHAT", org=orguser.org)

    with pytest.raises(HttpError) as excinfo:
        update_ai_dashboard_chat_settings(
            request,
            UpdateOrgAIDashboardChatSchema(org_context_markdown="## Org context"),
        )

    assert excinfo.value.status_code == 409
    assert str(excinfo.value) == "Enable AI data sharing before updating organization AI context"


def test_dashboard_ai_context_is_hidden_when_feature_flag_is_off(orguser, dashboard, seed_db):
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_dashboard_ai_context(request, dashboard.id)

    assert excinfo.value.status_code == 404
    assert str(excinfo.value) == "Chat with dashboards is not enabled for this organization"


def test_update_dashboard_ai_context_requires_ai_consent(orguser, dashboard, seed_db):
    request = mock_request(orguser)
    enable_feature_flag("AI_DASHBOARD_CHAT", org=orguser.org)

    with pytest.raises(HttpError) as excinfo:
        update_dashboard_ai_context(
            request,
            dashboard.id,
            UpdateDashboardAIContextSchema(dashboard_context_markdown="## Dashboard context"),
        )

    assert excinfo.value.status_code == 409
    assert str(excinfo.value) == "Enable AI data sharing before updating dashboard AI context"
