"""API tests for AI dashboard chat settings and dashboard context endpoints."""

import os
import django
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from django.utils import timezone

from ddpui.auth import ACCOUNT_MANAGER_ROLE, GUEST_ROLE
from ddpui.api.dashboard_native_api import get_dashboard_ai_context, update_dashboard_ai_context
from ddpui.api.org_preferences_api import (
    get_ai_dashboard_chat_settings,
    get_ai_dashboard_chat_status,
    update_ai_dashboard_chat_settings,
)
from ddpui.models.dashboard import Dashboard
from ddpui.models.org import Org, OrgDbt
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.schemas.dashboard_schema import DashboardAiContextUpdate
from ddpui.schemas.org_preferences_schema import (
    AiChatSourceConfigSchema,
    UpdateAiDashboardChatSettingsSchema,
)
from ddpui.tests.api_tests.test_user_org_api import mock_request, seed_db
from ddpui.utils.feature_flags import enable_feature_flag

pytestmark = pytest.mark.django_db


@pytest.fixture
def manager_user():
    user = User.objects.create(
        username="aichatmanager", email="aichatmanager@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def guest_user():
    user = User.objects.create(
        username="aichatguest", email="aichatguest@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def other_manager_user():
    user = User.objects.create(
        username="aichatother", email="aichatother@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(name="AI Chat Org", slug="aichat-org", airbyte_workspace_id="ws-1")
    yield org
    org.delete()


@pytest.fixture
def other_org():
    org = Org.objects.create(name="Other AI Org", slug="aichat-org-2", airbyte_workspace_id="ws-2")
    yield org
    org.delete()


@pytest.fixture
def manager_orguser(manager_user, org, seed_db):
    orguser = OrgUser.objects.create(
        user=manager_user,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def guest_orguser(guest_user, org, seed_db):
    orguser = OrgUser.objects.create(
        user=guest_user,
        org=org,
        new_role=Role.objects.filter(slug=GUEST_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def other_manager_orguser(other_manager_user, other_org, seed_db):
    orguser = OrgUser.objects.create(
        user=other_manager_user,
        org=other_org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


def attach_org_dbt(org: Org, *, vector_last_ingested_at=None) -> OrgDbt:
    """Create and attach a dbt config row to an org for AI ingest freshness checks."""
    orgdbt = OrgDbt.objects.create(
        project_dir="/tmp/dbt-project",
        target_type="postgres",
        default_schema="analytics",
        ai_vector_last_ingested_at=vector_last_ingested_at,
    )
    org.dbt = orgdbt
    org.save(update_fields=["dbt"])
    return orgdbt


def test_get_ai_dashboard_chat_settings_returns_org_scoped_values(
    manager_orguser, other_manager_orguser, seed_db
):
    """The settings endpoint should only expose the current org's AI settings."""
    enable_feature_flag("AI_DASHBOARD_CHAT", org=manager_orguser.org)
    OrgPreferences.objects.create(
        org=manager_orguser.org,
        ai_data_sharing_enabled=True,
        ai_org_context_markdown="# Primary org context",
    )
    OrgPreferences.objects.create(
        org=other_manager_orguser.org,
        ai_data_sharing_enabled=False,
        ai_org_context_markdown="# Other org context",
    )

    response = get_ai_dashboard_chat_settings(mock_request(manager_orguser))

    assert response["success"] is True
    assert response["res"]["feature_flag_enabled"] is True
    assert response["res"]["ai_data_sharing_enabled"] is True
    assert response["res"]["ai_org_context_markdown"] == "# Primary org context"
    assert response["res"]["ai_chat_source_config"]["org_context"] is True
    assert response["res"]["ai_chat_source_config"]["dashboard_context"] is True


def test_update_ai_dashboard_chat_settings_requires_manage_permission(guest_orguser, seed_db):
    """Only privileged org roles should be able to modify AI chat settings."""
    request = mock_request(guest_orguser)

    with pytest.raises(HttpError) as excinfo:
        update_ai_dashboard_chat_settings(
            request,
            UpdateAiDashboardChatSettingsSchema(ai_data_sharing_enabled=True),
        )

    assert str(excinfo.value) == "unauthorized"


def test_update_ai_dashboard_chat_settings_persists_changes_and_marks_dirty(
    manager_orguser, seed_db
):
    """Updating org chat settings should persist consent/context and clear ingest freshness."""
    attach_org_dbt(manager_orguser.org, vector_last_ingested_at=timezone.now())
    request = mock_request(manager_orguser)

    response = update_ai_dashboard_chat_settings(
        request,
        UpdateAiDashboardChatSettingsSchema(
            ai_data_sharing_enabled=True,
            ai_org_context_markdown="# Updated org context",
            ai_chat_source_config=AiChatSourceConfigSchema(dashboard_context=False),
        ),
    )

    preferences = OrgPreferences.objects.get(org=manager_orguser.org)
    manager_orguser.org.refresh_from_db()
    manager_orguser.org.dbt.refresh_from_db()

    assert response["success"] is True
    assert response["res"]["ai_data_sharing_enabled"] is True
    assert response["res"]["ai_org_context_markdown"] == "# Updated org context"
    assert response["res"]["ai_chat_source_config"] == {
        "org_context": True,
        "dashboard_context": False,
        "dbt_manifest": True,
        "dbt_catalog": True,
    }
    assert preferences.ai_data_sharing_consented_by_id == manager_orguser.id
    assert preferences.ai_org_context_updated_by_id == manager_orguser.id
    assert manager_orguser.org.dbt.ai_vector_last_ingested_at is None


def test_get_ai_dashboard_chat_status_returns_effective_org_state(
    manager_orguser, other_manager_orguser, seed_db
):
    """Status should combine feature flag state and consent state per org."""
    enable_feature_flag("AI_DASHBOARD_CHAT", org=manager_orguser.org)
    OrgPreferences.objects.create(org=manager_orguser.org, ai_data_sharing_enabled=True)
    OrgPreferences.objects.create(org=other_manager_orguser.org, ai_data_sharing_enabled=True)

    enabled_status = get_ai_dashboard_chat_status(mock_request(manager_orguser))
    disabled_status = get_ai_dashboard_chat_status(mock_request(other_manager_orguser))

    assert enabled_status["success"] is True
    assert enabled_status["res"]["feature_flag_enabled"] is True
    assert enabled_status["res"]["chat_available"] is True
    assert disabled_status["res"]["feature_flag_enabled"] is False
    assert disabled_status["res"]["chat_available"] is False


def test_get_dashboard_ai_context_requires_manage_permission(guest_orguser, seed_db):
    """Only privileged org roles should be able to fetch dashboard AI context."""
    dashboard = Dashboard.objects.create(
        title="Guest Dashboard",
        dashboard_type="native",
        created_by=guest_orguser,
        org=guest_orguser.org,
    )

    with pytest.raises(HttpError) as excinfo:
        get_dashboard_ai_context(mock_request(guest_orguser), dashboard.id)

    assert str(excinfo.value) == "unauthorized"


def test_get_dashboard_ai_context_is_org_scoped(manager_orguser, other_manager_orguser, seed_db):
    """Dashboard AI context lookups should not leak across organizations."""
    other_dashboard = Dashboard.objects.create(
        title="Other Org Dashboard",
        dashboard_type="native",
        created_by=other_manager_orguser,
        org=other_manager_orguser.org,
    )

    with pytest.raises(HttpError) as excinfo:
        get_dashboard_ai_context(mock_request(manager_orguser), other_dashboard.id)

    assert excinfo.value.status_code == 404
    assert str(excinfo.value) == "Dashboard not found"


def test_update_dashboard_ai_context_persists_and_marks_dirty(manager_orguser, seed_db):
    """Saving dashboard context should persist the markdown and clear ingest freshness."""
    attach_org_dbt(manager_orguser.org, vector_last_ingested_at=timezone.now())
    dashboard = Dashboard.objects.create(
        title="Context Dashboard",
        dashboard_type="native",
        created_by=manager_orguser,
        org=manager_orguser.org,
    )

    response = update_dashboard_ai_context(
        mock_request(manager_orguser),
        dashboard.id,
        DashboardAiContextUpdate(ai_context_markdown="## Dashboard context"),
    )

    dashboard.refresh_from_db()
    manager_orguser.org.refresh_from_db()
    manager_orguser.org.dbt.refresh_from_db()

    assert response["success"] is True
    assert response["res"]["dashboard_id"] == dashboard.id
    assert response["res"]["ai_context_markdown"] == "## Dashboard context"
    assert dashboard.ai_context_updated_by_id == manager_orguser.id
    assert manager_orguser.org.dbt.ai_vector_last_ingested_at is None
