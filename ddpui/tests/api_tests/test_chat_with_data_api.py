"""API tests for Chat with Data endpoints — status, session CRUD, history."""

import os
import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User

from ninja.errors import HttpError

from ddpui.api.chat_with_data_api import (
    create_session,
    delete_session,
    get_status,
    list_sessions,
    rename_session,
)
from ddpui.models.chat_with_data import ChatWithDataSession
from ddpui.models.dashboard import Dashboard
from ddpui.models.visualization import Chart
from ddpui.schemas.chat_with_data_schemas import SessionCreate, SessionRename
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request
from ddpui.utils import feature_flags

pytestmark = pytest.mark.django_db


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="cwdapiuser", email="cwdapiuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="CWD API Test Org", slug="cwd-api-test", airbyte_workspace_id="workspace-id"
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


# ── Status ──────────────────────────────────────────────────────────────────


def test_status_disabled_when_feature_flag_off(orguser, seed_db):
    response = get_status(mock_request(orguser))
    assert response["success"] is True
    assert response["data"]["enabled"] is False
    assert response["data"]["reason"] == "feature_disabled"


def test_status_requires_llm_consent_then_warehouse_then_ok(orguser, org, seed_db):
    feature_flags.enable_feature_flag("CHAT_WITH_DATA", org)

    assert get_status(mock_request(orguser))["data"]["reason"] == "llm_consent_required"

    OrgPreferences.objects.create(org=org, llm_optin=True)
    assert get_status(mock_request(orguser))["data"]["reason"] == "no_warehouse"

    OrgWarehouse.objects.create(org=org, wtype="postgres")
    response = get_status(mock_request(orguser))
    assert response["data"] == {"enabled": True, "reason": "ok"}


# ── Sessions ────────────────────────────────────────────────────────────────


@pytest.fixture
def other_orguser(org, seed_db):
    """A different user in the SAME org — must not see the first user's sessions."""
    user = User.objects.create(username="cwdother", email="cwdother@test.com", password="x")
    ou = OrgUser.objects.create(
        user=user,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield ou
    ou.delete()
    user.delete()


def test_session_lifecycle_create_list_rename_delete(orguser, seed_db):
    created = create_session(mock_request(orguser))
    session_id = created["data"]["id"]
    assert created["data"]["title"] == "New chat"

    listed = list_sessions(mock_request(orguser))
    assert [s["id"] for s in listed["data"]] == [session_id]

    renamed = rename_session(mock_request(orguser), session_id, SessionRename(title="Pune surveys"))
    assert renamed["data"]["title"] == "Pune surveys"

    delete_session(mock_request(orguser), session_id)
    assert list_sessions(mock_request(orguser))["data"] == []
    # soft delete: the row survives with deleted_at set
    assert ChatWithDataSession.objects.get(id=session_id).deleted_at is not None


@pytest.fixture
def dashboard_with_chart(org, orguser, seed_db):
    chart = Chart.objects.create(
        title="Surveys by district",
        chart_type="bar",
        computation_type="aggregated",
        schema_name="prod",
        table_name="surveys",
        org=org,
        created_by=orguser,
    )
    dashboard = Dashboard.objects.create(
        title="Field Performance",
        org=org,
        created_by=orguser,
        tabs=[
            {
                "id": "t1",
                "title": "Main",
                "layout_config": {},
                "components": {"c1": {"type": "chart", "config": {"chartId": chart.id}}},
            }
        ],
    )
    yield dashboard
    dashboard.delete()
    chart.delete()


def test_create_session_with_dashboard_scope(orguser, dashboard_with_chart, seed_db):
    created = create_session(
        mock_request(orguser),
        SessionCreate(scope_type="dashboard", scope_id=dashboard_with_chart.id),
    )
    assert created["data"]["scope_type"] == "dashboard"
    assert created["data"]["scope_id"] == dashboard_with_chart.id

    session = ChatWithDataSession.objects.get(id=created["data"]["id"])
    assert session.scope_type == "dashboard"
    assert session.scope_id == dashboard_with_chart.id


def test_create_session_empty_body_still_makes_org_session(orguser, seed_db):
    # legacy clients POST with no payload — must keep working
    created = create_session(mock_request(orguser))
    assert created["data"]["scope_type"] == "org"
    assert created["data"]["scope_id"] is None


def test_create_session_dashboard_scope_requires_scope_id(orguser, seed_db):
    with pytest.raises(HttpError, match="scope_id"):
        create_session(mock_request(orguser), SessionCreate(scope_type="dashboard"))


def test_create_session_rejects_missing_or_cross_org_dashboard(orguser, seed_db):
    with pytest.raises(HttpError):
        create_session(
            mock_request(orguser), SessionCreate(scope_type="dashboard", scope_id=999999)
        )


def test_create_session_rejects_empty_dashboard(orguser, org, seed_db):
    empty = Dashboard.objects.create(title="Empty", org=org, created_by=orguser, tabs=[])
    with pytest.raises(HttpError, match="no charts"):
        create_session(
            mock_request(orguser), SessionCreate(scope_type="dashboard", scope_id=empty.id)
        )


def test_list_sessions_can_filter_by_scope_type(orguser, dashboard_with_chart, seed_db):
    create_session(mock_request(orguser))  # org session
    create_session(
        mock_request(orguser),
        SessionCreate(scope_type="dashboard", scope_id=dashboard_with_chart.id),
    )

    org_only = list_sessions(mock_request(orguser), scope_type="org")
    assert [s["scope_type"] for s in org_only["data"]] == ["org"]

    everything = list_sessions(mock_request(orguser))
    assert len(everything["data"]) == 2


def test_sessions_are_owner_scoped_within_the_org(orguser, other_orguser, seed_db):
    created = create_session(mock_request(orguser))
    session_id = created["data"]["id"]

    # same org, different user: invisible and untouchable
    assert list_sessions(mock_request(other_orguser))["data"] == []
    with pytest.raises(HttpError):
        rename_session(mock_request(other_orguser), session_id, SessionRename(title="hijack"))
    with pytest.raises(HttpError):
        delete_session(mock_request(other_orguser), session_id)
