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
    get_settings,
    get_status,
    list_sessions,
    rename_session,
    update_settings,
)
from ddpui.core.ai.agent.org_memory import MAX_ORG_MEMORY_CHARS
from ddpui.models.chat_with_data import ChatWithDataOrgMemory, ChatWithDataSession
from ddpui.schemas.chat_with_data_schemas import CopilotSettingsUpdate, SessionRename
from ddpui.auth import ACCOUNT_MANAGER_ROLE, ANALYST_ROLE
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
    assert response["data"]["enabled"] is True
    assert response["data"]["reason"] == "ok"
    # model selector data rides on the status payload
    assert response["data"]["default_model"]
    offered = [m["id"] for m in response["data"]["models"]]
    assert all(isinstance(m, str) for m in offered)


# ── Copilot settings (enable toggle + org memory) ───────────────────────────


@pytest.fixture
def analyst_orguser(org, seed_db):
    """Role 4 — has neither chat access nor settings access after v1.1."""
    user = User.objects.create(username="cwdanalyst", email="cwdanalyst@test.com", password="x")
    ou = OrgUser.objects.create(
        user=user, org=org, new_role=Role.objects.filter(slug=ANALYST_ROLE).first()
    )
    yield ou
    ou.delete()
    user.delete()


def test_settings_default_shape_when_nothing_configured(orguser, seed_db):
    data = get_settings(mock_request(orguser))["data"]
    assert data == {
        "enabled": False,
        "text": "",
        "updated_at": None,
        "updated_by_email": None,
        "max_chars": MAX_ORG_MEMORY_CHARS,
    }


def test_toggle_writes_the_org_feature_flag_row(orguser, org, seed_db):
    data = update_settings(mock_request(orguser), CopilotSettingsUpdate(enabled=True))["data"]
    assert data["enabled"] is True
    assert feature_flags.is_feature_flag_enabled("CHAT_WITH_DATA", org) is True

    data = update_settings(mock_request(orguser), CopilotSettingsUpdate(enabled=False))["data"]
    assert data["enabled"] is False
    assert feature_flags.is_feature_flag_enabled("CHAT_WITH_DATA", org) is False


def test_memory_round_trips_stripped_with_author(orguser, authuser, org, seed_db):
    payload = CopilotSettingsUpdate(text="  'SHG' means self-help group.  ")
    data = update_settings(mock_request(orguser), payload)["data"]
    assert data["text"] == "'SHG' means self-help group."
    assert data["updated_by_email"] == authuser.email

    memory = ChatWithDataOrgMemory.objects.get(org=org)
    assert memory.text == "'SHG' means self-help group."
    assert memory.updated_by == orguser

    # empty string is a valid clear, not "field omitted"
    data = update_settings(mock_request(orguser), CopilotSettingsUpdate(text=""))["data"]
    assert data["text"] == ""


def test_memory_over_cap_is_rejected_with_400(orguser, seed_db):
    payload = CopilotSettingsUpdate(text="x" * (MAX_ORG_MEMORY_CHARS + 1))
    with pytest.raises(HttpError):
        update_settings(mock_request(orguser), payload)
    assert not ChatWithDataOrgMemory.objects.exists()


def test_partial_update_of_enabled_leaves_memory_untouched(orguser, org, seed_db):
    update_settings(mock_request(orguser), CopilotSettingsUpdate(text="fiscal year Apr-Mar"))
    data = update_settings(mock_request(orguser), CopilotSettingsUpdate(enabled=True))["data"]
    assert data["text"] == "fiscal year Apr-Mar"
    assert ChatWithDataOrgMemory.objects.get(org=org).text == "fiscal year Apr-Mar"


def test_analyst_can_neither_manage_settings_nor_chat(analyst_orguser, seed_db):
    # settings were never theirs; chat access was revoked in v1.1 (seed change)
    with pytest.raises(HttpError):
        get_settings(mock_request(analyst_orguser))
    with pytest.raises(HttpError):
        update_settings(mock_request(analyst_orguser), CopilotSettingsUpdate(enabled=True))
    with pytest.raises(HttpError):
        get_status(mock_request(analyst_orguser))


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


def test_sessions_are_owner_scoped_within_the_org(orguser, other_orguser, seed_db):
    created = create_session(mock_request(orguser))
    session_id = created["data"]["id"]

    # same org, different user: invisible and untouchable
    assert list_sessions(mock_request(other_orguser))["data"] == []
    with pytest.raises(HttpError):
        rename_session(mock_request(other_orguser), session_id, SessionRename(title="hijack"))
    with pytest.raises(HttpError):
        delete_session(mock_request(other_orguser), session_id)
