"""Tests for dashboard chat session creation and reuse rules."""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from django.contrib.auth.models import User

from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.dashboard_chat.sessions.service import (
    DashboardChatSessionError,
    create_dashboard_chat_user_message,
    create_dashboard_chat_user_message_with_status,
    get_or_create_dashboard_chat_session,
)
from ddpui.core.dashboard_chat.vector.documents import build_dashboard_chat_collection_name
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import DashboardChatMessage, DashboardChatSession
from ddpui.models.org import Org, OrgDbt
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


@pytest.fixture
def org():
    organization = Org.objects.create(
        name="Dashboard Chat Org",
        slug="dashchat-service",
        airbyte_workspace_id="workspace-1",
    )
    yield organization
    organization.delete()


@pytest.fixture
def dashboard(org, seed_db):
    owner = OrgUser.objects.create(
        user=User.objects.create(
            username="dashchat-owner",
            email="dashchat-owner@test.com",
            password="testpassword",
        ),
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    dashboard_instance = Dashboard.objects.create(
        title="Impact Overview",
        dashboard_type="native",
        created_by=owner,
        last_modified_by=owner,
        org=org,
    )
    yield dashboard_instance
    dashboard_instance.delete()
    owner.delete()
    owner.user.delete()


@pytest.fixture
def session_owner(org, seed_db):
    user = User.objects.create(
        username="dashchat-session-owner",
        email="dashchat-session-owner@test.com",
        password="testpassword",
    )
    orguser = OrgUser.objects.create(
        user=user,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()
    user.delete()


@pytest.fixture
def other_orguser(org, seed_db):
    user = User.objects.create(
        username="dashchat-other-user",
        email="dashchat-other-user@test.com",
        password="testpassword",
    )
    orguser = OrgUser.objects.create(
        user=user,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()
    user.delete()


@pytest.fixture
def other_org(seed_db):
    organization = Org.objects.create(
        name="Other Dashboard Chat Org",
        slug="other-dashchat",
        airbyte_workspace_id="workspace-2",
    )
    yield organization
    organization.delete()


@pytest.fixture
def other_org_dashboard(other_org, seed_db):
    owner = OrgUser.objects.create(
        user=User.objects.create(
            username="other-dashchat-owner",
            email="other-dashchat-owner@test.com",
            password="testpassword",
        ),
        org=other_org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    dashboard_instance = Dashboard.objects.create(
        title="Other Impact Overview",
        dashboard_type="native",
        created_by=owner,
        last_modified_by=owner,
        org=other_org,
    )
    yield dashboard_instance
    dashboard_instance.delete()
    owner.delete()
    owner.user.delete()


def test_get_or_create_dashboard_chat_session_creates_new_session(session_owner, dashboard):
    """A missing session_id should create a new session for the current user."""
    session = get_or_create_dashboard_chat_session(
        orguser=session_owner,
        dashboard=dashboard,
        session_id=None,
    )

    assert isinstance(session, DashboardChatSession)
    assert session.orguser == session_owner
    assert session.dashboard == dashboard
    assert session.vector_collection_name is None


def test_get_or_create_dashboard_chat_session_rejects_cross_org_dashboard_on_create(
    session_owner,
    other_org_dashboard,
):
    """New chat sessions must not be created against a dashboard from another org."""
    with pytest.raises(
        DashboardChatSessionError,
        match="outside the current organization",
    ):
        get_or_create_dashboard_chat_session(
            orguser=session_owner,
            dashboard=other_org_dashboard,
            session_id=None,
        )


def test_get_or_create_dashboard_chat_session_pins_active_vector_collection(
    session_owner,
    dashboard,
):
    """New chat sessions should pin the active org vector collection at creation time."""
    org_dbt = OrgDbt.objects.create(
        project_dir="client_dbt/dashchat",
        target_type="postgres",
        default_schema="analytics",
        vector_last_ingested_at=datetime(2026, 3, 23, 12, 0, tzinfo=timezone.utc),
    )
    session_owner.org.dbt = org_dbt
    session_owner.org.save(update_fields=["dbt"])

    with patch.dict(
        "os.environ",
        {"AI_DASHBOARD_CHAT_CHROMA_COLLECTION_PREFIX": "tenant_"},
        clear=False,
    ):
        session = get_or_create_dashboard_chat_session(
            orguser=session_owner,
            dashboard=dashboard,
            session_id=None,
        )

    assert session.vector_collection_name == build_dashboard_chat_collection_name(
        session_owner.org.id,
        prefix="tenant_",
        version=org_dbt.vector_last_ingested_at,
    )


def test_get_or_create_dashboard_chat_session_rejects_other_user_session(
    session_owner,
    other_orguser,
    dashboard,
):
    """Session reuse is limited to the user who created the conversation."""
    session = DashboardChatSession.objects.create(
        org=session_owner.org,
        orguser=session_owner,
        dashboard=dashboard,
    )

    with pytest.raises(
        DashboardChatSessionError, match="Chat session not found for this dashboard"
    ):
        get_or_create_dashboard_chat_session(
            orguser=other_orguser,
            dashboard=dashboard,
            session_id=str(session.session_id),
        )


def test_create_dashboard_chat_user_message_is_idempotent_for_client_message_id(
    session_owner,
    dashboard,
):
    """Retries with the same client message id should reuse the stored message."""
    session = DashboardChatSession.objects.create(
        org=session_owner.org,
        orguser=session_owner,
        dashboard=dashboard,
    )

    first_message = create_dashboard_chat_user_message(
        session=session,
        content="Why did funding drop?",
        client_message_id="client-1",
    )
    second_message = create_dashboard_chat_user_message(
        session=session,
        content="Why did funding drop?",
        client_message_id="client-1",
    )

    assert first_message.id == second_message.id
    assert first_message.sequence_number == 1
    assert DashboardChatMessage.objects.filter(session=session).count() == 1


def test_create_dashboard_chat_user_message_with_status_marks_reused_message(
    session_owner,
    dashboard,
):
    """The duplicate-detection path must report that the second write reused the row."""
    session = DashboardChatSession.objects.create(
        org=session_owner.org,
        orguser=session_owner,
        dashboard=dashboard,
    )

    first_result = create_dashboard_chat_user_message_with_status(
        session=session,
        content="Why did funding drop?",
        client_message_id="client-1",
    )
    second_result = create_dashboard_chat_user_message_with_status(
        session=session,
        content="Why did funding drop?",
        client_message_id="client-1",
    )

    assert first_result.created is True
    assert second_result.created is False
    assert first_result.message.id == second_result.message.id
