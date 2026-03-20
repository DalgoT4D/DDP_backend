"""Tests for dashboard chat session creation and reuse rules."""

import os

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User

from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.dashboard_chat.session_service import (
    DashboardChatSessionError,
    create_dashboard_chat_user_message,
    get_or_create_dashboard_chat_session,
)
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import DashboardChatMessage, DashboardChatSession
from ddpui.models.org import Org
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
