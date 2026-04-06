"""Tests for dashboard chat session creation, reuse, and turn execution."""

from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest

from django.contrib.auth.models import User

from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.dashboard_chat.contracts.event_contracts import DashboardChatProgressStage
from ddpui.core.dashboard_chat.orchestration.runtime_signals import (
    DashboardChatRunCancelled,
    publish_runtime_progress,
)
from ddpui.core.dashboard_chat.sessions.session_service import (
    DashboardChatSessionError,
    create_dashboard_chat_user_message,
    create_dashboard_chat_user_message_with_status,
    execute_dashboard_chat_turn,
    get_or_create_dashboard_chat_session,
)
from ddpui.core.dashboard_chat.vector.vector_documents import build_dashboard_chat_collection_name
from ddpui.core.dashboard_chat.contracts import DashboardChatIntent, DashboardChatResponse
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


@patch("ddpui.core.dashboard_chat.orchestration.orchestrator.get_dashboard_chat_runtime")
def test_execute_dashboard_chat_turn_persists_assistant_message(get_runtime, session_owner, dashboard):
    """Successful turns should persist the assistant reply through the session service."""
    session = DashboardChatSession.objects.create(
        org=session_owner.org,
        orguser=session_owner,
        dashboard=dashboard,
    )
    user_message = DashboardChatMessage.objects.create(
        session=session,
        sequence_number=1,
        role="user",
        content="Why did funding drop?",
    )
    runtime = Mock()
    runtime.run.return_value = DashboardChatResponse(
        answer_text="Funding dropped because donor inflows slowed this quarter.",
        intent=DashboardChatIntent.QUERY_WITH_SQL,
        warnings=["Example warning"],
        sql="SELECT 1",
        sql_results=[{"value": 1}],
        metadata={
            "timing_breakdown": {
                "runtime_total_ms": 123.4,
                "graph_nodes_ms": {"load_context": 10.0},
            }
        },
    )
    get_runtime.return_value = runtime

    result = execute_dashboard_chat_turn(str(session.session_id), user_message.id)

    assistant_message = DashboardChatMessage.objects.get(session=session, role="assistant")
    assert assistant_message.sequence_number == 2
    assert assistant_message.content == "Funding dropped because donor inflows slowed this quarter."
    assert assistant_message.payload["sql"] == "SELECT 1"
    assert assistant_message.response_latency_ms is not None
    assert assistant_message.response_latency_ms >= 0
    assert assistant_message.timing_breakdown == {
        "runtime_total_ms": 123.4,
        "graph_nodes_ms": {"load_context": 10.0},
    }
    assert result.id == assistant_message.id


@patch("ddpui.core.dashboard_chat.orchestration.orchestrator.get_dashboard_chat_runtime")
def test_execute_dashboard_chat_turn_bubbles_runtime_errors(get_runtime, session_owner, dashboard):
    """Runtime failures should propagate without persisting an assistant reply."""
    session = DashboardChatSession.objects.create(
        org=session_owner.org,
        orguser=session_owner,
        dashboard=dashboard,
    )
    user_message = DashboardChatMessage.objects.create(
        session=session,
        sequence_number=1,
        role="user",
        content="Why did funding drop?",
    )
    runtime = Mock()
    runtime.run.side_effect = RuntimeError("boom")
    get_runtime.return_value = runtime

    with pytest.raises(RuntimeError, match="boom"):
        execute_dashboard_chat_turn(str(session.session_id), user_message.id)

    assert DashboardChatMessage.objects.filter(session=session, role="assistant").count() == 0


@patch("ddpui.core.dashboard_chat.orchestration.orchestrator.get_dashboard_chat_runtime")
def test_execute_dashboard_chat_turn_reuses_existing_assistant_reply(
    get_runtime,
    session_owner,
    dashboard,
):
    """Duplicate execution attempts should reuse the persisted assistant reply."""
    session = DashboardChatSession.objects.create(
        org=session_owner.org,
        orguser=session_owner,
        dashboard=dashboard,
    )
    user_message = DashboardChatMessage.objects.create(
        session=session,
        sequence_number=1,
        role="user",
        content="Why did funding drop?",
    )
    assistant_message = DashboardChatMessage.objects.create(
        session=session,
        sequence_number=2,
        role="assistant",
        content="Existing answer",
        payload={"intent": "query_without_sql"},
    )

    result = execute_dashboard_chat_turn(str(session.session_id), user_message.id)

    assert result.id == assistant_message.id
    get_runtime.assert_not_called()


@patch("ddpui.core.dashboard_chat.orchestration.orchestrator.get_dashboard_chat_runtime")
def test_execute_dashboard_chat_turn_forwards_progress_updates(
    get_runtime,
    session_owner,
    dashboard,
):
    """Runtime progress hooks should reach the session-owned progress publisher."""
    session = DashboardChatSession.objects.create(
        org=session_owner.org,
        orguser=session_owner,
        dashboard=dashboard,
    )
    user_message = DashboardChatMessage.objects.create(
        session=session,
        sequence_number=1,
        role="user",
        content="Why did funding drop?",
    )
    progress_publisher = Mock()

    runtime = Mock()

    def run_with_progress(**kwargs):
        publish_runtime_progress(
            "Loading dashboard context",
            DashboardChatProgressStage.LOADING_CONTEXT,
        )
        return DashboardChatResponse(
            answer_text="Funding dropped because donor inflows slowed this quarter.",
            intent=DashboardChatIntent.QUERY_WITH_SQL,
        )

    runtime.run.side_effect = run_with_progress
    get_runtime.return_value = runtime

    execute_dashboard_chat_turn(
        str(session.session_id),
        user_message.id,
        progress_publisher=progress_publisher,
    )

    progress_publisher.assert_called_once_with(
        "Loading dashboard context",
        DashboardChatProgressStage.LOADING_CONTEXT,
    )


@patch("ddpui.core.dashboard_chat.orchestration.orchestrator.get_dashboard_chat_runtime")
def test_execute_dashboard_chat_turn_stops_before_persisting_when_cancelled(
    get_runtime,
    session_owner,
    dashboard,
):
    """A cancelled turn should not persist a new assistant reply."""
    session = DashboardChatSession.objects.create(
        org=session_owner.org,
        orguser=session_owner,
        dashboard=dashboard,
    )
    user_message = DashboardChatMessage.objects.create(
        session=session,
        sequence_number=1,
        role="user",
        content="Why did funding drop?",
    )
    runtime = Mock()
    runtime.run.return_value = DashboardChatResponse(
        answer_text="Funding dropped because donor inflows slowed this quarter.",
        intent=DashboardChatIntent.QUERY_WITH_SQL,
    )
    get_runtime.return_value = runtime

    with pytest.raises(DashboardChatRunCancelled):
        execute_dashboard_chat_turn(
            str(session.session_id),
            user_message.id,
            cancel_checker=lambda: True,
        )

    assert DashboardChatMessage.objects.filter(session=session, role="assistant").count() == 0
