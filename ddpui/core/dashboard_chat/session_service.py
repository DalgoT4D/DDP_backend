"""Session and message persistence helpers for dashboard chat."""

from uuid import UUID

from django.db import IntegrityError
from django.db import transaction
from django.db.models import Max
from django.utils import timezone

from ddpui.core.dashboard_chat.runtime_types import DashboardChatConversationMessage
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import (
    DashboardChatMessage,
    DashboardChatMessageRole,
    DashboardChatSession,
)
from ddpui.models.org_user import OrgUser


class DashboardChatSessionError(Exception):
    """Raised when a dashboard chat session cannot be created or reused."""


def get_or_create_dashboard_chat_session(
    *,
    orguser: OrgUser,
    dashboard: Dashboard,
    session_id: str | None,
) -> DashboardChatSession:
    """Create a new session or validate an existing one for the current dashboard."""
    if session_id is None:
        return DashboardChatSession.objects.create(
            org=orguser.org,
            orguser=orguser,
            dashboard=dashboard,
        )

    try:
        session_uuid = UUID(str(session_id))
    except ValueError as error:
        raise DashboardChatSessionError("Invalid session_id") from error

    session = DashboardChatSession.objects.filter(
        session_id=session_uuid,
        org=orguser.org,
        orguser=orguser,
        dashboard=dashboard,
    ).first()
    if session is None:
        raise DashboardChatSessionError("Chat session not found for this dashboard")
    return session


def create_dashboard_chat_user_message(
    *,
    session: DashboardChatSession,
    content: str,
    client_message_id: str | None,
) -> DashboardChatMessage:
    """Persist one user message and advance the session timestamp."""
    return _create_dashboard_chat_message(
        session=session,
        role=DashboardChatMessageRole.USER.value,
        content=content,
        client_message_id=client_message_id,
        payload=None,
    )


def create_dashboard_chat_assistant_message(
    *,
    session: DashboardChatSession,
    content: str,
    payload: dict | None,
) -> DashboardChatMessage:
    """Persist one assistant message and advance the session timestamp."""
    return _create_dashboard_chat_message(
        session=session,
        role=DashboardChatMessageRole.ASSISTANT.value,
        content=content,
        client_message_id=None,
        payload=payload,
    )


def list_dashboard_chat_history(
    session: DashboardChatSession,
    *,
    exclude_message_id: int | None = None,
) -> list[DashboardChatConversationMessage]:
    """Return prior session messages in the format expected by the runtime."""
    query = session.messages.order_by("sequence_number")
    if exclude_message_id is not None:
        query = query.exclude(id=exclude_message_id)
    return [
        DashboardChatConversationMessage(role=message.role, content=message.content)
        for message in query
    ]


def serialize_dashboard_chat_message(message: DashboardChatMessage) -> dict:
    """Return the websocket payload shape for one persisted chat message."""
    return {
        "id": str(message.id),
        "role": message.role,
        "content": message.content,
        "payload": message.payload or {},
        "created_at": message.created_at.isoformat(),
    }


def _create_dashboard_chat_message(
    *,
    session: DashboardChatSession,
    role: str,
    content: str,
    client_message_id: str | None,
    payload: dict | None,
) -> DashboardChatMessage:
    """Create a session-scoped chat message with a stable next sequence number."""
    with transaction.atomic():
        locked_session = DashboardChatSession.objects.select_for_update().get(id=session.id)
        if client_message_id:
            existing_message = DashboardChatMessage.objects.filter(
                session=locked_session,
                client_message_id=client_message_id,
            ).first()
            if existing_message is not None:
                return existing_message

        next_sequence_number = (
            locked_session.messages.aggregate(max_sequence_number=Max("sequence_number"))[
                "max_sequence_number"
            ]
            or 0
        ) + 1
        try:
            message = DashboardChatMessage.objects.create(
                session=locked_session,
                sequence_number=next_sequence_number,
                role=role,
                content=content,
                client_message_id=client_message_id,
                payload=payload,
            )
        except IntegrityError:
            if not client_message_id:
                raise
            message = DashboardChatMessage.objects.filter(
                session=locked_session,
                client_message_id=client_message_id,
            ).first()
            if message is None:
                raise
        DashboardChatSession.objects.filter(id=locked_session.id).update(updated_at=timezone.now())
    return message
