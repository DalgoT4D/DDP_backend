"""Session and message persistence helpers for dashboard chat."""

from dataclasses import dataclass
from uuid import UUID

from django.db import IntegrityError
from django.db import transaction
from django.db.models import Max
from django.utils import timezone

from ddpui.core.dashboard_chat.config import DashboardChatVectorStoreConfig
from ddpui.core.dashboard_chat.vector.vector_documents import build_dashboard_chat_collection_name
from ddpui.core.dashboard_chat.contracts import DashboardChatConversationMessage
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import (
    DashboardChatMessage,
    DashboardChatMessageRole,
    DashboardChatSession,
)
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("dashboard_chat")


class DashboardChatSessionError(Exception):
    """Raised when a dashboard chat session cannot be created or reused."""


@dataclass(frozen=True)
class DashboardChatMessageCreateResult:
    """Outcome of creating or reusing one persisted chat message."""

    message: DashboardChatMessage
    created: bool


def get_or_create_dashboard_chat_session(
    *,
    orguser: OrgUser,
    dashboard: Dashboard,
    session_id: str | None,
) -> DashboardChatSession:
    """Create a new session or validate an existing one for the current dashboard."""
    if session_id is None:
        if dashboard.org_id != orguser.org_id:
            raise DashboardChatSessionError(
                "Cannot create a chat session for a dashboard outside the current organization"
            )
        collection_name = None
        if orguser.org.dbt and orguser.org.dbt.vector_last_ingested_at is not None:
            vector_store_config = DashboardChatVectorStoreConfig.from_env()
            collection_name = build_dashboard_chat_collection_name(
                orguser.org.id,
                prefix=vector_store_config.collection_prefix,
                version=orguser.org.dbt.vector_last_ingested_at,
            )
        return DashboardChatSession.objects.create(
            org=orguser.org,
            orguser=orguser,
            dashboard=dashboard,
            vector_collection_name=collection_name,
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
    return create_dashboard_chat_user_message_with_status(
        session=session,
        content=content,
        client_message_id=client_message_id,
    ).message


def create_dashboard_chat_user_message_with_status(
    *,
    session: DashboardChatSession,
    content: str,
    client_message_id: str | None,
) -> DashboardChatMessageCreateResult:
    """Persist one user message and report whether a new row was created."""
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
    response_latency_ms: int | None = None,
    timing_breakdown: dict | None = None,
) -> DashboardChatMessage:
    """Persist one assistant message and advance the session timestamp."""
    return _create_dashboard_chat_message(
        session=session,
        role=DashboardChatMessageRole.ASSISTANT.value,
        content=content,
        client_message_id=None,
        payload=payload,
        response_latency_ms=response_latency_ms,
        timing_breakdown=timing_breakdown,
    ).message


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
        DashboardChatConversationMessage(
            role=message.role,
            content=message.content,
            payload=message.payload or {},
        )
        for message in query
    ]


def serialize_dashboard_chat_message(message: DashboardChatMessage) -> dict:
    """Return the websocket payload shape for one persisted chat message."""
    return {
        "id": str(message.id),
        "role": message.role,
        "content": message.content,
        "payload": message.payload or {},
        "response_latency_ms": message.response_latency_ms,
        "timing_breakdown": message.timing_breakdown or {},
        "created_at": message.created_at.isoformat(),
    }


def find_dashboard_chat_assistant_reply(
    *,
    session: DashboardChatSession,
    user_message: DashboardChatMessage,
) -> DashboardChatMessage | None:
    """Return the first assistant reply that follows a user turn, if it exists."""
    return (
        session.messages.filter(
            role=DashboardChatMessageRole.ASSISTANT.value,
            sequence_number__gt=user_message.sequence_number,
        )
        .order_by("sequence_number")
        .first()
    )


def execute_dashboard_chat_turn(session_id: str, user_message_id: int) -> DashboardChatMessage:
    """Load session and message, run the runtime, persist and return the assistant reply.

    Returns the assistant DashboardChatMessage on success.
    Raises Exception if the session or user message cannot be found.
    """
    from ddpui.core.dashboard_chat.orchestration.orchestrator import get_dashboard_chat_runtime

    session = (
        DashboardChatSession.objects.select_related("org", "dashboard", "orguser")
        .filter(session_id=session_id)
        .first()
    )
    if session is None or session.dashboard is None:
        raise Exception("Chat session could not be found")

    user_message = DashboardChatMessage.objects.filter(
        id=user_message_id,
        session=session,
        role="user",
    ).first()
    if user_message is None:
        raise Exception("Chat message could not be found")

    # Safety net: if an assistant reply already exists, return it without re-running.
    existing_assistant_message = find_dashboard_chat_assistant_reply(
        session=session,
        user_message=user_message,
    )
    if existing_assistant_message is not None:
        return existing_assistant_message

    response = get_dashboard_chat_runtime().run(
        org=session.org,
        dashboard_id=session.dashboard.id,
        user_query=user_message.content,
        session_id=str(session.session_id),
        vector_collection_name=session.vector_collection_name,
        conversation_history=list_dashboard_chat_history(
            session,
            exclude_message_id=user_message.id,
        ),
    )
    response_payload = response.to_dict()
    assistant_payload = {
        key: value for key, value in response_payload.items() if key != "answer_text"
    }
    timing_breakdown = dict(response_payload.get("metadata") or {}).get("timing_breakdown") or {}
    assistant_message = create_dashboard_chat_assistant_message(
        session=session,
        content=response.answer_text,
        payload=assistant_payload,
        timing_breakdown=timing_breakdown,
    )
    response_latency_ms = max(
        0,
        int((assistant_message.created_at - user_message.created_at).total_seconds() * 1000),
    )
    assistant_message.response_latency_ms = response_latency_ms
    assistant_message.save(update_fields=["response_latency_ms"])
    return assistant_message


def _create_dashboard_chat_message(
    *,
    session: DashboardChatSession,
    role: str,
    content: str,
    client_message_id: str | None,
    payload: dict | None,
    response_latency_ms: int | None = None,
    timing_breakdown: dict | None = None,
) -> DashboardChatMessageCreateResult:
    """Create a session-scoped chat message with a stable next sequence number."""
    created = False
    with transaction.atomic():
        locked_session = DashboardChatSession.objects.select_for_update().get(id=session.id)
        if client_message_id:
            existing_message = DashboardChatMessage.objects.filter(
                session=locked_session,
                client_message_id=client_message_id,
            ).first()
            if existing_message is not None:
                return DashboardChatMessageCreateResult(
                    message=existing_message,
                    created=False,
                )

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
                response_latency_ms=response_latency_ms,
                timing_breakdown=timing_breakdown,
            )
            created = True
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
    return DashboardChatMessageCreateResult(message=message, created=created)
