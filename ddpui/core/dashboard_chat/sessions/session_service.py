"""Session and message persistence helpers for dashboard chat."""

from dataclasses import dataclass
from threading import Thread
from uuid import UUID

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.db import close_old_connections
from django.db import IntegrityError
from django.db import transaction
from django.db.models import Max
from django.utils import timezone

from ddpui.core.dashboard_chat.config import DashboardChatVectorStoreConfig
from ddpui.core.dashboard_chat.vector.vector_documents import build_dashboard_chat_collection_name
from ddpui.core.dashboard_chat.contracts.conversation_contracts import (
    DashboardChatConversationMessage,
)
from ddpui.core.dashboard_chat.contracts.event_contracts import (
    DashboardChatAssistantMessageEvent,
    DashboardChatCancelledEvent,
    DashboardChatProgressEvent,
    DashboardChatProgressStage,
)
from ddpui.core.dashboard_chat.orchestration.runtime_signals import (
    dashboard_chat_runtime_hooks,
    DashboardChatRunCancelled,
)
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import (
    DashboardChatMessage,
    DashboardChatMessageRole,
    DashboardChatSession,
    DashboardChatTurn,
    DashboardChatTurnStatus,
)
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("dashboard_chat")
DASHBOARD_CHAT_SESSION_GROUP_PREFIX = "dashboard_chat_session_"


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


def update_dashboard_chat_turn(
    turn_id: int,
    *,
    status: DashboardChatTurnStatus | None = None,
    progress_label: str | None = None,
    assistant_message: DashboardChatMessage | None = None,
    error_message: str | None = None,
    started_at=None,
    completed_at=None,
    cancel_requested_at=None,
) -> DashboardChatTurn:
    """Persist one dashboard-chat turn status transition and return the refreshed row."""

    update_fields: dict = {}
    if status is not None:
        update_fields["status"] = status
    if progress_label is not None:
        update_fields["progress_label"] = progress_label
    if assistant_message is not None:
        update_fields["assistant_message"] = assistant_message
    if error_message is not None:
        update_fields["error_message"] = error_message
    if started_at is not None:
        update_fields["started_at"] = started_at
    if completed_at is not None:
        update_fields["completed_at"] = completed_at
    if cancel_requested_at is not None:
        update_fields["cancel_requested_at"] = cancel_requested_at
    if update_fields:
        DashboardChatTurn.objects.filter(id=turn_id).update(**update_fields)
    return DashboardChatTurn.objects.select_related("session", "user_message").get(id=turn_id)


def publish_dashboard_chat_progress(
    *,
    session: DashboardChatSession,
    turn: DashboardChatTurn,
    label: str,
    stage: DashboardChatProgressStage | None,
    message_id: int | None = None,
) -> None:
    """Publish one progress update to all websocket listeners for the current session."""

    event = DashboardChatProgressEvent(
        session_id=str(session.session_id),
        turn_id=str(turn.id),
        dashboard_id=session.dashboard_id or 0,
        occurred_at=timezone.now(),
        label=label,
        stage=stage,
        message_id=str(message_id) if message_id is not None else None,
    )
    _publish_dashboard_chat_event(
        session_id=str(session.session_id),
        status="success",
        message="",
        data=event.model_dump(mode="json"),
    )


def publish_dashboard_chat_cancelled(
    *,
    session: DashboardChatSession,
    turn: DashboardChatTurn,
    label: str = "Generation stopped",
) -> None:
    """Publish a cancelled event to the active dashboard-chat websocket session."""

    event = DashboardChatCancelledEvent(
        session_id=str(session.session_id),
        turn_id=str(turn.id),
        dashboard_id=session.dashboard_id or 0,
        occurred_at=timezone.now(),
        label=label,
    )
    _publish_dashboard_chat_event(
        session_id=str(session.session_id),
        status="success",
        message="",
        data=event.model_dump(mode="json"),
    )


def publish_dashboard_chat_assistant_message(
    *,
    session: DashboardChatSession,
    turn: DashboardChatTurn,
    message: DashboardChatMessage,
) -> None:
    """Publish the completed assistant reply for one dashboard-chat turn."""

    event = DashboardChatAssistantMessageEvent(
        session_id=str(session.session_id),
        turn_id=str(turn.id),
        message_id=str(message.id),
        dashboard_id=session.dashboard_id or 0,
        occurred_at=timezone.now(),
        id=str(message.id),
        role="assistant",
        content=message.content,
        created_at=message.created_at,
        payload=message.payload or {},
        response_latency_ms=message.response_latency_ms,
        timing_breakdown=message.timing_breakdown or {},
    )
    _publish_dashboard_chat_event(
        session_id=str(session.session_id),
        status="success",
        message="",
        data=event.model_dump(mode="json"),
    )


def publish_dashboard_chat_error(
    *,
    session: DashboardChatSession,
    message: str,
) -> None:
    """Publish one terminal error envelope to the current dashboard-chat websocket session."""

    _publish_dashboard_chat_event(
        session_id=str(session.session_id),
        status="error",
        message=message,
        data={},
    )


def start_dashboard_chat_turn_background(turn_id: int) -> None:
    """Run one dashboard-chat turn in a background thread without blocking the websocket."""

    Thread(
        target=_run_dashboard_chat_turn_in_background,
        kwargs={"turn_id": turn_id},
        daemon=True,
        name=f"dashboard-chat-turn-{turn_id}",
    ).start()


def _run_dashboard_chat_turn_in_background(turn_id: int) -> None:
    """Own execution, progress, and cancellation for one dashboard-chat turn."""

    close_old_connections()
    try:
        turn = (
            DashboardChatTurn.objects.select_related(
                "session",
                "session__dashboard",
                "session__org",
                "session__orguser",
                "user_message",
            )
            .filter(id=turn_id)
            .first()
        )
        if turn is None or turn.session is None or turn.user_message is None:
            logger.warning("dashboard chat turn %s not found", turn_id)
            return

        if turn.status in {
            DashboardChatTurnStatus.CANCEL_REQUESTED,
            DashboardChatTurnStatus.CANCELLED,
        }:
            cancelled_turn = update_dashboard_chat_turn(
                turn.id,
                status=DashboardChatTurnStatus.CANCELLED,
                progress_label="Generation stopped",
                completed_at=timezone.now(),
            )
            publish_dashboard_chat_cancelled(
                session=cancelled_turn.session,
                turn=cancelled_turn,
            )
            return

        running_turn = update_dashboard_chat_turn(
            turn.id,
            status=DashboardChatTurnStatus.RUNNING,
            progress_label="Understanding question",
            started_at=timezone.now(),
            error_message="",
        )

        def progress_publisher(
            label: str,
            stage: DashboardChatProgressStage | None,
        ) -> None:
            refreshed_turn = update_dashboard_chat_turn(
                running_turn.id,
                progress_label=label,
            )
            publish_dashboard_chat_progress(
                session=refreshed_turn.session,
                turn=refreshed_turn,
                label=label,
                stage=stage,
                message_id=refreshed_turn.user_message_id,
            )

        def cancel_checker() -> bool:
            status = (
                DashboardChatTurn.objects.filter(id=running_turn.id)
                .values_list("status", flat=True)
                .first()
            )
            return status in {
                DashboardChatTurnStatus.CANCEL_REQUESTED,
                DashboardChatTurnStatus.CANCELLED,
            }

        try:
            if cancel_checker():
                cancelled_turn = update_dashboard_chat_turn(
                    running_turn.id,
                    status=DashboardChatTurnStatus.CANCELLED,
                    progress_label="Generation stopped",
                    completed_at=timezone.now(),
                )
                publish_dashboard_chat_cancelled(
                    session=cancelled_turn.session,
                    turn=cancelled_turn,
                )
                return

            assistant_message = execute_dashboard_chat_turn(
                str(running_turn.session.session_id),
                running_turn.user_message_id,
                progress_publisher=progress_publisher,
                cancel_checker=cancel_checker,
            )
        except DashboardChatRunCancelled:
            cancelled_turn = update_dashboard_chat_turn(
                running_turn.id,
                status=DashboardChatTurnStatus.CANCELLED,
                progress_label="Generation stopped",
                completed_at=timezone.now(),
            )
            publish_dashboard_chat_cancelled(
                session=cancelled_turn.session,
                turn=cancelled_turn,
            )
            return
        except Exception:
            logger.exception("dashboard chat turn %s failed", turn_id)
            failed_turn = update_dashboard_chat_turn(
                running_turn.id,
                status=DashboardChatTurnStatus.FAILED,
                progress_label="",
                error_message="Something went wrong while generating the response",
                completed_at=timezone.now(),
            )
            publish_dashboard_chat_error(
                session=failed_turn.session,
                message="Something went wrong while generating the response",
            )
            return

        completed_turn = update_dashboard_chat_turn(
            running_turn.id,
            status=DashboardChatTurnStatus.COMPLETED,
            progress_label="",
            assistant_message=assistant_message,
            completed_at=timezone.now(),
        )
        publish_dashboard_chat_assistant_message(
            session=completed_turn.session,
            turn=completed_turn,
            message=assistant_message,
        )
    finally:
        close_old_connections()


def execute_dashboard_chat_turn(
    session_id: str,
    user_message_id: int,
    *,
    progress_publisher=None,
    cancel_checker=None,
) -> DashboardChatMessage:
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
    existing_assistant_message = (
        session.messages.filter(
            role=DashboardChatMessageRole.ASSISTANT.value,
            sequence_number__gt=user_message.sequence_number,
        )
        .order_by("sequence_number")
        .first()
    )
    if existing_assistant_message is not None:
        return existing_assistant_message

    with dashboard_chat_runtime_hooks(
        progress_publisher=progress_publisher,
        cancel_checker=cancel_checker,
    ):
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
        if cancel_checker is not None and cancel_checker():
            raise DashboardChatRunCancelled()
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


def _publish_dashboard_chat_event(
    *,
    session_id: str,
    status: str,
    message: str,
    data: dict,
) -> None:
    """Send one event envelope to all websocket listeners for one chat session."""

    channel_layer = get_channel_layer()
    if channel_layer is None:
        return
    async_to_sync(channel_layer.group_send)(
        f"{DASHBOARD_CHAT_SESSION_GROUP_PREFIX}{session_id}",
        {
            "type": "dashboard_chat_event",
            "status": status,
            "message": message,
            "data": data,
        },
    )


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
