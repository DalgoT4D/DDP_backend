"""Chat with Data REST endpoints: availability status, session CRUD, history.

The chat turns themselves run over WebSocket (ddpui/websockets/
chat_with_data_consumer.py); these endpoints manage everything around them.
"""

from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.core.ai.chat import sessions as service
from ddpui.core.ai.chat import history
from ddpui.core.ai.chat.sessions import SessionNotFound
from ddpui.models.org_user import OrgUser
from ddpui.schemas.chat_with_data_schemas import SessionOut, SessionRename
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.response_wrapper import api_response

logger = CustomLogger("ddpui")

chat_with_data_router = Router()


@chat_with_data_router.get("/status")
@has_permission(["can_use_chat_with_data"])
def get_status(request):
    """Whether chat is available for this org, and the blocking reason if not."""
    orguser: OrgUser = request.orguser
    return api_response(success=True, data=service.get_status(orguser))


@chat_with_data_router.post("/sessions/")
@has_permission(["can_use_chat_with_data"])
def create_session(request):
    """Start a new chat session."""
    orguser: OrgUser = request.orguser
    session = service.create_session(orguser)
    return api_response(success=True, data=SessionOut.from_model(session))


@chat_with_data_router.get("/sessions/")
@has_permission(["can_use_chat_with_data"])
def list_sessions(request):
    """The requesting user's sessions, most recent first."""
    orguser: OrgUser = request.orguser
    sessions = service.list_sessions(orguser)
    return api_response(
        success=True,
        data=[SessionOut.from_model(session).model_dump() for session in sessions],
    )


@chat_with_data_router.put("/sessions/{session_id}")
@has_permission(["can_use_chat_with_data"])
def rename_session(request, session_id: int, payload: SessionRename):
    """Rename a session (owner only)."""
    orguser: OrgUser = request.orguser
    try:
        session = service.rename_session(orguser, session_id, payload.title)
    except SessionNotFound as err:
        raise HttpError(404, "session not found") from err
    return api_response(success=True, data=SessionOut.from_model(session))


@chat_with_data_router.delete("/sessions/{session_id}")
@has_permission(["can_use_chat_with_data"])
def delete_session(request, session_id: int):
    """Soft-delete a session (owner only)."""
    orguser: OrgUser = request.orguser
    try:
        service.delete_session(orguser, session_id)
    except SessionNotFound as err:
        raise HttpError(404, "session not found") from err
    return api_response(success=True, message="session deleted")


@chat_with_data_router.get("/sessions/{session_id}/messages")
@has_permission(["can_use_chat_with_data"])
async def get_session_messages(request, session_id: int):
    """Conversation history, replayed from the LangGraph checkpointer."""
    orguser: OrgUser = request.orguser
    try:
        session = await service.aget_session(orguser, session_id)
    except SessionNotFound as err:
        raise HttpError(404, "session not found") from err

    messages = await history.read_thread_messages(str(session.thread_id))
    return api_response(success=True, data=[message.model_dump() for message in messages])
