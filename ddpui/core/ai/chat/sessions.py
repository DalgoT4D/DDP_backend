"""Service layer for Chat with Data sessions and status."""

from ddpui.core.ai.agent.chat_data_agent import available_models, default_model_id
from ddpui.models.chat_with_data import ChatWithDataSession
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org import OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.schemas.chat_with_data_schemas import StatusResponse
from ddpui.utils.feature_flags import is_feature_flag_enabled

CHAT_WITH_DATA_FLAG = "CHAT_WITH_DATA"


class SessionNotFound(Exception):
    """Session missing or not owned by the requesting user."""


def get_status(orguser: OrgUser) -> StatusResponse:
    """Is the chat usable for this org? Reports the first blocking reason —
    feature flag, then AI consent (llm_optin), then warehouse presence."""
    org = orguser.org
    if not is_feature_flag_enabled(CHAT_WITH_DATA_FLAG, org):
        return StatusResponse(enabled=False, reason="feature_disabled")

    preferences = OrgPreferences.objects.filter(org=org).first()
    if preferences is None or not preferences.llm_optin:
        return StatusResponse(enabled=False, reason="llm_consent_required")

    if not OrgWarehouse.objects.filter(org=org).exists():
        return StatusResponse(enabled=False, reason="no_warehouse")

    return StatusResponse(
        enabled=True,
        reason="ok",
        models=available_models(),
        default_model=default_model_id(),
    )


def create_session(orguser: OrgUser) -> ChatWithDataSession:
    """Create a new chat session for this user."""
    return ChatWithDataSession.objects.create(org=orguser.org, orguser=orguser)


def _owned_live_sessions(orguser: OrgUser):
    """The one queryset every lookup builds on: the requesting user's own
    non-deleted sessions. Someone else's session id is indistinguishable from
    a missing one."""
    return ChatWithDataSession.objects.filter(
        org=orguser.org, orguser=orguser, deleted_at__isnull=True
    )


def list_sessions(orguser: OrgUser) -> list[ChatWithDataSession]:
    """The requesting user's own live sessions, most recent activity first."""
    return list(_owned_live_sessions(orguser).order_by("-updated_at"))


def get_session(orguser: OrgUser, session_id: int) -> ChatWithDataSession:
    """Owner-scoped lookup."""
    session = _owned_live_sessions(orguser).filter(id=session_id).first()
    if session is None:
        raise SessionNotFound(f"session {session_id} not found")
    return session


async def aget_session(orguser: OrgUser, session_id: int) -> ChatWithDataSession:
    """Async variant of get_session for async endpoints/consumers."""
    session = await _owned_live_sessions(orguser).filter(id=session_id).afirst()
    if session is None:
        raise SessionNotFound(f"session {session_id} not found")
    return session


def rename_session(orguser: OrgUser, session_id: int, title: str) -> ChatWithDataSession:
    session = get_session(orguser, session_id)
    session.title = title.strip()[:255]
    session.save(update_fields=["title", "updated_at"])
    return session


def delete_session(orguser: OrgUser, session_id: int) -> None:
    session = get_session(orguser, session_id)
    session.soft_delete()
