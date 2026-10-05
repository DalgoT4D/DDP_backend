"""Service layer for Chat with Data sessions and status."""

from ddpui.core.ai.agent.chat_data_agent import available_models, default_model_id
from ddpui.core.ai.constants import CHAT_WITH_DATA_FLAG
from ddpui.models.chat_with_data import ChatWithDataSession
from ddpui.models.org import OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.schemas.chat_with_data_schemas import StatusResponse
from ddpui.utils.feature_flags import is_feature_flag_enabled


class SessionNotFound(Exception):
    """Session missing, deleted, or in another org."""


def get_status(orguser: OrgUser) -> StatusResponse:
    """Is the chat usable for this org? Reports the first blocking reason —
    feature flag, then warehouse presence.

    Deliberately NOT gated on OrgPreferences.llm_optin: the admin flipping the
    Copilot toggle on the settings page IS the org's consent (the page says
    data is sent to an AI provider). llm_optin keeps gating the OTHER AI
    features (log summarization, AI data analysis), which have no toggle of
    their own — and webapp_v2 has no screen to set it."""
    org = orguser.org
    if not is_feature_flag_enabled(CHAT_WITH_DATA_FLAG, org):
        return StatusResponse(enabled=False, reason="feature_disabled")

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


def _org_live_sessions(orguser: OrgUser):
    """The one queryset every lookup builds on: the org's non-deleted sessions.
    Org-scoped, not user-scoped — chat is admin-only, and admins see every
    session in their org. Another org's session id looks missing."""
    return ChatWithDataSession.objects.filter(org=orguser.org, deleted_at__isnull=True)


def list_sessions(orguser: OrgUser) -> list[ChatWithDataSession]:
    """All of the org's live sessions, most recent activity first."""
    return list(_org_live_sessions(orguser).order_by("-updated_at"))


def get_session(orguser: OrgUser, session_id: int) -> ChatWithDataSession:
    """Org-scoped lookup."""
    session = _org_live_sessions(orguser).filter(id=session_id).first()
    if session is None:
        raise SessionNotFound(f"session {session_id} not found")
    return session


async def aget_session(orguser: OrgUser, session_id: int) -> ChatWithDataSession:
    """Async variant of get_session for async endpoints/consumers."""
    session = await _org_live_sessions(orguser).filter(id=session_id).afirst()
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
