"""Service layer for Chat with Data sessions and status."""

from ddpui.auth import orguser_has_permission
from ddpui.core.ai.scopes.base import ScopeUnavailable
from ddpui.core.ai.scopes.resolver import resolve_scope
from ddpui.models.chat_with_data import ChatWithDataSession
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org import OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.schemas.chat_with_data_schemas import SessionCreate, StatusResponse
from ddpui.utils.feature_flags import is_feature_flag_enabled

CHAT_WITH_DATA_FLAG = "CHAT_WITH_DATA"

SCOPE_TYPES = ("org", "dashboard")


class SessionNotFound(Exception):
    """Session missing or not owned by the requesting user."""


class InvalidScope(Exception):
    """The requested session scope can't be created (bad type, missing id,
    unknown/empty dashboard, or no permission to view it). Maps to HTTP 400."""


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

    return StatusResponse(enabled=True, reason="ok")


def create_session(orguser: OrgUser, payload: SessionCreate | None = None) -> ChatWithDataSession:
    """Create a session, validating any requested scope up front so the user
    finds out at the button click, not on their first question."""
    scope_type = payload.scope_type if payload else "org"
    scope_id = payload.scope_id if payload else None

    if scope_type not in SCOPE_TYPES:
        raise InvalidScope(f"Unknown scope_type '{scope_type}'")

    if scope_type == "dashboard":
        if scope_id is None:
            raise InvalidScope("scope_id is required for a dashboard-scoped chat")
        if not orguser_has_permission(orguser, "can_view_dashboards"):
            raise InvalidScope("You don't have permission to view dashboards")
        try:
            resolve_scope(orguser.org, scope_type, scope_id)
        except ScopeUnavailable as err:
            raise InvalidScope(str(err)) from err

    return ChatWithDataSession.objects.create(
        org=orguser.org, orguser=orguser, scope_type=scope_type, scope_id=scope_id
    )


def _owned_live_sessions(orguser: OrgUser):
    """The one queryset every lookup builds on: the requesting user's own
    non-deleted sessions. Someone else's session id is indistinguishable from
    a missing one."""
    return ChatWithDataSession.objects.filter(
        org=orguser.org, orguser=orguser, deleted_at__isnull=True
    )


def list_sessions(orguser: OrgUser, scope_type: str | None = None) -> list[ChatWithDataSession]:
    """The requesting user's own live sessions, most recent activity first.
    scope_type filters (the main chat page passes "org" so dashboard-drawer
    sessions don't clutter its sidebar); None returns everything."""
    queryset = _owned_live_sessions(orguser)
    if scope_type is not None:
        queryset = queryset.filter(scope_type=scope_type)
    return list(queryset.order_by("-updated_at"))


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
