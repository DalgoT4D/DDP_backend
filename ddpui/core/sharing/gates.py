"""Reusable HTTP gates for by-id endpoints: view/edit/owner checks and the
dynamic share-permission check for routes generic over ``{rtype}``.

This module never decides access, it only raises —
``access_resolver.effective_permission`` stays the single source of truth.
"""

from ninja.errors import HttpError

from ddpui.auth import UNAUTHORIZED
from ddpui.core.ownership import can_delete_resource
from ddpui.core.sharing.access_resolver import effective_permission
from ddpui.core.sharing.shareable_types import get_resource_type

# Matches the exact wording of each detail GET's 403; "kpi" -> "KPI" is
# deliberate casing.
_NOUN_BY_RTYPE = {
    "chart": "chart",
    "dashboard": "dashboard",
    "report": "report",
    "alert": "alert",
    "metric": "metric",
    "kpi": "KPI",
}


def require_view_access(viewer, rtype: str, resource) -> None:
    """Raise ``HttpError(403)`` unless ``viewer`` has at least view access
    to ``resource``."""
    if effective_permission(viewer, rtype, resource) is None:
        noun = _NOUN_BY_RTYPE.get(rtype, rtype)
        raise HttpError(403, f"You do not have access to this {noun}")


def require_edit_access(viewer, rtype: str, resource) -> None:
    """Raise ``HttpError(403)`` unless ``viewer`` resolves to edit on ``resource``."""
    if effective_permission(viewer, rtype, resource) != "edit":
        noun = _NOUN_BY_RTYPE.get(rtype, rtype)
        raise HttpError(403, f"You do not have edit access to this {noun}")


def require_owner_access(viewer, rtype: str, resource) -> None:
    """Raise ``HttpError(403)`` unless ``viewer`` is the current owner or an
    admin. Stricter than ``require_edit_access``: grant-derived "edit" never
    satisfies this."""
    if not can_delete_resource(viewer, resource):
        noun = _NOUN_BY_RTYPE.get(rtype, rtype)
        raise HttpError(403, f"You do not have owner access to this {noun}")


def require_share_permission(request, rtype: str) -> None:
    """Dynamic ``@has_permission`` for routes generic over ``{rtype}``: checks
    the registry's ``share_permission_slug`` against ``request.permissions``.
    Any failure raises the decorator's exact ``HttpError(404, "unauthorized")``."""
    entry = get_resource_type(rtype)
    slug = entry.share_permission_slug if entry else None
    if slug is None or not request.permissions or not set(request.permissions).issuperset({slug}):
        raise HttpError(404, UNAUTHORIZED)
