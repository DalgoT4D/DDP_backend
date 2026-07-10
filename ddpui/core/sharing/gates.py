"""The doorman: a re-usable 403 gate for content sub-endpoints (Task 3b).

Task 3 inlined ``if effective_permission(...) is None: raise HttpError(403,
"You do not have access to this <noun>")`` at the 5 single-resource detail
GETs. Task 3b repeats that exact check across every other by-id
sub-endpoint that serves a resource's content (chart/KPI data, previews,
consumers, logs, notes, filters, ...) so the check is pulled into one
function here instead of re-typing (and risking re-wording) it a dozen
times across 5 files.

Task 5 adds the two mutation-side gates:

- ``require_edit_access`` — resolver-edit object check for update endpoints
  and the ``/api/access/*`` mutation routes.
- ``require_share_permission`` — the dynamic mirror of ``@has_permission``
  for routes generic over ``{rtype}``: a static decorator literal cannot
  vary per rtype, so the slug is read from the registry entry
  (``share_permission_slug``) and checked against ``request.permissions``
  (the same Redis-backed set the decorator reads) with identical observable
  semantics — a missing slug raises ``HttpError(404, "unauthorized")``,
  exactly what ``@has_permission`` raises.

This module is NOT the resolver — it never decides access, it only raises.
``access_resolver.effective_permission`` remains the single source of truth
for the decision.
"""

from ninja.errors import HttpError

from ddpui.auth import UNAUTHORIZED
from ddpui.core.sharing.access_resolver import effective_permission
from ddpui.core.sharing.shareable_types import get_resource_type

# Matches the exact wording Task 3 used at each detail GET. "kpi" -> "KPI"
# is deliberate: that's the casing in kpi_api.get_kpi's message.
_NOUN_BY_RTYPE = {
    "dashboard": "dashboard",
    "report": "report",
    "alert": "alert",
    "metric": "metric",
    "kpi": "KPI",
}


def require_view_access(viewer, rtype: str, resource) -> None:
    """Raise ``HttpError(403)`` unless ``viewer`` has at least view access
    to ``resource``. No-op (returns None) for admins, owners, and anyone
    else ``effective_permission`` admits — mirrors the inline gate Task 3
    added to the 5 detail GETs.
    """
    if effective_permission(viewer, rtype, resource) is None:
        noun = _NOUN_BY_RTYPE.get(rtype, rtype)
        raise HttpError(403, f"You do not have access to this {noun}")


def require_edit_access(viewer, rtype: str, resource) -> None:
    """Raise ``HttpError(403)`` unless ``viewer`` resolves to **edit** on
    ``resource``. Used by resource update endpoints (after their slug gate)
    and by the ``/api/access/*`` mutation routes."""
    if effective_permission(viewer, rtype, resource) != "edit":
        noun = _NOUN_BY_RTYPE.get(rtype, rtype)
        raise HttpError(403, f"You do not have edit access to this {noun}")


def require_share_permission(request, rtype: str) -> None:
    """Dynamic ``@has_permission([...])`` for routes generic over ``{rtype}``:
    checks the rtype's ``share_permission_slug`` (registry data) against
    ``request.permissions``. Mirrors the decorator's observable behavior —
    any failure raises ``HttpError(404, "unauthorized")``."""
    entry = get_resource_type(rtype)
    slug = entry.share_permission_slug if entry else None
    if slug is None or not request.permissions or not set(request.permissions).issuperset({slug}):
        raise HttpError(404, UNAUTHORIZED)
