"""v1.2 decorator gates: resource extraction + the flat permission pool.

Stack (top = runs first), per plan v1.2 §1:

    @has_permission(["can_view_dashboards"])            # ① role slugs — untouched
    @extract_resource("dashboard")                      # ② fetch + 404 wall
    @has_resource_permission("can_edit_dashboards")     # ③ pool membership
    def update_dashboard(request, dashboard_id):
        dashboard = request.resource                    # body: zero access code

The pool is flat — grant FKs, floors, and ownership are peer sources; a
single ``required slug in pool`` check decides. No hierarchy: a Member
granted Edit on one dashboard edits that dashboard. (Role slugs answer ①
only — see ``build_permission_pool``.)

Ownership-only actions (delete, transfer) are NOT pool questions — the role
carries ``can_delete_*`` org-wide, so a pool check would widen delete to
every Analyst. Those stay on the ownership axis (``can_delete_resource``).
"""

import inspect
from functools import wraps

from ninja.errors import HttpError

from ddpui.auth import ADMIN_ROLE, ANALYST_ROLE, MEMBER_ROLE, SUPER_ADMIN_ROLE
from ddpui.core.sharing.access_resolver import principal_match_q
from ddpui.core.sharing.permission_map import RTYPE_LEVEL_SLUG, implied_closure, slug_for
from ddpui.core.sharing.shareable_types import get_resource_type
from ddpui.models.general_access import AccessLevel
from ddpui.models.resource_share import ResourceShare

# 403/404 noun per rtype — same wording contract as gates.py (the webapp
# matches on these strings).
_NOUN_BY_RTYPE = {
    "chart": "chart",
    "dashboard": "dashboard",
    "report": "report",
    "alert": "alert",
    "metric": "metric",
    "kpi": "KPI",
}

_FLOOR_FIELD_BY_ROLE = {ANALYST_ROLE: "analyst_level", MEMBER_ROLE: "member_level"}

_KNOWN_RESOURCE_SLUGS = set(RTYPE_LEVEL_SLUG.values())


def _role_slug(orguser):
    role = getattr(orguser, "new_role", None)
    return getattr(role, "slug", None) if role is not None else None


def _is_owner(orguser, resource):
    owner_id = getattr(resource, "owner_id", None)
    if owner_id is not None:
        return owner_id == orguser.id
    created_by_id = getattr(resource, "created_by_id", None)
    return created_by_id is not None and created_by_id == orguser.id


def build_permission_pool(orguser, rtype, resource):
    """The flat pool for (orguser, resource): grant slugs ∪ floor slug ∪
    owner/admin contribution, closed under implication.

    Role slugs are deliberately NOT a pool source — they answer ① ("may you
    use this area of the app"). Pooling them would hand every Member
    ``can_view_dashboards`` on every dashboard, erasing floors and list
    scoping. The role's per-resource contribution is the floor columns.
    No-hierarchy still holds where it matters: grants are never capped by
    role on ``member_edit_grants`` rtypes."""
    pool = set()
    entry = get_resource_type(rtype)
    role_slug = _role_slug(orguser)
    view_slug = slug_for(rtype, "view")
    edit_slug = slug_for(rtype, "edit")

    is_admin = role_slug in (ADMIN_ROLE, SUPER_ADMIN_ROLE)
    if is_admin or _is_owner(orguser, resource):
        pool.update(s for s in (view_slug, edit_slug) if s)

    # Member exclusion mirrors the resolver: member_sharing=False rtypes give
    # Members nothing from floors or grants (ownership already handled).
    member_excluded = role_slug == MEMBER_ROLE and entry is not None and not entry.member_sharing
    if not member_excluded:
        floor_field = _FLOOR_FIELD_BY_ROLE.get(role_slug)
        if floor_field is not None:
            level = getattr(resource, floor_field, None)
            if level and level != AccessLevel.NONE:
                floor_slug = slug_for(rtype, level)
                if floor_slug:
                    pool.add(floor_slug)

        # One query: this viewer's active grant rows (user + group) on this
        # resource, FK slug joined in; varchar level is the fallback while
        # the FK is still nullable.
        grant_rows = ResourceShare.objects.filter(
            principal_match_q(orguser),
            org_id=getattr(resource, "org_id", None),
            resource_type=rtype,
            resource_id=str(resource.pk),
        ).values_list("granted_permission__slug", "permission")
        for fk_slug, level in grant_rows:
            grant_slug = fk_slug or slug_for(rtype, level)
            if (
                grant_slug == edit_slug
                and role_slug == MEMBER_ROLE
                and not (entry and entry.member_edit_grants)
            ):
                # rtype hasn't opted into Member real-edit yet (plan §5)
                grant_slug = view_slug
            if grant_slug:
                pool.add(grant_slug)

    return implied_closure(pool)


def extract_resource(rtype: str, param: str = None):
    """② Fetch the org-scoped resource named by the route param (default
    ``{rtype}_id``) and attach it as ``request.resource``. Cross-org is
    indistinguishable from missing: 404."""
    entry = get_resource_type(rtype)
    if entry is None:
        raise ValueError(f"extract_resource: unknown rtype '{rtype}'")
    param = param or f"{rtype}_id"
    noun = _NOUN_BY_RTYPE.get(rtype, rtype)
    not_found = f"{noun[0].upper()}{noun[1:]} not found"

    def decorator(api_endpoint):
        sig = inspect.signature(api_endpoint)

        @wraps(api_endpoint)
        def wrapper(*args, **kwargs):
            request = args[0]
            bound = sig.bind(*args, **kwargs)
            if param not in bound.arguments:
                raise HttpError(404, not_found)
            resource = entry.model.objects.filter(
                pk=bound.arguments[param], org=request.orguser.org
            ).first()
            if resource is None:
                raise HttpError(404, not_found)
            request.resource = resource
            request.resource_rtype = rtype
            return api_endpoint(*args, **kwargs)

        return wrapper

    return decorator


def has_resource_permission(slug: str):
    """③ Deny with 403 unless ``slug`` is in the viewer's pool for
    ``request.resource``. Attaches the pool as
    ``request.resource_permissions`` for body reads."""
    if slug not in _KNOWN_RESOURCE_SLUGS:
        # Fail at import time, not per request — typos never ship.
        raise ValueError(f"has_resource_permission: unknown resource slug '{slug}'")

    def decorator(api_endpoint):
        @wraps(api_endpoint)
        def wrapper(*args, **kwargs):
            request = args[0]
            resource = getattr(request, "resource", None)
            rtype = getattr(request, "resource_rtype", None)
            if resource is None or rtype is None:
                raise RuntimeError("has_resource_permission requires @extract_resource above it")
            pool = build_permission_pool(request.orguser, rtype, resource)
            if slug not in pool:
                noun = _NOUN_BY_RTYPE.get(rtype, rtype)
                if slug == slug_for(rtype, "edit"):
                    raise HttpError(403, f"You do not have edit access to this {noun}")
                raise HttpError(403, f"You do not have access to this {noun}")
            request.resource_permissions = pool
            return api_endpoint(*args, **kwargs)

        return wrapper

    return decorator
