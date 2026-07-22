"""Read-only Resource Sharing access decisions.

Pure functions: no writes, no HTTP, no per-rtype branching — this module
only knows OrgUser roles, the generic shareable-resource contract
(``analyst_level``/``member_level``/``owner``/``created_by``/``org``), and
``ResourceShare`` grant rows.

Decision ladder:
    1. Admin / super-admin?                           -> edit
    2. Owner (owner_id, falling back to created_by)?  -> edit
    3. The viewer's role's general-access level, if set
    4. Grant rows matching the viewer -> best level; a Member's grant
       contribution is capped at "view" except on rtypes with
       ``member_edit_grants=True`` (v1.2 flat pool — dashboards first)
    5. Best of steps 3-4; nothing matched -> None (default-deny, never raise)

Role reads are getattr-safe: a null/unknown role is denied, never raises.
A viewer in a different org always gets None. Rtypes with
``member_sharing=False`` give Member viewers nothing from steps 3-4;
ownership still wins.
"""

from typing import Callable, Optional, Set

from django.db.models import BigIntegerField, Q
from django.db.models.functions import Cast

from ddpui.auth import ADMIN_ROLE, ANALYST_ROLE, MEMBER_ROLE, SUPER_ADMIN_ROLE
from ddpui.core.sharing.permission_map import implied_closure, slug_for
from ddpui.core.sharing.shareable_types import get_resource_type
from ddpui.models.general_access import AccessLevel
from ddpui.models.resource_share import ResourceShare
from ddpui.models.user_group import UserGroupMember, UserGroupMemberStatus

GetGroupIds = Callable[[object], Set[int]]

# Role rank derives from slugs, deliberately not `Role.level`.
ROLE_RANK = {
    MEMBER_ROLE: 1,
    ANALYST_ROLE: 2,
    ADMIN_ROLE: 3,
    SUPER_ADMIN_ROLE: 4,
}

# Which model field holds a role's general-access level. Only Analyst and
# Member are stored — admins already returned "edit" at step 1.
GENERAL_LEVEL_FIELD_BY_ROLE = {
    ANALYST_ROLE: "analyst_level",
    MEMBER_ROLE: "member_level",
}

PERMISSION_RANK = {"view": 1, "edit": 2}

# A Q that never matches — used where a role has no general-access field to read.
_NEVER_Q = Q(pk__in=[])


def _default_get_group_ids(viewer):
    """Active group ids for `viewer`, as a lazy values_list queryset — it
    embeds as a SQL subquery in `principal_match_q`, adding no extra round trip."""
    viewer_id = getattr(viewer, "id", None)
    if viewer_id is None:
        return UserGroupMember.objects.none().values_list("group_id", flat=True)
    return UserGroupMember.objects.filter(
        orguser_id=viewer_id, status=UserGroupMemberStatus.ACTIVE
    ).values_list("group_id", flat=True)


def _role_slug(viewer) -> Optional[str]:
    role = getattr(viewer, "new_role", None)
    return getattr(role, "slug", None) if role is not None else None


def _member_excluded(role_slug: Optional[str], rtype: str) -> bool:
    """True when the viewer is a Member and `rtype` has `member_sharing=False`
    — steps 3-4 then contribute nothing for them."""
    if role_slug != MEMBER_ROLE:
        return False
    entry = get_resource_type(rtype)
    return entry is not None and not entry.member_sharing


def _best_permission(*perms: Optional[str]) -> Optional[str]:
    candidates = [p for p in perms if p is not None]
    if not candidates:
        return None
    return max(candidates, key=lambda p: PERMISSION_RANK.get(p, 0))


def principal_match_q(viewer, get_group_ids: Optional[GetGroupIds] = None) -> Q:
    """One Q matching active `ResourceShare` rows for this viewer: rows granted
    to their OrgUser id or to any group `get_group_ids(viewer)` returns.
    "audience" rows never match. `get_group_ids` may return a lazy queryset —
    deliberately not materialized with set(), which would force-evaluate it;
    None is normalized to "no groups"."""
    if get_group_ids is None:
        get_group_ids = _default_get_group_ids
    group_ids = get_group_ids(viewer)
    if group_ids is None:
        group_ids = []

    principal_q = Q(principal_type="user", principal_id=viewer.id) | Q(
        principal_type="group", principal_id__in=group_ids
    )

    return Q(status="active") & principal_q


def _grant_permission(viewer, rtype: str, resource, get_group_ids: GetGroupIds) -> Optional[str]:
    """Best permission among active grant rows matching this viewer for this
    specific resource."""
    perms = ResourceShare.objects.filter(
        principal_match_q(viewer, get_group_ids),
        org_id=getattr(resource, "org_id", None),
        resource_type=rtype,
        resource_id=str(resource.pk),
    ).values_list("permission", flat=True)
    return _best_permission(*perms)


def _is_owner(viewer, resource) -> bool:
    """Ownership check: owner_id wins; created_by is a fallback when owner
    is null. Mirrors ddpui.core.ownership.can_delete_resource."""
    owner_id = getattr(resource, "owner_id", None)
    if owner_id is not None:
        return owner_id == viewer.id
    created_by_id = getattr(resource, "created_by_id", None)
    return created_by_id is not None and created_by_id == viewer.id


def _general_permission(role_slug: Optional[str], resource) -> Optional[str]:
    """Step 3: the resource's general-access level for this viewer's role.
    None if the role isn't Analyst/Member or the stored level is "none"."""
    field_name = GENERAL_LEVEL_FIELD_BY_ROLE.get(role_slug)
    if field_name is None:
        return None
    level = getattr(resource, field_name, None)
    return level if level and level != AccessLevel.NONE else None


def effective_permission(
    viewer,
    rtype: str,
    resource,
    get_group_ids: Optional[GetGroupIds] = None,
) -> Optional[str]:
    """Return "edit", "view", or None for what `viewer` may do with `resource`."""
    if get_group_ids is None:
        get_group_ids = _default_get_group_ids

    viewer_org_id = getattr(viewer, "org_id", None)
    resource_org_id = getattr(resource, "org_id", None)
    if viewer_org_id is None or resource_org_id is None or viewer_org_id != resource_org_id:
        return None

    role_slug = _role_slug(viewer)
    rank = ROLE_RANK.get(role_slug)

    # Step 1: org-wide admin override.
    if rank is not None and rank >= ROLE_RANK[ADMIN_ROLE]:
        return "edit"

    # Step 2: ownership.
    if _is_owner(viewer, resource):
        return "edit"

    # Member viewers get nothing from steps 3-4 on member_sharing=False rtypes.
    if _member_excluded(role_slug, rtype):
        return None

    # Step 3: this role's own general-access level.
    general = _general_permission(role_slug, resource)

    # Step 4: explicit grants. A Member's grant contribution is capped at
    # "view" unless the rtype has opted into the v1.2 flat-pool behavior
    # (member_edit_grants=True — dashboards first, plan v1.2 §5).
    grant = _grant_permission(viewer, rtype, resource, get_group_ids)
    if role_slug == MEMBER_ROLE and grant == "edit":
        entry = get_resource_type(rtype)
        if entry is None or not entry.member_edit_grants:
            grant = "view"

    # Step 5: best of steps 3-4.
    return _best_permission(general, grant)


def get_resource_permissions(orguser, rtype: str, resource) -> Set[str]:
    """Permission slugs `orguser` holds on this specific resource (v1.2):
    grant rows ∪ the role's floor ∪ owner/admin, closed under implication
    (edit slugs include their view slug).

    `effective_permission` above answers the same question in view/edit
    vocabulary; this is the slug vocabulary the decorator gates check.
    Role slugs are deliberately NOT a source — they answer Layer 2 ("may you
    use this area of the app"); the role's per-resource contribution is the
    floor columns. Empty set = no access (default-deny)."""
    permissions: Set[str] = set()
    entry = get_resource_type(rtype)
    role_slug = _role_slug(orguser)
    view_slug = slug_for(rtype, "view")
    edit_slug = slug_for(rtype, "edit")

    rank = ROLE_RANK.get(role_slug)
    is_admin = rank is not None and rank >= ROLE_RANK[ADMIN_ROLE]
    if is_admin or _is_owner(orguser, resource):
        permissions.update(s for s in (view_slug, edit_slug) if s)

    if not _member_excluded(role_slug, rtype):
        floor_field = GENERAL_LEVEL_FIELD_BY_ROLE.get(role_slug)
        if floor_field is not None:
            level = getattr(resource, floor_field, None)
            if level and level != AccessLevel.NONE:
                floor_slug = slug_for(rtype, level)
                if floor_slug:
                    permissions.add(floor_slug)

        # One query: active grant rows for this viewer (user + groups) on this
        # resource, FK slug joined in; the varchar level covers null-FK rows.
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
                # rtype hasn't opted into Member real-edit yet (plan v1.2 §5)
                grant_slug = view_slug
            if grant_slug:
                permissions.add(grant_slug)

    return implied_closure(permissions)


def accessible_filter(
    viewer,
    rtype: str,
    get_group_ids: Optional[GetGroupIds] = None,
) -> Q:
    """One ORM Q for list endpoints: rows `viewer` can see via general access,
    a grant, or ownership. Admins are not special-cased — the caller applies
    the org-wide admin override (or skips this entirely for admins)."""
    if get_group_ids is None:
        get_group_ids = _default_get_group_ids

    viewer_org_id = getattr(viewer, "org_id", None)
    role_slug = _role_slug(viewer)

    # Same Member exclusion as effective_permission: on member_sharing=False
    # rtypes a Member viewer is admitted by ownership only.
    if _member_excluded(role_slug, rtype):
        general_q = _NEVER_Q
        granted_q = _NEVER_Q
    else:
        field_name = GENERAL_LEVEL_FIELD_BY_ROLE.get(role_slug)
        general_q = ~Q(**{field_name: AccessLevel.NONE}) if field_name is not None else _NEVER_Q

        granted_ids = (
            ResourceShare.objects.filter(
                principal_match_q(viewer, get_group_ids),
                org_id=viewer_org_id,
                resource_type=rtype,
            )
            .annotate(_resource_pk=Cast("resource_id", output_field=BigIntegerField()))
            .values_list("_resource_pk", flat=True)
        )
        granted_q = Q(id__in=granted_ids)

    owned_q = Q(owner_id=getattr(viewer, "id", None)) | Q(created_by_id=getattr(viewer, "id", None))

    return Q(org_id=viewer_org_id) & (general_q | granted_q | owned_q)
