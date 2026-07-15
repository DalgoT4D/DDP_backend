"""The bouncer: read-only Resource Sharing access decisions.

Pure functions only. NO writes, NO HTTP, NO notification calls, and NO
``if resource_type == "..."`` branching — this module never imports a
specific resource model and never knows what a "dashboard" is; it only
knows about OrgUser roles, the generic shareable-resource contract
(``analyst_level``/``member_level``/``owner``/``created_by``/``org``), and
``ResourceShare`` grant rows.

D1 (permission-model rework): general access used to be one org-wide
(audience, level) pair -- an audience *threshold* (private/admins/
analysts_plus/all_users) admitting every role at or above a tier to the
SAME level. It is now one independent ``AccessLevel`` per role
(``analyst_level``, ``member_level``) -- Admins are never stored, they
always resolve to full access at step 1.

Decision ladder (plan Sec 4.4, updated for D1):
    1. Admin / super-admin?                              -> edit (org-wide override)
    2. Owner? (owner_id, falling back to created_by)      -> edit
    3. Viewer's role has a general-access level set?      -> that role's stored level
       (Analyst -> ``resource.analyst_level``, Member -> ``resource.member_level``;
       "none" or an unresolvable role -> nothing from this step)
    4. Grant rows matching the viewer?                    -> best level among them;
       a Member's grant contribution is capped at "view" (unchanged pre-D1 rule
       -- grants are explicitly UNTOUCHED by this rework)
    5. Best of steps 3-4.
    6. Nothing matched, or role is null/legacy?            -> None (default-deny, never raise)

Note the Member cap moved: pre-D1 it capped the COMBINED result (general
access + grants) at "view" for every Member, because the old model had no
way to give Members "edit" via general access at all. D1 makes
``member_level="edit"`` explicitly storable and meaningful (the design
screens require it), so the cap is no longer applied to the general-access
contribution -- only to the grant contribution, which this task deliberately
leaves UNTOUCHED.

All role reads are getattr-safe: a viewer with a null/unknown role never
raises, it is just denied whatever that role would have granted. A viewer
in a different org than the resource always gets None.
"""

from typing import Callable, Optional, Set

from django.db.models import BigIntegerField, Q
from django.db.models.functions import Cast

from ddpui.auth import ADMIN_ROLE, ANALYST_ROLE, MEMBER_ROLE, SUPER_ADMIN_ROLE
from ddpui.models.general_access import AccessLevel
from ddpui.models.resource_share import ResourceShare
from ddpui.models.user_group import UserGroupMember, UserGroupMemberStatus

GetGroupIds = Callable[[object], Set[int]]

# Role rank derives from slugs, deliberately NOT `Role.level` (plan Sec 4.4).
ROLE_RANK = {
    MEMBER_ROLE: 1,
    ANALYST_ROLE: 2,
    ADMIN_ROLE: 3,
    SUPER_ADMIN_ROLE: 4,
}

# Which model field holds a given role's general-access level. Only
# Analyst and Member are stored -- Admins/super-admins never reach step 3
# (step 1 already returned "edit"), and any other/unresolvable role slug
# has no general-access field to read.
GENERAL_LEVEL_FIELD_BY_ROLE = {
    ANALYST_ROLE: "analyst_level",
    MEMBER_ROLE: "member_level",
}

PERMISSION_RANK = {"view": 1, "edit": 2}

# A Q that never matches -- used where a role has no general-access field
# to read (accessible_filter's list-scoping equivalent of step 3).
_NEVER_Q = Q(pk__in=[])


def _default_get_group_ids(viewer):
    """Active ``UserGroupMember`` group ids for ``viewer`` (Task 7).

    Returns a lazy ``values_list`` queryset, NOT a materialized set — it is
    passed straight through to ``principal_id__in=`` in ``principal_match_q``
    and embeds as a SQL subquery, so wiring this in as the default adds no
    extra round trip for any existing caller (lists, gates, access
    endpoints all keep their query counts).
    """
    viewer_id = getattr(viewer, "id", None)
    if viewer_id is None:
        return UserGroupMember.objects.none().values_list("group_id", flat=True)
    return UserGroupMember.objects.filter(
        orguser_id=viewer_id, status=UserGroupMemberStatus.ACTIVE
    ).values_list("group_id", flat=True)


def _role_slug(viewer) -> Optional[str]:
    role = getattr(viewer, "new_role", None)
    return getattr(role, "slug", None) if role is not None else None


def _best_permission(*perms: Optional[str]) -> Optional[str]:
    candidates = [p for p in perms if p is not None]
    if not candidates:
        return None
    return max(candidates, key=lambda p: PERMISSION_RANK.get(p, 0))


def principal_match_q(viewer, get_group_ids: Optional[GetGroupIds] = None) -> Q:
    """ONE Q-object predicate matching active ``ResourceShare`` rows for this
    viewer: rows granted to their OrgUser id, union rows granted to any
    group id ``get_group_ids(viewer)`` returns.

    ``principal_type="audience"`` rows are never matched here (v1 deferral).

    ``get_group_ids`` may return anything ``principal_id__in=`` accepts,
    including a lazy queryset (the Task 7 default does, so it embeds as a
    SQL subquery instead of forcing an extra round trip). Deliberately does
    NOT materialize it with ``set(...)`` the way the seam's first cut did —
    that would force-evaluate a lazy queryset default. ``None`` (a stub
    explicitly opting out) is normalized to "no groups".
    """
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
    """Step 3: the resource's own general-access level for this viewer's
    role -- ``resource.analyst_level`` for an Analyst, ``resource.
    member_level`` for a Member. None if the role isn't Analyst/Member
    (unresolvable, null, legacy, or already-handled Admin/super-admin) or
    the stored level for that role is "none"."""
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

    # Step 3: this role's own general-access level (uncapped -- D1 makes
    # member_level="edit" a real, storable outcome).
    general = _general_permission(role_slug, resource)

    # Step 4: explicit grants. A Member's grant contribution stays capped
    # at "view" -- the pre-D1 rule, preserved here because grants are
    # explicitly UNTOUCHED by this rework (only the general-access cap moved).
    grant = _grant_permission(viewer, rtype, resource, get_group_ids)
    if role_slug == MEMBER_ROLE and grant == "edit":
        grant = "view"

    # Step 5: best of steps 3-4.
    return _best_permission(general, grant)


def accessible_filter(
    viewer,
    rtype: str,
    get_group_ids: Optional[GetGroupIds] = None,
) -> Q:
    """One ORM Q for list endpoints: rows `viewer` can see via general
    access (their role's stored level is not "none"), a `ResourceShare`
    grant, or ownership.

    Admins are not special-cased here — the ladder's org-wide admin
    override is the caller's responsibility to apply (callers may skip
    calling this at all for admins). This Q is truthful for what general
    access + grants + ownership admit, on its own.
    """
    if get_group_ids is None:
        get_group_ids = _default_get_group_ids

    viewer_org_id = getattr(viewer, "org_id", None)
    role_slug = _role_slug(viewer)

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
