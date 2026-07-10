"""The bouncer: read-only Resource Sharing access decisions.

Pure functions only. NO writes, NO HTTP, NO notification calls, and NO
``if resource_type == "..."`` branching — this module never imports a
specific resource model and never knows what a "dashboard" is; it only
knows about OrgUser roles, the generic shareable-resource contract
(``general_audience``/``general_level``/``owner``/``created_by``/``org``),
and ``ResourceShare`` grant rows.

Decision ladder (plan Sec 4.4):
    1. Admin / super-admin?                          -> edit (org-wide override)
    2. Owner? (owner_id, falling back to created_by)  -> edit
    3. General access admits viewer's role tier?      -> the resource's general_level
    4. Grant rows matching the viewer?                -> best level among them
    5. Best of steps 3-4; viewer is Member?           -> capped at "view"
    6. Nothing matched, or role is null/legacy?        -> None (default-deny, never raise)

All role reads are getattr-safe: a viewer with a null/unknown role never
raises, it is just denied whatever that role would have granted. A viewer
in a different org than the resource always gets None.
"""

from typing import Callable, Optional, Set

from django.db.models import BigIntegerField, Q
from django.db.models.functions import Cast

from ddpui.auth import ADMIN_ROLE, ANALYST_ROLE, MEMBER_ROLE, SUPER_ADMIN_ROLE
from ddpui.models.general_access import GeneralAudience
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

# Minimum viewer role-rank a general-access audience tier admits.
# `private` maps to None: nobody is admitted via general access.
AUDIENCE_MIN_RANK = {
    GeneralAudience.PRIVATE: None,
    GeneralAudience.ADMINS: ROLE_RANK[ADMIN_ROLE],
    GeneralAudience.ANALYSTS_PLUS: ROLE_RANK[ANALYST_ROLE],
    GeneralAudience.ALL_USERS: ROLE_RANK[MEMBER_ROLE],
}

PERMISSION_RANK = {"view": 1, "edit": 2}


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


def _general_permission(rank: Optional[int], resource) -> Optional[str]:
    """The resource's general_level, if its general_audience tier admits a
    viewer of this role rank. None if the viewer's role is unknown/null or
    the tier doesn't admit them (including `private`, which admits nobody)."""
    if rank is None:
        return None
    min_rank = AUDIENCE_MIN_RANK.get(getattr(resource, "general_audience", None))
    if min_rank is not None and rank >= min_rank:
        return getattr(resource, "general_level", None)
    return None


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

    # Steps 3-4: best of general access and explicit grants.
    combined = _best_permission(
        _general_permission(rank, resource),
        _grant_permission(viewer, rtype, resource, get_group_ids),
    )
    if combined is None:
        return None

    # Step 5: Members are capped at "view" regardless of source.
    return "view" if role_slug == MEMBER_ROLE else combined


def accessible_filter(
    viewer,
    rtype: str,
    get_group_ids: Optional[GetGroupIds] = None,
) -> Q:
    """One ORM Q for list endpoints: rows `viewer` can see via general
    access (excluding `private`), a `ResourceShare` grant, or ownership.

    Admins are not special-cased here — the ladder's org-wide admin
    override is the caller's responsibility to apply (callers may skip
    calling this at all for admins). This Q is truthful for what general
    access + grants + ownership admit, on its own.
    """
    if get_group_ids is None:
        get_group_ids = _default_get_group_ids

    viewer_org_id = getattr(viewer, "org_id", None)
    rank = ROLE_RANK.get(_role_slug(viewer))

    allowed_audiences = [
        audience
        for audience, min_rank in AUDIENCE_MIN_RANK.items()
        if min_rank is not None and rank is not None and rank >= min_rank
    ]
    general_q = Q(general_audience__in=allowed_audiences)

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
