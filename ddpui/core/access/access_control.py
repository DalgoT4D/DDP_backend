"""The single decision point for per-resource access (enforcement pass).

Answers one question three ways:
- ``get_user_access``      — one resource: "view", "edit", or None (invisible)
- ``get_user_access_map``  — many resources at once (lists; avoids N+1 queries)
- ``accessible_filter``    — the same rule as one ORM ``Q`` for list endpoints

Precedence (first match wins):
  1. creator or admin/super-admin        -> "edit" (full)
  2. explicit USER grant                 -> its level ("no_access" -> None)
  3. GROUP grants (max across groups)    -> best level
  4. org-default floor                   -> default_analyst_level / default_member_level

``no_access`` at step 2 is the explicit deny — the only way to revoke one
principal below a permissive org floor. This is precedence, not max-merge,
on purpose.

This module imports models only. ``ddpui.auth`` imports it for the ``has_access``
decorator, and ``ownership.py`` imports ``ddpui.auth`` — importing auth here
would create a cycle.
"""

from typing import Iterable, Optional

from django.db.models import Model, Q

from ddpui.core.access import shareable_types
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_user import OrgUser, OrgUserGroupMember
from ddpui.models.resource_share import AccessLevel, LEVEL_RANK, ResourceShare, ResourceSharePrincipalType
from ddpui.models.role_based_access import RoleSlug


def _is_admin(orguser: OrgUser) -> bool:
    return orguser.new_role is not None and orguser.new_role.slug in (
        RoleSlug.ADMIN,
        RoleSlug.SUPER_ADMIN,
    )


def _org_floor(orguser: OrgUser) -> str:
    """The org-default level for this orguser's role. Analysts read the analyst
    column; every other non-admin role reads the member column. A missing
    OrgPreferences row means both floors are "view" (the model defaults)."""
    prefs = OrgPreferences.objects.filter(org=orguser.org).first()
    if orguser.new_role is not None and orguser.new_role.slug == RoleSlug.ANALYST:
        return prefs.default_analyst_level if prefs else AccessLevel.VIEW
    return prefs.default_member_level if prefs else AccessLevel.VIEW


def _grants_map(
    orguser: OrgUser, rtype: str, resource_ids: Optional[Iterable[str]] = None
) -> dict[str, str]:
    """Resolve grants into {resource_id(str): level} honoring precedence
    (an explicit user grant beats any group grant). One query."""
    group_ids = list(
        OrgUserGroupMember.objects.filter(orguser=orguser).values_list("group_id", flat=True)
    )
    principal_q = Q(principal_type=ResourceSharePrincipalType.USER, principal_id=orguser.id)
    if group_ids:
        principal_q |= Q(
            principal_type=ResourceSharePrincipalType.GROUP, principal_id__in=group_ids
        )

    rows = ResourceShare.objects.filter(org=orguser.org, resource_type=rtype).filter(principal_q)
    if resource_ids is not None:
        rows = rows.filter(resource_id__in=[str(rid) for rid in resource_ids])

    user_levels: dict[str, str] = {}
    group_levels: dict[str, str] = {}
    for row in rows:
        if row.principal_type == ResourceSharePrincipalType.USER:
            user_levels[row.resource_id] = row.access_level
        else:
            best = group_levels.get(row.resource_id)
            if best is None or LEVEL_RANK[row.access_level] > LEVEL_RANK[best]:
                group_levels[row.resource_id] = row.access_level

    return {**group_levels, **user_levels}  # user rows override group rows


def _hide_no_access(level: str) -> Optional[str]:
    """Map an AccessLevel to the public answer: a ``no_access`` grant reads as
    invisible (None), everything else as itself."""
    return None if level == AccessLevel.NO_ACCESS else level


def get_user_access(orguser: OrgUser, rtype: str, resource_id) -> Optional[str]:
    """What can this orguser do with this resource? "edit", "view", or None
    (invisible — the caller should treat it exactly like a missing resource)."""
    entry = shareable_types.get_rtype_entry(rtype)
    row = entry["model"].objects.filter(org=orguser.org, pk=resource_id).values("created_by_id").first()
    if row is None:
        return None

    if row["created_by_id"] is not None and row["created_by_id"] == orguser.id:
        return AccessLevel.EDIT
    if _is_admin(orguser):
        return AccessLevel.EDIT

    grants = _grants_map(orguser, rtype, [resource_id])
    granted = grants.get(str(resource_id))
    if granted is not None:
        return _hide_no_access(granted)

    return _hide_no_access(_org_floor(orguser))


def get_user_access_map(
    orguser: OrgUser, rtype: str, resources: Iterable[Model]
) -> dict[int, Optional[str]]:
    """Batch variant of ``get_user_access`` for list serialization — one grants
    query for the whole page instead of one per row."""
    resources = list(resources)
    if _is_admin(orguser):
        return {r.pk: AccessLevel.EDIT for r in resources}

    grants = _grants_map(orguser, rtype, [r.pk for r in resources])
    floor = _org_floor(orguser)

    levels: dict[int, Optional[str]] = {}
    for resource in resources:
        if resource.created_by_id is not None and resource.created_by_id == orguser.id:
            levels[resource.pk] = AccessLevel.EDIT
        elif str(resource.pk) in grants:
            levels[resource.pk] = _hide_no_access(grants[str(resource.pk)])
        else:
            levels[resource.pk] = _hide_no_access(floor)
    return levels


def accessible_filter(orguser: OrgUser, rtype: str) -> Q:
    """The same rule as one ORM filter, for list endpoints: which resources of
    this rtype may the orguser see at all (level >= view)? Compose it into the
    list query with ``&`` — inaccessible rows never leave the database."""
    if _is_admin(orguser):
        return Q()

    grants = _grants_map(orguser, rtype)
    allowed_ids = [int(rid) for rid, level in grants.items() if level != AccessLevel.NO_ACCESS]
    denied_ids = [int(rid) for rid, level in grants.items() if level == AccessLevel.NO_ACCESS]

    if _org_floor(orguser) == AccessLevel.NO_ACCESS:
        return Q(created_by=orguser) | Q(id__in=allowed_ids)
    return Q(created_by=orguser) | ~Q(id__in=denied_ids)
