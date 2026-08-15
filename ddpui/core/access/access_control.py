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
"""

from typing import Iterable, Optional

from django.db.models import Model, Q

from ddpui.core.access import shareable_types
from ddpui.core.access.ownership import is_creator_or_admin
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_user import OrgUser, OrgUserGroupMember
from ddpui.models.resource_share import AccessLevel, ResourceShare, ResourceSharePrincipalType, max_access_level
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
            user_levels[row.resource_id] = max_access_level(user_levels.get(row.resource_id), row.access_level)
        else:
            group_levels[row.resource_id] = max_access_level(group_levels.get(row.resource_id), row.access_level)

    return {
        rid: max_access_level(user_levels.get(rid), group_levels.get(rid))
        for rid in user_levels.keys() | group_levels.keys()
    }


def _hide_no_access(level: str) -> Optional[str]:
    """Map an AccessLevel to the public answer: a ``no_access`` grant reads as
    invisible (None), everything else as itself."""
    return None if level == AccessLevel.NO_ACCESS else level


def get_user_access(orguser: OrgUser, rtype: str, resource_id) -> Optional[str]:
    """What can this orguser do with this resource?

    Returns:
        None                  — resource does not exist in this org
        AccessLevel.NO_ACCESS — resource exists but caller has no access
        AccessLevel.VIEW/EDIT — caller's effective level
    """
    entry = shareable_types.get_rtype_entry(rtype)
    row = (
        entry["model"].objects.filter(org=orguser.org, pk=resource_id)
        .only("created_by_id", "is_private")
        .first()
    )
    if row is None:
        return None

    if is_creator_or_admin(orguser, row):
        return AccessLevel.EDIT

    grants = _grants_map(orguser, rtype, [resource_id])
    granted = grants.get(str(resource_id))
    if row.is_private:
        return granted or AccessLevel.NO_ACCESS
    return max_access_level(granted, _org_floor(orguser)) or AccessLevel.NO_ACCESS


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
        else:
            granted = grants.get(str(resource.pk))
            if resource.is_private:
                levels[resource.pk] = _hide_no_access(granted)
            else:
                levels[resource.pk] = _hide_no_access(max_access_level(granted, floor))
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
    # Floor applies only to non-private resources; private resources need an explicit grant.
    return (
        Q(created_by=orguser)
        | Q(id__in=allowed_ids)
        | (Q(is_private=False) & ~Q(id__in=denied_ids))
    )
