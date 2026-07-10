"""The hands: mutations for Resource Sharing — creating/revoking grants and
changing general access — plus the read that feeds the sharing modal.

Rules of this module:
- NO HTTP concerns: raise ``ddpui.core.sharing.exceptions`` errors; the API
  layer maps them to status codes.
- NO per-rtype branching: every capability/permission-slug lookup reads the
  ``shareable_types`` registry entry (data, not if/else).
- ``access_resolver`` stays read-only — this module is the ONLY place
  Resource Sharing writes happen.

Public links and access requests are later tasks; owner transfer is a later
task. Only grants + general access mutate here.
"""

from typing import List, Optional

from django.db import transaction

from ddpui.core.sharing.access_resolver import PERMISSION_RANK, effective_permission
from ddpui.core.sharing.exceptions import (
    GrantNotFoundError,
    PrincipalNotFoundError,
    SharingValidationError,
)
from ddpui.core.sharing.shareable_types import ShareableType, get_resource_type
from ddpui.models.general_access import GeneralAudience, GeneralLevel
from ddpui.models.org_user import OrgUser
from ddpui.models.resource_share import ResourceShare
from ddpui.schemas.access_schema import (
    AccessOverviewResponse,
    CapabilityFlags,
    GeneralAccessOut,
    GeneralAccessUpdate,
    GeneralAccessUpdateResponse,
    GrantCreate,
    GrantOut,
    OwnerOut,
    ViewerOut,
)

# Narrower-than comparison for the warn-and-offer protocol (plan Sec 4.5).
AUDIENCE_ORDER = {
    GeneralAudience.PRIVATE: 0,
    GeneralAudience.ADMINS: 1,
    GeneralAudience.ANALYSTS_PLUS: 2,
    GeneralAudience.ALL_USERS: 3,
}


def _entry_for(rtype: str) -> ShareableType:
    entry = get_resource_type(rtype)
    if entry is None:
        raise SharingValidationError(f"'{rtype}' is not a shareable resource type")
    return entry


def _orguser_name(orguser: OrgUser) -> str:
    """Display name convention used across the codebase (dbt_api, alert_api)."""
    user = orguser.user
    return f"{user.first_name} {user.last_name}".strip() or user.email


def _owner_orguser(resource) -> Optional[OrgUser]:
    """The resource's owner: owner FK wins; created_by is the fallback when
    owner is null. Mirrors the resolver's ownership rule."""
    if getattr(resource, "owner_id", None) is not None:
        return resource.owner
    return getattr(resource, "created_by", None)


def _grants_for(rtype: str, resource):
    return ResourceShare.objects.filter(
        org_id=resource.org_id,
        resource_type=rtype,
        resource_id=str(resource.pk),
    ).order_by("id")


def _grant_out(share: ResourceShare, orgusers_by_id: dict) -> GrantOut:
    principal = orgusers_by_id.get(share.principal_id) if share.principal_type == "user" else None
    return GrantOut(
        id=share.id,
        principal_type=share.principal_type,
        principal_id=share.principal_id,
        email=principal.user.email if principal else share.pending_email,
        name=_orguser_name(principal) if principal else None,
        permission=share.permission,
        status=share.status,
    )


def _grants_out(shares: List[ResourceShare]) -> List[GrantOut]:
    user_ids = [s.principal_id for s in shares if s.principal_type == "user" and s.principal_id]
    orgusers_by_id = {
        ou.id: ou for ou in OrgUser.objects.filter(id__in=user_ids).select_related("user")
    }
    return [_grant_out(share, orgusers_by_id) for share in shares]


def get_access_overview(viewer: OrgUser, rtype: str, resource) -> AccessOverviewResponse:
    """Who has access to `resource` and via which path: owner, general
    access, and grant rows (active + pending). Read-only."""
    entry = _entry_for(rtype)

    owner = _owner_orguser(resource)
    owner_out = (
        OwnerOut(orguser_id=owner.id, email=owner.user.email, name=_orguser_name(owner))
        if owner
        else None
    )

    general_out = None
    if entry.general:
        general_out = GeneralAccessOut(
            audience=resource.general_audience, level=resource.general_level
        )

    shares = list(_grants_for(rtype, resource).filter(status__in=["active", "pending"]))

    return AccessOverviewResponse(
        resource_type=rtype,
        resource_id=str(resource.pk),
        capabilities=CapabilityFlags(
            general=entry.general,
            grants=entry.grants,
            public_link=entry.public_link,
            requests=entry.requests,
        ),
        owner=owner_out,
        general_access=general_out,
        grants=_grants_out(shares),
        viewer=ViewerOut(
            effective_permission=effective_permission(viewer, rtype, resource),
            is_owner=owner is not None and owner.id == viewer.id,
        ),
    )


def upsert_grant(grantor: OrgUser, rtype: str, resource, payload: GrantCreate) -> GrantOut:
    """Grant `payload.permission` on `resource` to a user principal. A
    duplicate (same principal, same resource) updates the existing row
    instead of stacking a second one."""
    entry = _entry_for(rtype)
    if not entry.grants:
        raise SharingValidationError(f"{rtype} does not support per-user grants")

    if payload.principal_type == "audience":
        raise SharingValidationError("audience grants are not supported")
    if payload.principal_type == "group":
        raise SharingValidationError("group grants are not available yet")
    if payload.principal_type != "user":
        raise SharingValidationError(f"invalid principal_type '{payload.principal_type}'")

    if payload.permission not in GeneralLevel.values:
        raise SharingValidationError(f"invalid permission '{payload.permission}'")

    principal = (
        OrgUser.objects.filter(id=payload.principal_id, org_id=grantor.org_id)
        .select_related("user")
        .first()
    )
    if principal is None:
        raise PrincipalNotFoundError("user not found in this organization")

    # Re-share cap: a grantor may grant at most their own effective level.
    grantor_level = effective_permission(grantor, rtype, resource)
    if PERMISSION_RANK.get(payload.permission, 0) > PERMISSION_RANK.get(grantor_level or "", 0):
        raise SharingValidationError(
            "you cannot grant a higher level of access than you have yourself"
        )

    share, _ = ResourceShare.objects.update_or_create(
        org_id=grantor.org_id,
        resource_type=rtype,
        resource_id=str(resource.pk),
        principal_type="user",
        principal_id=principal.id,
        defaults={
            "permission": payload.permission,
            "status": "active",
            "pending_email": None,
            "created_by": grantor,
        },
    )
    return _grant_out(share, {principal.id: principal})


def remove_grant(orguser: OrgUser, rtype: str, resource, grant_id: int) -> None:
    """Delete one grant row. The row must belong to this org + resource."""
    _entry_for(rtype)
    deleted, _ = ResourceShare.objects.filter(
        id=grant_id,
        org_id=orguser.org_id,
        resource_type=rtype,
        resource_id=str(resource.pk),
    ).delete()
    if deleted == 0:
        raise GrantNotFoundError("grant not found for this resource")


def set_general_access(
    orguser: OrgUser,  # pylint: disable=unused-argument  (kept: actor context, audit hook)
    rtype: str,
    resource,
    payload: GeneralAccessUpdate,
) -> GeneralAccessUpdateResponse:
    """Change the resource's general access, with the narrowing
    warn-and-offer protocol (plan Sec 4.5):

    - Widening (or same-width) changes apply immediately.
    - Narrowing while active grants exist: the first call (no
      `remove_grant_ids` field) returns `requires_confirmation=True` with
      the grants that would keep people in, and changes NOTHING. The client
      re-sends with `remove_grant_ids` (possibly []) to commit.
    """
    entry = _entry_for(rtype)
    if not entry.general:
        raise SharingValidationError(f"{rtype} does not support general access")

    if payload.audience not in GeneralAudience.values:
        raise SharingValidationError(f"invalid audience '{payload.audience}'")
    if payload.level not in GeneralLevel.values:
        raise SharingValidationError(f"invalid level '{payload.level}'")

    current_rank = AUDIENCE_ORDER.get(resource.general_audience, 0)
    new_rank = AUDIENCE_ORDER[payload.audience]
    narrowing = new_rank < current_rank

    if narrowing and payload.remove_grant_ids is None:
        persisting = list(_grants_for(rtype, resource).filter(status="active"))
        if persisting:
            return GeneralAccessUpdateResponse(
                requires_confirmation=True,
                persisting_grants=_grants_out(persisting),
            )

    with transaction.atomic():
        if payload.remove_grant_ids:
            remove_ids = set(payload.remove_grant_ids)
            removable = _grants_for(rtype, resource).filter(id__in=remove_ids)
            if removable.count() != len(remove_ids):
                raise GrantNotFoundError("one or more grant ids not found for this resource")
            removable.delete()

        resource.general_audience = payload.audience
        resource.general_level = payload.level
        resource.save(update_fields=["general_audience", "general_level"])

    return GeneralAccessUpdateResponse(
        general_access=GeneralAccessOut(audience=payload.audience, level=payload.level)
    )
