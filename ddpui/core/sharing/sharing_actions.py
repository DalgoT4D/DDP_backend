"""The hands: mutations for Resource Sharing — creating/revoking grants and
changing general access — plus the read that feeds the sharing modal.

Rules of this module:
- NO HTTP concerns: raise ``ddpui.core.sharing.exceptions`` errors; the API
  layer maps them to status codes.
- NO per-rtype branching: every capability/permission-slug lookup reads the
  ``shareable_types`` registry entry (data, not if/else).
- ``access_resolver`` stays read-only — this module is the ONLY place
  Resource Sharing writes happen.

Grants, general access, ownership transfer, the bulk fan-out (Task 17 — a
loop over the single-item functions, per the plan's "bulk is a loop" call),
and the generic public toggle mutate here.
"""

import secrets
from typing import List, Optional, Tuple

from django.db import transaction
from django.db.models import Count, Q
from django.utils import timezone

from ddpui.auth import ADMIN_ROLE, ANALYST_ROLE, MEMBER_ROLE, SUPER_ADMIN_ROLE
from ddpui.core import orguserfunctions
from ddpui.core.sharing.access_resolver import PERMISSION_RANK, effective_permission
from ddpui.core.sharing.deep_links import NOUN_BY_RTYPE, build_resource_url, resource_label
from ddpui.core.sharing.exceptions import (
    GrantNotFoundError,
    PrincipalNotFoundError,
    SharingPermissionError,
    SharingValidationError,
)
from ddpui.core.sharing.public_sharing_gate import org_allows_public_sharing
from ddpui.core.sharing.shareable_types import ShareableType, get_resource_type
from ddpui.models.general_access import ACCESS_LEVEL_RANK, AccessLevel, GeneralLevel
from ddpui.models.org_user import NewInvitationSchema, OrgUser
from ddpui.models.resource_share import ResourceShare
from ddpui.models.role_based_access import Role
from ddpui.models.user_group import UserGroup, UserGroupMemberStatus
from ddpui.schemas.access_schema import (
    AccessOverviewResponse,
    BulkAccessRequest,
    BulkAccessResponse,
    BulkConfirmationItem,
    BulkItemRef,
    BulkSkippedItem,
    CapabilityFlags,
    GeneralAccessOut,
    GeneralAccessUpdate,
    GeneralAccessUpdateResponse,
    GrantCreate,
    GrantOut,
    OwnerOut,
    ViewerOut,
)
from ddpui.utils import awsses
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.sharing.sharing_actions")

# Hard cap on a bulk selection (Task 17) — a plain constant, enforced at
# the API layer before any per-item work happens.
BULK_MAX_ITEMS = 100

# Roles a share-flow email invite may assign (Phase C3). super-admin is
# deliberately NOT invitable through sharing.
INVITABLE_ROLE_SLUGS = (MEMBER_ROLE, ANALYST_ROLE, ADMIN_ROLE)


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


def _grant_out(share: ResourceShare, orgusers_by_id: dict, groups_by_id: dict) -> GrantOut:
    if share.principal_type == "group":
        group = groups_by_id.get(share.principal_id)
        return GrantOut(
            id=share.id,
            principal_type=share.principal_type,
            principal_id=share.principal_id,
            email=None,
            name=group.name if group else None,
            permission=share.permission,
            status=share.status,
            member_count=getattr(group, "annotated_member_count", None) if group else None,
        )
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


def _grants_out(shares: List[ResourceShare], org_id) -> List[GrantOut]:
    user_ids = [s.principal_id for s in shares if s.principal_type == "user" and s.principal_id]
    group_ids = [s.principal_id for s in shares if s.principal_type == "group" and s.principal_id]

    orgusers_by_id = {
        ou.id: ou for ou in OrgUser.objects.filter(id__in=user_ids).select_related("user")
    }
    groups_by_id = {
        g.id: g
        for g in UserGroup.objects.filter(id__in=group_ids, org_id=org_id).annotate(
            annotated_member_count=Count(
                "members",
                filter=Q(members__status=UserGroupMemberStatus.ACTIVE),
                distinct=True,
            )
        )
    }
    return [_grant_out(share, orgusers_by_id, groups_by_id) for share in shares]


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
            analyst_level=resource.analyst_level, member_level=resource.member_level
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
        grants=_grants_out(shares, resource.org_id),
        viewer=ViewerOut(
            effective_permission=effective_permission(viewer, rtype, resource),
            is_owner=owner is not None and owner.id == viewer.id,
        ),
    )


def _resolve_invite_role(grantor: OrgUser, invite_role: Optional[str]) -> Role:
    """The role a share-flow invite assigns (Phase C3). Default **Member**.

    A non-Member role may only be requested by an admin/super-admin caller
    -- 403 otherwise. This is deliberately STRICTER than `invite_user_v1`'s
    own `invited_role.level > inviter.level` tier check (which would let an
    analyst mint analysts): the share flow keeps Part C's "non-Admin invites
    Member only" cap, now with an admin-only escape hatch. The level check
    still runs downstream inside `invite_user_v1` as defense in depth.
    """
    slug = invite_role or MEMBER_ROLE
    if slug not in INVITABLE_ROLE_SLUGS:
        raise SharingValidationError(f"invalid invite_role '{invite_role}'")

    if slug != MEMBER_ROLE:
        grantor_slug = grantor.new_role.slug if grantor.new_role else None
        if grantor_slug not in (ADMIN_ROLE, SUPER_ADMIN_ROLE):
            raise SharingPermissionError(f"only admins can invite new users as {slug}")

    role = Role.objects.filter(slug=slug).first()
    if role is None:
        raise SharingValidationError(f"the {slug} role is not configured for this org")
    return role


def _invite_email_once(
    grantor: OrgUser, email: str, invite_role: Optional[str] = None
) -> Optional[OrgUser]:
    """Send ONE share-flow invite for `email` (Task 9 / Part B) at the
    resolved invite role -- **Member** unless an admin caller chose more
    (`_resolve_invite_role`, Phase C3). The role gate runs BEFORE
    `invite_user_v1`, so an over-privileged request never sends an email or
    creates an `Invitation`.

    `invite_user_v1` short-circuits to an *instant* OrgUser when `email`
    already has a platform account (even one outside this org) -- no
    `Invitation` is created on that path. Returns that instant OrgUser when
    it happens, else None (a real Invitation was created/refreshed and the
    caller should write pending grant rows).
    """
    role = _resolve_invite_role(grantor, invite_role)

    _, error = orguserfunctions.invite_user_v1(
        grantor, NewInvitationSchema(invited_email=email, invited_role_uuid=role.uuid)
    )
    if error:
        raise SharingValidationError(error)

    return (
        OrgUser.objects.filter(org_id=grantor.org_id, user__email__iexact=email)
        .select_related("user")
        .first()
    )


def _email_grant_row(
    grantor: OrgUser,
    rtype: str,
    resource,
    email: str,
    permission: str,
    instant_principal: Optional[OrgUser],
) -> GrantOut:
    """One grant row for an invited email: active for an instant OrgUser
    (platform account already existed), else `status="pending"`, matched
    later by `orguserfunctions.activate_pending_shares_and_memberships` on
    invitation accept."""
    if instant_principal is not None:
        share, _ = ResourceShare.objects.update_or_create(
            org_id=grantor.org_id,
            resource_type=rtype,
            resource_id=str(resource.pk),
            principal_type="user",
            principal_id=instant_principal.id,
            defaults={
                "permission": permission,
                "status": "active",
                "pending_email": None,
                "created_by": grantor,
            },
        )
        return _grant_out(share, {instant_principal.id: instant_principal}, {})

    share, _ = ResourceShare.objects.update_or_create(
        org_id=grantor.org_id,
        resource_type=rtype,
        resource_id=str(resource.pk),
        principal_type="user",
        principal_id=None,
        pending_email=email,
        defaults={"permission": permission, "status": "pending", "created_by": grantor},
    )
    return _grant_out(share, {}, {})


def _invite_and_create_pending_grant(
    grantor: OrgUser,
    rtype: str,
    resource,
    email: str,
    permission: str,
    invite_role: Optional[str] = None,
) -> GrantOut:
    """The single-resource share-flow invite: `email` isn't an OrgUser of
    this org yet -- invite once (at `invite_role`, Member by default), then
    write the (pending or instant) grant row. Bulk (Task 17) uses the same
    two halves directly so a selection of N resources still sends exactly
    ONE invitation."""
    instant_principal = _invite_email_once(grantor, email, invite_role)
    return _email_grant_row(grantor, rtype, resource, email, permission, instant_principal)


def _notify_resource_shared(
    grantor: OrgUser, rtype: str, resource, principal: OrgUser, permission: str
) -> None:
    """Email an existing active org user that `resource` was just shared with
    them (Phase D1). Best-effort: a send failure is logged and swallowed --
    it must never fail the share request. Sent AFTER the grant row is
    committed, so a bounced email never leaves a half-applied share."""
    try:
        noun = NOUN_BY_RTYPE.get(rtype, rtype)
        awsses.send_resource_shared_email(
            to_email=principal.user.email,
            granter_email=grantor.user.email,
            resource_name=resource_label(rtype, resource),
            resource_noun=noun,
            permission_label="Edit" if permission == "edit" else "View",
            date_str=timezone.now().strftime("%b %d, %Y"),
            resource_url=build_resource_url(rtype, resource.pk),
        )
    except Exception:  # pylint: disable=broad-except
        logger.exception(
            f"failed to send resource-shared email to {principal.user.email} "
            f"for {rtype} {resource.pk}"
        )


def upsert_grant(grantor: OrgUser, rtype: str, resource, payload: GrantCreate) -> GrantOut:
    """Grant `payload.permission` on `resource` to a user or group principal.
    A duplicate (same principal, same resource) updates the existing row
    instead of stacking a second one.

    `principal_type="user"` accepts either `principal_id` (a same-org
    OrgUser) or `email` (Task 9 share-flow invite): an email matching an
    existing OrgUser in this org grants instantly (activation path 2 -- it
    never goes through `pending`); an unknown email invites them and
    creates a pending grant (`_invite_and_create_pending_grant`).
    """
    entry = _entry_for(rtype)
    if not entry.grants:
        raise SharingValidationError(f"{rtype} does not support per-user grants")

    if payload.principal_type == "audience":
        raise SharingValidationError("audience grants are not supported")
    if payload.principal_type not in ("user", "group"):
        raise SharingValidationError(f"invalid principal_type '{payload.principal_type}'")

    if payload.permission not in GeneralLevel.values:
        raise SharingValidationError(f"invalid permission '{payload.permission}'")

    if payload.principal_type == "group" and payload.email:
        raise SharingValidationError("email is only valid for principal_type='user'")

    # Re-share cap: a grantor may grant at most their own effective level.
    # Checked before any invite/pending row is created -- an over-cap
    # request must not send an email or create an Invitation.
    grantor_level = effective_permission(grantor, rtype, resource)
    if PERMISSION_RANK.get(payload.permission, 0) > PERMISSION_RANK.get(grantor_level or "", 0):
        raise SharingValidationError(
            "you cannot grant a higher level of access than you have yourself"
        )

    if payload.principal_type == "group":
        if payload.principal_id is None:
            raise SharingValidationError("principal_id is required for group grants")
        principal = UserGroup.objects.filter(id=payload.principal_id, org_id=grantor.org_id).first()
        if principal is None:
            raise PrincipalNotFoundError("group not found in this organization")
        share, _ = ResourceShare.objects.update_or_create(
            org_id=grantor.org_id,
            resource_type=rtype,
            resource_id=str(resource.pk),
            principal_type="group",
            principal_id=principal.id,
            defaults={
                "permission": payload.permission,
                "status": "active",
                "pending_email": None,
                "created_by": grantor,
            },
        )
        return _grant_out(share, {}, {principal.id: principal})

    # principal_type == "user"
    if payload.principal_id is not None and payload.email:
        raise SharingValidationError("provide only one of principal_id or email")
    if payload.principal_id is None and not payload.email:
        raise SharingValidationError("principal_id or email is required")

    if payload.principal_id is not None:
        principal = (
            OrgUser.objects.filter(id=payload.principal_id, org_id=grantor.org_id)
            .select_related("user")
            .first()
        )
        if principal is None:
            raise PrincipalNotFoundError("user not found in this organization")
    else:
        email = payload.email.strip().lower()
        principal = (
            OrgUser.objects.filter(org_id=grantor.org_id, user__email__iexact=email)
            .select_related("user")
            .first()
        )
        if principal is None:
            return _invite_and_create_pending_grant(
                grantor, rtype, resource, email, payload.permission, payload.invite_role
            )

    share, created = ResourceShare.objects.update_or_create(
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
    # D1: notify only on a genuinely NEW grant to an ACTIVE org user -- never
    # on a permission update (created is False), never for group grants (this
    # is the user-principal branch), never on the invite/pending path (that
    # returns earlier via _invite_and_create_pending_grant, which already
    # sends its own invitation email).
    if created and principal.user.is_active:
        _notify_resource_shared(grantor, rtype, resource, principal, payload.permission)
    return _grant_out(share, {principal.id: principal}, {})


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


def transfer_ownership(actor: OrgUser, rtype: str, resource, new_owner_orguser_id: int) -> OwnerOut:
    """Transfer `resource.owner` to `new_owner_orguser_id` (a same-org,
    active OrgUser). The OLD owner gets an explicit active Edit
    `ResourceShare` grant -- a uniform rule applied even when the old owner
    is also `created_by` (who'd already be admitted via
    `accessible_filter`'s `created_by` clause): it's what keeps their
    list/detail views consistent, and it's cheap. No reclaim: nothing marks
    the old owner special afterwards, they just stop passing the owner gate.

    Bypasses `entry.grants` deliberately: that flag gates the public
    `POST .../grants/` ENDPOINT (a UI capability), not this action -- a
    `metric`/`kpi` transfer (both `grants=False`) still writes the old
    owner's Edit row directly via `ResourceShare.objects.update_or_create`,
    same as every other write in this module, instead of routing through
    `upsert_grant` (which would reject it).
    """
    _entry_for(rtype)  # 404-equivalent validation: rtype must be registered

    old_owner = _owner_orguser(resource)
    if old_owner is not None and old_owner.id == new_owner_orguser_id:
        raise SharingValidationError("resource is already owned by this user")

    new_owner = (
        OrgUser.objects.filter(
            id=new_owner_orguser_id, org_id=resource.org_id, user__is_active=True
        )
        .select_related("user")
        .first()
    )
    if new_owner is None:
        raise PrincipalNotFoundError("user not found in this organization")

    with transaction.atomic():
        resource.owner = new_owner
        resource.save(update_fields=["owner"])

        if old_owner is not None:
            ResourceShare.objects.update_or_create(
                org_id=resource.org_id,
                resource_type=rtype,
                resource_id=str(resource.pk),
                principal_type="user",
                principal_id=old_owner.id,
                defaults={
                    "permission": "edit",
                    "status": "active",
                    "pending_email": None,
                    "created_by": actor,
                },
            )

    return OwnerOut(
        orguser_id=new_owner.id, email=new_owner.user.email, name=_orguser_name(new_owner)
    )


def _narrowed_roles(resource, payload: GeneralAccessUpdate) -> set:
    """Which of {ANALYST_ROLE, MEMBER_ROLE} had their general-access level
    narrowed by this change -- the new level ranks lower than the current
    one. D1: each role's narrowing is independent -- widening one role
    while narrowing the other in the SAME request still flags the narrowed
    one (e.g. analyst_level view->edit + member_level view->none narrows
    Members only)."""
    narrowed = set()
    if ACCESS_LEVEL_RANK[payload.analyst_level] < ACCESS_LEVEL_RANK.get(resource.analyst_level, 0):
        narrowed.add(ANALYST_ROLE)
    if ACCESS_LEVEL_RANK[payload.member_level] < ACCESS_LEVEL_RANK.get(resource.member_level, 0):
        narrowed.add(MEMBER_ROLE)
    return narrowed


def _persisting_grants_for_narrowed_roles(rtype: str, resource, narrowed_roles: set) -> list:
    """Active grants that would keep someone admitted even after the
    narrowing commits -- the warn-and-offer prompt's contents.

    Filtered to the roles actually narrowed (D1): a `user`-principal grant
    held by an Analyst is irrelevant to a member_level-only narrowing,
    since nothing about that Analyst's access is changing. `group`-principal
    grants are conservatively ALWAYS included -- a group's membership can
    mix roles, and resolving "does this group contain anyone whose narrowed
    role would otherwise lose access" would need a per-group membership
    join this task doesn't build; over-warning (surfacing a group grant
    that might not be affected) is the safer default over silently
    dropping people the admin should be told about.
    """
    if not narrowed_roles:
        return []
    grants = list(_grants_for(rtype, resource).filter(status="active"))
    if not grants:
        return []

    user_ids = [g.principal_id for g in grants if g.principal_type == "user" and g.principal_id]
    role_by_orguser_id = dict(
        OrgUser.objects.filter(id__in=user_ids).values_list("id", "new_role__slug")
    )

    persisting = []
    for grant in grants:
        if grant.principal_type == "group":
            persisting.append(grant)
        elif role_by_orguser_id.get(grant.principal_id) in narrowed_roles:
            persisting.append(grant)
    return persisting


def set_general_access(
    orguser: OrgUser,  # pylint: disable=unused-argument  (kept: actor context, audit hook)
    rtype: str,
    resource,
    payload: GeneralAccessUpdate,
) -> GeneralAccessUpdateResponse:
    """Change the resource's per-role general access (D1: `analyst_level`/
    `member_level`, each independently "none"/"view"/"edit"), with the
    narrowing warn-and-offer protocol (plan Sec 4.5) now evaluated per role:

    - Each role's level is compared independently -- narrowing is per role,
      not an overall audience-width comparison.
    - Narrowing a role while active grants held by that role's principals
      exist: the first call (no `remove_grant_ids` field) returns
      `requires_confirmation=True` with the grants that would keep those
      people in, and changes NOTHING. The client re-sends with
      `remove_grant_ids` (possibly []) to commit.
    """
    entry = _entry_for(rtype)
    if not entry.general:
        raise SharingValidationError(f"{rtype} does not support general access")

    if payload.analyst_level not in AccessLevel.values:
        raise SharingValidationError(f"invalid analyst_level '{payload.analyst_level}'")
    if payload.member_level not in AccessLevel.values:
        raise SharingValidationError(f"invalid member_level '{payload.member_level}'")

    narrowed_roles = _narrowed_roles(resource, payload)

    if narrowed_roles and payload.remove_grant_ids is None:
        persisting = _persisting_grants_for_narrowed_roles(rtype, resource, narrowed_roles)
        if persisting:
            return GeneralAccessUpdateResponse(
                requires_confirmation=True,
                persisting_grants=_grants_out(persisting, resource.org_id),
            )

    with transaction.atomic():
        if payload.remove_grant_ids:
            remove_ids = set(payload.remove_grant_ids)
            removable = _grants_for(rtype, resource).filter(id__in=remove_ids)
            if removable.count() != len(remove_ids):
                raise GrantNotFoundError("one or more grant ids not found for this resource")
            removable.delete()

        resource.analyst_level = payload.analyst_level
        resource.member_level = payload.member_level
        resource.save(update_fields=["analyst_level", "member_level"])

    return GeneralAccessUpdateResponse(
        general_access=GeneralAccessOut(
            analyst_level=payload.analyst_level, member_level=payload.member_level
        )
    )


def set_public(
    actor: OrgUser,  # pylint: disable=unused-argument -- kept as actor context / audit hook
    rtype: str,
    resource,
    is_public: bool,
) -> None:
    """Set the resource's public-link state — generic over the public-link
    contract every ``public_link=True`` rtype's model satisfies
    (``is_public``, ``public_share_token``, ``public_shared_at``,
    ``public_disabled_at`` — Dashboard and ReportSnapshot share these field
    names), so no per-rtype branching. This is the ONE place the flip
    happens: the single-item toggles (`dashboard_native_api.
    toggle_dashboard_sharing`, `report_api.toggle_report_sharing`) and the
    bulk `toggle_public` action all call this function directly, so the
    kill-switch rule -- enabling mints a token if missing and is blocked
    while the org kill switch is off; disabling always works and keeps the
    token for audit -- is defined exactly once.
    """
    entry = _entry_for(rtype)
    if not entry.public_link:
        raise SharingValidationError(f"{rtype} does not support public links")

    if is_public:
        if not org_allows_public_sharing(resource.org_id):
            raise SharingValidationError(
                "Public sharing is disabled for this organization. "
                "Ask an org admin to re-enable it."
            )
        if not resource.public_share_token:
            resource.public_share_token = secrets.token_urlsafe(48)
        resource.public_shared_at = timezone.now()
        resource.public_disabled_at = None
    else:
        resource.public_disabled_at = timezone.now()

    resource.is_public = is_public
    resource.save(
        update_fields=["is_public", "public_share_token", "public_shared_at", "public_disabled_at"]
    )


# ================================================================================
# Bulk (Task 17): one action fanned out over a resolved selection.
# Apply-where-possible: per-item failures become `skipped` rows with a
# reason code; there is deliberately NO selection-wide transaction — one
# item's failure never rolls back another's applied change.
# ================================================================================

ResolvedItems = List[Tuple[str, object]]  # [(rtype, resource), ...] already org+edit gated


def _skip(skipped: List[BulkSkippedItem], rtype: str, resource, reason: str) -> None:
    skipped.append(BulkSkippedItem(rtype=rtype, id=str(resource.pk), reason=reason))


def _validate_bulk_grant_payload(payload: GrantCreate) -> None:
    """The payload-shape half of `upsert_grant`'s validation, run ONCE for
    the whole selection: a malformed action payload is a client bug and
    fails the request (400), not N per-item skips. Resource-dependent
    failures (re-share cap, capability flag, cross-org principal) stay
    per-item."""
    if payload.principal_type not in ("user", "group"):
        raise SharingValidationError(f"invalid principal_type '{payload.principal_type}'")
    if payload.permission not in GeneralLevel.values:
        raise SharingValidationError(f"invalid permission '{payload.permission}'")
    if payload.principal_type == "group":
        if payload.email:
            raise SharingValidationError("email is only valid for principal_type='user'")
        if payload.principal_id is None:
            raise SharingValidationError("principal_id is required for group grants")
    else:
        if payload.principal_id is not None and payload.email:
            raise SharingValidationError("provide only one of principal_id or email")
        if payload.principal_id is None and not payload.email:
            raise SharingValidationError("principal_id or email is required")


def _bulk_add_grant(
    actor: OrgUser,
    payload: GrantCreate,
    resolved: ResolvedItems,
    applied: List[BulkItemRef],
    skipped: List[BulkSkippedItem],
) -> None:
    """`upsert_grant` per resource — except the unknown-email case, where
    the invite half runs ONCE for the whole selection (one Invitation, one
    email) and only the per-resource grant rows are fanned out."""
    _validate_bulk_grant_payload(payload)

    unknown_email = None
    if payload.principal_type == "user" and payload.email and payload.principal_id is None:
        email = payload.email.strip().lower()
        in_org = OrgUser.objects.filter(org_id=actor.org_id, user__email__iexact=email).exists()
        if not in_org:
            unknown_email = email

    if unknown_email is None:
        for rtype, resource in resolved:
            if not get_resource_type(rtype).grants:
                _skip(skipped, rtype, resource, "grants_not_supported")
                continue
            try:
                upsert_grant(actor, rtype, resource, payload)
            except PrincipalNotFoundError:
                _skip(skipped, rtype, resource, "principal_not_found")
            except SharingValidationError:
                _skip(skipped, rtype, resource, "validation_error")
            else:
                applied.append(BulkItemRef(rtype=rtype, id=str(resource.pk)))
        return

    # Unknown email: decide eligibility per resource FIRST (an over-cap or
    # capability-blocked selection must not send an email), then invite
    # once, then write one grant row per eligible resource.
    eligible: ResolvedItems = []
    for rtype, resource in resolved:
        if not get_resource_type(rtype).grants:
            _skip(skipped, rtype, resource, "grants_not_supported")
            continue
        grantor_level = effective_permission(actor, rtype, resource)
        if PERMISSION_RANK.get(payload.permission, 0) > PERMISSION_RANK.get(grantor_level or "", 0):
            _skip(skipped, rtype, resource, "validation_error")
            continue
        eligible.append((rtype, resource))

    if not eligible:
        return

    instant_principal = _invite_email_once(actor, unknown_email, payload.invite_role)
    for rtype, resource in eligible:
        _email_grant_row(
            actor, rtype, resource, unknown_email, payload.permission, instant_principal
        )
        applied.append(BulkItemRef(rtype=rtype, id=str(resource.pk)))


def _partition_remove_grant_ids(
    actor: OrgUser, remove_grant_ids: List[int], resolved: ResolvedItems
) -> dict:
    """Split the request's flat `remove_grant_ids` per (rtype, resource_id).
    Every id must be a grant of one of the RESOLVED selection items in the
    caller's org — an unknown id, a cross-org id, or an id belonging to a
    resource outside the (gate-surviving) selection is a client bug and
    fails the whole request, mirroring the single-item endpoint's 404."""
    requested = set(remove_grant_ids)
    resolved_keys = {(rtype, str(resource.pk)) for rtype, resource in resolved}
    ids_by_resource: dict = {}
    found = set()
    for gid, g_rtype, g_rid in ResourceShare.objects.filter(
        id__in=requested, org_id=actor.org_id
    ).values_list("id", "resource_type", "resource_id"):
        if (g_rtype, g_rid) not in resolved_keys:
            raise GrantNotFoundError("one or more grant ids not found for the selected resources")
        ids_by_resource.setdefault((g_rtype, g_rid), []).append(gid)
        found.add(gid)
    if found != requested:
        raise GrantNotFoundError("one or more grant ids not found for the selected resources")
    return ids_by_resource


def _bulk_set_general(
    actor: OrgUser,
    payload: GeneralAccessUpdate,
    resolved: ResolvedItems,
    applied: List[BulkItemRef],
    skipped: List[BulkSkippedItem],
    confirmations: List[BulkConfirmationItem],
) -> None:
    """`set_general_access` per resource, with the AGGREGATED narrow prompt:
    on the first call (no `remove_grant_ids`), resources that need
    confirmation are collected into `confirmations` (nothing changed for
    them) while every other resource applies immediately. The re-send
    (field present, possibly []) commits per resource with that resource's
    slice of the flat id list."""
    if payload.analyst_level not in AccessLevel.values:
        raise SharingValidationError(f"invalid analyst_level '{payload.analyst_level}'")
    if payload.member_level not in AccessLevel.values:
        raise SharingValidationError(f"invalid member_level '{payload.member_level}'")

    ids_by_resource = None
    if payload.remove_grant_ids is not None:
        ids_by_resource = _partition_remove_grant_ids(actor, payload.remove_grant_ids, resolved)

    for rtype, resource in resolved:
        if not get_resource_type(rtype).general:
            _skip(skipped, rtype, resource, "general_access_not_supported")
            continue
        item_ids = None
        if ids_by_resource is not None:
            item_ids = ids_by_resource.get((rtype, str(resource.pk)), [])
        item_payload = GeneralAccessUpdate(
            analyst_level=payload.analyst_level,
            member_level=payload.member_level,
            remove_grant_ids=item_ids,
        )
        try:
            result = set_general_access(actor, rtype, resource, item_payload)
        except SharingValidationError:
            _skip(skipped, rtype, resource, "validation_error")
            continue
        if result.requires_confirmation:
            confirmations.append(
                BulkConfirmationItem(
                    rtype=rtype,
                    id=str(resource.pk),
                    persisting_grants=result.persisting_grants,
                )
            )
        else:
            applied.append(BulkItemRef(rtype=rtype, id=str(resource.pk)))


def _bulk_toggle_public(
    actor: OrgUser,
    is_public: bool,
    resolved: ResolvedItems,
    applied: List[BulkItemRef],
    skipped: List[BulkSkippedItem],
) -> None:
    """`set_public` per resource. The kill switch is read once for the
    request: enabling while it's off skips every public-linkable item
    (reason `public_sharing_disabled`); disabling is always allowed."""
    enable_allowed = org_allows_public_sharing(actor.org_id) if is_public else True
    for rtype, resource in resolved:
        if not get_resource_type(rtype).public_link:
            _skip(skipped, rtype, resource, "public_link_not_supported")
            continue
        if is_public and not enable_allowed:
            _skip(skipped, rtype, resource, "public_sharing_disabled")
            continue
        try:
            set_public(actor, rtype, resource, is_public)
        except SharingValidationError:
            _skip(skipped, rtype, resource, "validation_error")
        else:
            applied.append(BulkItemRef(rtype=rtype, id=str(resource.pk)))


def bulk_apply(
    actor: OrgUser,
    payload: BulkAccessRequest,
    resolved: ResolvedItems,
    skipped: List[BulkSkippedItem],
) -> BulkAccessResponse:
    """Fan `payload.action` out over `resolved` — the selection items that
    survived the API layer's per-item gates (registry, share slug,
    org-scoped fetch, resolver edit). `skipped` arrives holding the gate
    skips and is extended with per-item action skips. NO selection-wide
    transaction, by design: each single-item function manages its own
    atomicity, and one item's failure never rolls back another."""
    applied: List[BulkItemRef] = []
    confirmations: List[BulkConfirmationItem] = []

    if payload.action == "add_grant":
        _bulk_add_grant(actor, payload.add_grant, resolved, applied, skipped)
    elif payload.action == "set_general":
        _bulk_set_general(actor, payload.set_general, resolved, applied, skipped, confirmations)
    elif payload.action == "toggle_public":
        _bulk_toggle_public(actor, payload.toggle_public.is_public, resolved, applied, skipped)
    else:  # the API layer validates first; this is defense in depth
        raise SharingValidationError(f"invalid action '{payload.action}'")

    return BulkAccessResponse(
        applied=applied,
        skipped=skipped,
        requires_confirmation=confirmations,
        applied_count=len(applied),
        skipped_count=len(skipped),
    )
