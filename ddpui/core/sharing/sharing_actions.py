"""Mutations for Resource Sharing: grants, general access, ownership transfer,
public links, and the bulk fan-out — plus the read that feeds the sharing modal.

- No HTTP concerns: raise ``ddpui.core.sharing.exceptions``; the API layer
  maps them to status codes.
- Capability lookups read the ``shareable_types`` registry instead of
  per-rtype branching, with two deliberate exceptions:
  ``MEMBER_GRANTS_DEFERRED_RTYPES`` (a policy set, not a capability) and the
  dashboard-only broadening warnings (dashboards are the only rtype that
  contains other shareable resources).
- All sharing writes happen here; ``access_resolver`` stays read-only.
"""

import secrets
from typing import List, Optional, Tuple

from django.db import transaction
from django.db.models import Count, Q
from django.utils import timezone

from ddpui.auth import ADMIN_ROLE, ANALYST_ROLE, MEMBER_ROLE, SUPER_ADMIN_ROLE
from ddpui.core import orguserfunctions
from ddpui.core.sharing import coverage
from ddpui.core.sharing.access_resolver import PERMISSION_RANK, effective_permission
from ddpui.core.sharing.chart_access import dashboard_chart_ids
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
from ddpui.models.visualization import Chart
from ddpui.schemas.access_schema import (
    AccessOverviewResponse,
    BulkAccessRequest,
    BulkAccessResponse,
    BulkConfirmationItem,
    BulkItemRef,
    BulkSkippedItem,
    CapabilityFlags,
    ChartCoverageOut,
    GeneralAccessOut,
    GeneralAccessUpdate,
    GeneralAccessUpdateResponse,
    GrantCreate,
    GrantCreateResponse,
    GrantOut,
    OwnerOut,
    ViewerOut,
)
from ddpui.utils import awsses
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.sharing.sharing_actions")

# Hard cap on a bulk selection, enforced at the API layer.
BULK_MAX_ITEMS = 100

# Roles a share-flow email invite may assign; super-admin is deliberately not invitable.
INVITABLE_ROLE_SLUGS = (MEMBER_ROLE, ANALYST_ROLE, ADMIN_ROLE)

# Rtypes where NEW direct/invite grants to Member-role users are blocked.
# Members still reach these via general access, group grants, or access requests.
MEMBER_GRANTS_DEFERRED_RTYPES = frozenset({"metric", "kpi"})


def _reject_member_principal(rtype: str, principal: OrgUser) -> None:
    """Reject a direct grant to a Member-role principal on a deferred rtype."""
    if rtype not in MEMBER_GRANTS_DEFERRED_RTYPES:
        return
    principal_role = principal.new_role.slug if principal.new_role else None
    if principal_role == MEMBER_ROLE:
        raise SharingValidationError(
            f"{rtype} can only be shared directly with Analysts or Admins right now "
            "-- Member grants aren't available yet"
        )


def _reject_member_invite(rtype: str, invite_role: Optional[str]) -> None:
    """Reject an invite that would resolve to Member on a deferred rtype.
    Runs before any invite email or Invitation row is created."""
    if rtype not in MEMBER_GRANTS_DEFERRED_RTYPES:
        return
    if (invite_role or MEMBER_ROLE) == MEMBER_ROLE:
        raise SharingValidationError(
            f"{rtype} invites can only be sent at Analyst or Admin right now "
            "-- Member invites aren't available yet"
        )


def _entry_for(rtype: str) -> ShareableType:
    entry = get_resource_type(rtype)
    if entry is None:
        raise SharingValidationError(f"'{rtype}' is not a shareable resource type")
    return entry


# Role slugs a user-principal grant may target on a member_sharing=False rtype.
_MEMBER_SHARING_EXEMPT_SLUGS = (ANALYST_ROLE, ADMIN_ROLE, SUPER_ADMIN_ROLE)


def _member_share_blocked_message(rtype: str) -> str:
    noun = NOUN_BY_RTYPE.get(rtype, rtype)
    return (
        f"{noun}s cannot be shared with Members yet — Members keep seeing "
        f"them inside shared dashboards and reports"
    )


def _require_grantable_principal_role(entry: ShareableType, principal: OrgUser) -> None:
    """On a member_sharing=False rtype, user grants may only target Analyst/Admin
    principals. Group grants skip this: the resolver gives their Member members nothing."""
    if entry.member_sharing:
        return
    principal_slug = principal.new_role.slug if principal.new_role else None
    if principal_slug not in _MEMBER_SHARING_EXEMPT_SLUGS:
        raise SharingValidationError(_member_share_blocked_message(entry.rtype))


def _require_invitable_role_for_rtype(entry: ShareableType, invite_role: Optional[str]) -> None:
    """On a member_sharing=False rtype, an email invite must resolve to
    Analyst/Admin. Runs before any invitation email or pending row exists."""
    if entry.member_sharing:
        return
    if (invite_role or MEMBER_ROLE) == MEMBER_ROLE:
        noun = NOUN_BY_RTYPE.get(entry.rtype, entry.rtype)
        raise SharingValidationError(
            f"{noun}s cannot be shared with Members yet — invite them as an "
            f"Analyst or Admin instead"
        )


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
    """Resolve the role a share-flow invite assigns: Member by default; only
    admin/super-admin callers may pick a higher role. Deliberately stricter than
    `invite_user_v1`'s own tier check, which still runs downstream."""
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
    """Send one share-flow invite for `email` at the resolved role. Returns an
    instant OrgUser when the email already has a platform account (no Invitation
    is created on that path), else None — the caller writes pending grant rows."""
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
    """One grant row for an invited email: active for an instant OrgUser, else
    pending — activated later when the invitation is accepted."""
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
    """Invite an unknown email once, then write its grant row. Bulk uses the
    two halves directly so N resources still send exactly one invitation."""
    _reject_member_invite(rtype, invite_role)
    instant_principal = _invite_email_once(grantor, email, invite_role)
    return _email_grant_row(grantor, rtype, resource, email, permission, instant_principal)


def _notify_resource_shared(
    grantor: OrgUser, rtype: str, resource, principal: OrgUser, permission: str
) -> None:
    """Email the principal that `resource` was shared with them. Best-effort:
    a send failure is logged and swallowed — it must never fail the share."""
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
    """Grant `payload.permission` on `resource` to a user or group principal;
    a duplicate updates the existing row instead of stacking a second one.
    An email matching an existing org user grants instantly; an unknown email
    invites them and creates a pending grant."""
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

    # Re-share cap: a grantor may grant at most their own effective level,
    # checked before any invite email or pending row is created.
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
            .select_related("user", "new_role")
            .first()
        )
        if principal is None:
            raise PrincipalNotFoundError("user not found in this organization")
    else:
        email = payload.email.strip().lower()
        principal = (
            OrgUser.objects.filter(org_id=grantor.org_id, user__email__iexact=email)
            .select_related("user", "new_role")
            .first()
        )
        if principal is None:
            _require_invitable_role_for_rtype(entry, payload.invite_role)
            return _invite_and_create_pending_grant(
                grantor, rtype, resource, email, payload.permission, payload.invite_role
            )

    # Two deliberately separate member-block mechanisms: the registry member_sharing
    # flag (charts) and MEMBER_GRANTS_DEFERRED_RTYPES (metric/kpi). Do not unify
    # without a product decision.
    _require_grantable_principal_role(entry, principal)
    _reject_member_principal(rtype, principal)

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
    # Notify only on a genuinely new grant to an active org user — never on a
    # permission update, and the invite path already sends its own email.
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
    """Transfer ownership to a same-org active user. The old owner gets an
    explicit active Edit grant (uniform rule; no reclaim). Deliberately bypasses
    the `entry.grants` flag and the Member-grants-deferred check: the old owner
    keeps Edit regardless of rtype capability or role."""
    _entry_for(rtype)  # rtype must be registered

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


# ================================================================================
# Dashboard-broadening warnings and their "extend" action — dashboard-only
# by design (see module docstring).
# ================================================================================


def extend_charts_to_cover_dashboard(actor: OrgUser, dashboard, charts: List[Chart]) -> None:
    """Bring each chart's standalone access up to cover the dashboard's audience:
    raise `analyst_level` none -> view and copy the dashboard's active Analyst/Admin
    user grants and group grants onto the chart at View. Member and pending rows are
    skipped; existing chart grants and `member_level` are never touched. Requires
    the actor to have Edit on every chart — checked before any write."""
    if not charts:
        return

    for chart in charts:
        if effective_permission(actor, "chart", chart) != "edit":
            raise SharingPermissionError(
                f'you need Edit access on the chart "{chart.title}" to extend it'
            )

    grants = list(_grants_for("dashboard", dashboard).filter(status="active"))
    user_ids = [g.principal_id for g in grants if g.principal_type == "user" and g.principal_id]
    role_by_orguser_id = dict(
        OrgUser.objects.filter(id__in=user_ids).values_list("id", "new_role__slug")
    )

    with transaction.atomic():
        for chart in charts:
            if (
                dashboard.analyst_level != AccessLevel.NONE
                and chart.analyst_level == AccessLevel.NONE
            ):
                chart.analyst_level = AccessLevel.VIEW
                chart.save(update_fields=["analyst_level"])

            for grant in grants:
                if grant.principal_id is None:
                    continue
                if grant.principal_type == "user":
                    if role_by_orguser_id.get(grant.principal_id) not in (
                        ANALYST_ROLE,
                        ADMIN_ROLE,
                        SUPER_ADMIN_ROLE,
                    ):
                        continue  # Member / null-role principals skipped
                elif grant.principal_type != "group":
                    continue
                ResourceShare.objects.get_or_create(
                    org_id=dashboard.org_id,
                    resource_type="chart",
                    resource_id=str(chart.pk),
                    principal_type=grant.principal_type,
                    principal_id=grant.principal_id,
                    defaults={
                        "permission": "view",
                        "status": "active",
                        "pending_email": None,
                        "created_by": actor,
                    },
                )


def _broadening_confirmed(payload) -> bool:
    """Either confirm field present commits: `extend_chart_ids` (possibly [])
    extends that subset, `proceed=true` acknowledges without touching charts."""
    return payload.extend_chart_ids is not None or bool(payload.proceed)


def _validate_extend_subset(extend_chart_ids, warned: List[ChartCoverageOut]) -> set:
    """Every confirmed id must be a chart the warning named; returns the id set
    or raises the shared 400. `dashboard_native_api._validate_new_tile_charts`
    keeps an inline copy of this check that skips it when coverage is clean —
    mind that asymmetry before unifying the two."""
    extend_ids = set(extend_chart_ids or [])
    if extend_ids and not extend_ids <= {v.chart_id for v in warned}:
        raise SharingValidationError(
            "extend_chart_ids must be a subset of the under-covering charts"
        )
    return extend_ids


def _extend_confirmed_subset(
    actor: OrgUser, dashboard, extend_chart_ids, warned: List[ChartCoverageOut]
) -> None:
    """Validate + run the confirmed ``extend_chart_ids``."""
    extend_ids = _validate_extend_subset(extend_chart_ids, warned)
    if not extend_ids:
        return
    charts = list(Chart.objects.filter(id__in=extend_ids, org_id=dashboard.org_id))
    extend_charts_to_cover_dashboard(actor, dashboard, charts)


def _grant_widening_verdicts(
    grantor: OrgUser, dashboard, payload: GrantCreate
) -> List[ChartCoverageOut]:
    """Which tiles a new grant on `dashboard` would expose to the payload's
    principal. Resolves the principal read-only and leniently: an unresolvable
    principal yields no verdicts and falls through to `upsert_grant`'s validation."""
    if payload.principal_type == "group":
        if payload.principal_id is None:
            return []
        group = UserGroup.objects.filter(id=payload.principal_id, org_id=dashboard.org_id).first()
        if group is None:
            return []
        return coverage.under_covering_for_new_principal(grantor, dashboard, principal_group=group)

    if payload.principal_type != "user":
        return []

    principal = None
    if payload.principal_id is not None:
        principal = (
            OrgUser.objects.filter(id=payload.principal_id, org_id=dashboard.org_id)
            .select_related("user", "new_role")
            .first()
        )
    elif payload.email:
        principal = (
            OrgUser.objects.filter(
                org_id=dashboard.org_id, user__email__iexact=payload.email.strip().lower()
            )
            .select_related("user", "new_role")
            .first()
        )
        if principal is None:
            # unknown-email invite: judged by the role the invite would mint
            return coverage.under_covering_for_new_principal(
                grantor, dashboard, invite_role=payload.invite_role
            )
    if principal is None:
        return []
    return coverage.under_covering_for_new_principal(
        grantor, dashboard, principal_orguser=principal
    )


def upsert_grant_with_coverage(
    grantor: OrgUser, rtype: str, resource, payload: GrantCreate
) -> GrantCreateResponse:
    """`upsert_grant` wrapped in the dashboard-broadening warn-and-offer; a
    pass-through for every other rtype. If the new principal can't see some
    tiles standalone and no confirm field is present, nothing is written and
    the under-covering charts come back with `requires_confirmation=True`."""
    if rtype != "dashboard":
        return GrantCreateResponse(grant=upsert_grant(grantor, rtype, resource, payload))

    verdicts = _grant_widening_verdicts(grantor, resource, payload)
    if verdicts and not _broadening_confirmed(payload):
        return GrantCreateResponse(requires_confirmation=True, under_covering_charts=verdicts)

    # validate the extend subset BEFORE the grant write so a bad subset
    # cannot leave a half-applied share
    _validate_extend_subset(payload.extend_chart_ids, verdicts)

    grant = upsert_grant(grantor, rtype, resource, payload)
    _extend_confirmed_subset(grantor, resource, payload.extend_chart_ids, verdicts)
    return GrantCreateResponse(grant=grant)


def _narrowed_roles(resource, payload: GeneralAccessUpdate) -> set:
    """Which roles had their general-access level narrowed. Each role is compared
    independently — widening one while narrowing the other still flags the narrowed one."""
    narrowed = set()
    if ACCESS_LEVEL_RANK[payload.analyst_level] < ACCESS_LEVEL_RANK.get(resource.analyst_level, 0):
        narrowed.add(ANALYST_ROLE)
    if ACCESS_LEVEL_RANK[payload.member_level] < ACCESS_LEVEL_RANK.get(resource.member_level, 0):
        narrowed.add(MEMBER_ROLE)
    return narrowed


def _persisting_grants_for_narrowed_roles(rtype: str, resource, narrowed_roles: set) -> list:
    """Active grants that would keep someone admitted after the narrowing commits,
    filtered to the narrowed roles. Group grants are always included — memberships
    can mix roles, and over-warning is safer than silently dropping people."""
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
    orguser: OrgUser,
    rtype: str,
    resource,
    payload: GeneralAccessUpdate,
) -> GeneralAccessUpdateResponse:
    """Change the resource's per-role general access with the warn-and-offer
    protocol: narrowing a role while that role's principals hold active grants
    returns `requires_confirmation` with those grants and changes nothing; the
    re-send with `remove_grant_ids` (possibly []) commits. For dashboards,
    raising a level past an inner chart's access likewise requires confirmation
    (`extend_chart_ids`/`proceed`); extend runs after the levels save so it
    covers the new audience."""
    entry = _entry_for(rtype)
    if not entry.general:
        raise SharingValidationError(f"{rtype} does not support general access")

    if payload.analyst_level not in AccessLevel.values:
        raise SharingValidationError(f"invalid analyst_level '{payload.analyst_level}'")
    if payload.member_level not in AccessLevel.values:
        raise SharingValidationError(f"invalid member_level '{payload.member_level}'")
    # On a member_sharing=False rtype, member_level may only ever be "none".
    if payload.member_level != AccessLevel.NONE and not entry.member_sharing:
        raise SharingValidationError(_member_share_blocked_message(rtype))

    narrowed_roles = _narrowed_roles(resource, payload)

    persisting = []
    if narrowed_roles and payload.remove_grant_ids is None:
        persisting = _persisting_grants_for_narrowed_roles(rtype, resource, narrowed_roles)

    widening_charts: List[ChartCoverageOut] = []
    if rtype == "dashboard":
        widening_charts = coverage.under_covering_for_general_widening(
            orguser, resource, payload.analyst_level, payload.member_level
        )
    needs_widen_confirm = bool(widening_charts) and not _broadening_confirmed(payload)

    if persisting or needs_widen_confirm:
        return GeneralAccessUpdateResponse(
            requires_confirmation=True,
            persisting_grants=_grants_out(persisting, resource.org_id),
            under_covering_charts=widening_charts if needs_widen_confirm else [],
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

        if rtype == "dashboard" and payload.extend_chart_ids:
            # after the save: extend covers the dashboard's NEW audience
            _extend_confirmed_subset(orguser, resource, payload.extend_chart_ids, widening_charts)

    return GeneralAccessUpdateResponse(
        general_access=GeneralAccessOut(
            analyst_level=payload.analyst_level, member_level=payload.member_level
        )
    )


def set_public(
    actor: OrgUser,
    rtype: str,
    resource,
    is_public: bool,
    proceed: bool = False,
) -> Optional[List[ChartCoverageOut]]:
    """Flip the resource's public-link state — the one place this happens; the
    single-item toggles and bulk all call it. Enabling mints a token if missing
    and is blocked while the org kill switch is off; disabling always works and
    keeps the token for audit. Enabling a dashboard without `proceed` returns
    the under-covering charts and flips nothing; returns None when committed."""
    entry = _entry_for(rtype)
    if not entry.public_link:
        raise SharingValidationError(f"{rtype} does not support public links")

    if is_public and rtype == "dashboard" and not resource.is_public and not proceed:
        under_covering = coverage.under_covering_for_public_enable(actor, resource)
        if under_covering:
            return under_covering

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
    return None


# ================================================================================
# Bulk: one action fanned out over a resolved selection. Per-item failures
# become `skipped` rows; deliberately no selection-wide transaction — one
# item's failure never rolls back another's applied change.
# ================================================================================

ResolvedItems = List[Tuple[str, object]]  # [(rtype, resource), ...] already org+edit gated


def _skip(skipped: List[BulkSkippedItem], rtype: str, resource, reason: str) -> None:
    skipped.append(BulkSkippedItem(rtype=rtype, id=str(resource.pk), reason=reason))


def _validate_bulk_grant_payload(payload: GrantCreate) -> None:
    """Payload-shape validation, run once for the whole selection: a malformed
    payload fails the request rather than N per-item skips. Resource-dependent
    failures stay per-item."""
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


def _item_extend_ids(resource, flat_extend_ids, consumed: set):
    """The slice of a flat `extend_chart_ids` that are tiles on this dashboard
    (chart pks are unique org-wide). Marks them consumed so the caller can 400
    on leftovers that matched no selected dashboard."""
    if flat_extend_ids is None:
        return None
    tile_ids = dashboard_chart_ids(resource)
    item_ids = [cid for cid in flat_extend_ids if cid in tile_ids]
    consumed.update(item_ids)
    return item_ids


def _require_all_extend_ids_consumed(flat_extend_ids, consumed: set) -> None:
    if flat_extend_ids and set(flat_extend_ids) - consumed:
        raise SharingValidationError(
            "one or more extend_chart_ids are not tiles of the selected dashboards"
        )


def _bulk_add_grant(
    actor: OrgUser,
    payload: GrantCreate,
    resolved: ResolvedItems,
    applied: List[BulkItemRef],
    skipped: List[BulkSkippedItem],
    confirmations: List[BulkConfirmationItem],
) -> None:
    """`upsert_grant` per resource — except unknown emails, where the invite
    runs once for the whole selection and only the grant rows fan out.
    Dashboards needing a broadening confirmation land in `confirmations`
    with nothing written for them."""
    _validate_bulk_grant_payload(payload)

    consumed_extend_ids: set = set()

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
            item_payload = payload
            if rtype == "dashboard" and payload.extend_chart_ids is not None:
                item_payload = payload.model_copy(
                    update={
                        "extend_chart_ids": _item_extend_ids(
                            resource, payload.extend_chart_ids, consumed_extend_ids
                        )
                    }
                )
            try:
                result = upsert_grant_with_coverage(actor, rtype, resource, item_payload)
            except PrincipalNotFoundError:
                _skip(skipped, rtype, resource, "principal_not_found")
            except SharingValidationError:
                _skip(skipped, rtype, resource, "validation_error")
            else:
                if result.requires_confirmation:
                    confirmations.append(
                        BulkConfirmationItem(
                            rtype=rtype,
                            id=str(resource.pk),
                            under_covering_charts=result.under_covering_charts,
                        )
                    )
                else:
                    applied.append(BulkItemRef(rtype=rtype, id=str(resource.pk)))
        _require_all_extend_ids_consumed(payload.extend_chart_ids, consumed_extend_ids)
        return

    # Unknown email: decide eligibility per resource first — a fully blocked
    # selection must not send an email — then invite once and write grant rows.
    eligible: ResolvedItems = []
    warned_by_key: dict = {}
    for rtype, resource in resolved:
        entry = get_resource_type(rtype)
        if not entry.grants:
            _skip(skipped, rtype, resource, "grants_not_supported")
            continue
        # Both member-invite blocks, deliberately separate: registry member_sharing
        # (charts) and the deferred-rtypes set (metric/kpi). Do not unify without
        # a product decision.
        try:
            _require_invitable_role_for_rtype(entry, payload.invite_role)
        except SharingValidationError:
            _skip(skipped, rtype, resource, "validation_error")
            continue
        if (
            rtype in MEMBER_GRANTS_DEFERRED_RTYPES
            and (payload.invite_role or MEMBER_ROLE) == MEMBER_ROLE
        ):
            _skip(skipped, rtype, resource, "member_grants_deferred")
            continue
        grantor_level = effective_permission(actor, rtype, resource)
        if PERMISSION_RANK.get(payload.permission, 0) > PERMISSION_RANK.get(grantor_level or "", 0):
            _skip(skipped, rtype, resource, "validation_error")
            continue
        # An unconfirmed under-covering dashboard must not trigger an invite;
        # extend subsets are validated pre-invite so a bad list never leaves
        # a sent email behind.
        if rtype == "dashboard":
            verdicts = coverage.under_covering_for_new_principal(
                actor, resource, invite_role=payload.invite_role
            )
            if verdicts and not _broadening_confirmed(payload):
                confirmations.append(
                    BulkConfirmationItem(
                        rtype=rtype, id=str(resource.pk), under_covering_charts=verdicts
                    )
                )
                continue
            if payload.extend_chart_ids is not None:
                item_ids = _item_extend_ids(resource, payload.extend_chart_ids, consumed_extend_ids)
                _validate_extend_subset(item_ids, verdicts)
                warned_by_key[(rtype, str(resource.pk))] = (item_ids, verdicts)
        eligible.append((rtype, resource))

    _require_all_extend_ids_consumed(payload.extend_chart_ids, consumed_extend_ids)
    if not eligible:
        return

    instant_principal = _invite_email_once(actor, unknown_email, payload.invite_role)
    for rtype, resource in eligible:
        _email_grant_row(
            actor, rtype, resource, unknown_email, payload.permission, instant_principal
        )
        item_ids, verdicts = warned_by_key.get((rtype, str(resource.pk)), (None, []))
        if item_ids:
            _extend_confirmed_subset(actor, resource, item_ids, verdicts)
        applied.append(BulkItemRef(rtype=rtype, id=str(resource.pk)))


def _partition_remove_grant_ids(
    actor: OrgUser, remove_grant_ids: List[int], resolved: ResolvedItems
) -> dict:
    """Split the flat `remove_grant_ids` per (rtype, resource_id). Every id must
    be a grant of a resolved selection item in the caller's org — anything else
    fails the whole request."""
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
    """`set_general_access` per resource: items needing confirmation collect in
    `confirmations` (unchanged) while the rest apply immediately. The re-send
    commits each resource with its slice of the flat id list."""
    if payload.analyst_level not in AccessLevel.values:
        raise SharingValidationError(f"invalid analyst_level '{payload.analyst_level}'")
    if payload.member_level not in AccessLevel.values:
        raise SharingValidationError(f"invalid member_level '{payload.member_level}'")

    ids_by_resource = None
    if payload.remove_grant_ids is not None:
        ids_by_resource = _partition_remove_grant_ids(actor, payload.remove_grant_ids, resolved)

    consumed_extend_ids: set = set()

    for rtype, resource in resolved:
        if not get_resource_type(rtype).general:
            _skip(skipped, rtype, resource, "general_access_not_supported")
            continue
        item_ids = None
        if ids_by_resource is not None:
            item_ids = ids_by_resource.get((rtype, str(resource.pk)), [])
        item_extend_ids = None
        if rtype == "dashboard" and payload.extend_chart_ids is not None:
            # flat list partitioned per dashboard by tile membership
            item_extend_ids = _item_extend_ids(
                resource, payload.extend_chart_ids, consumed_extend_ids
            )
        item_payload = GeneralAccessUpdate(
            analyst_level=payload.analyst_level,
            member_level=payload.member_level,
            remove_grant_ids=item_ids,
            extend_chart_ids=item_extend_ids,
            proceed=payload.proceed,
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
                    under_covering_charts=result.under_covering_charts,
                )
            )
        else:
            applied.append(BulkItemRef(rtype=rtype, id=str(resource.pk)))

    _require_all_extend_ids_consumed(payload.extend_chart_ids, consumed_extend_ids)


def _bulk_toggle_public(
    actor: OrgUser,
    is_public: bool,
    resolved: ResolvedItems,
    applied: List[BulkItemRef],
    skipped: List[BulkSkippedItem],
    confirmations: List[BulkConfirmationItem],
    proceed: bool = False,
) -> None:
    """`set_public` per resource. The kill switch is read once: enabling while
    it's off skips every item; disabling is always allowed. Enabling a dashboard
    without `proceed` lands in `confirmations` with its charts named."""
    enable_allowed = org_allows_public_sharing(actor.org_id) if is_public else True
    for rtype, resource in resolved:
        if not get_resource_type(rtype).public_link:
            _skip(skipped, rtype, resource, "public_link_not_supported")
            continue
        if is_public and not enable_allowed:
            _skip(skipped, rtype, resource, "public_sharing_disabled")
            continue
        try:
            under_covering = set_public(actor, rtype, resource, is_public, proceed=proceed)
        except SharingValidationError:
            _skip(skipped, rtype, resource, "validation_error")
        else:
            if under_covering:
                confirmations.append(
                    BulkConfirmationItem(
                        rtype=rtype, id=str(resource.pk), under_covering_charts=under_covering
                    )
                )
            else:
                applied.append(BulkItemRef(rtype=rtype, id=str(resource.pk)))


def bulk_apply(
    actor: OrgUser,
    payload: BulkAccessRequest,
    resolved: ResolvedItems,
    skipped: List[BulkSkippedItem],
) -> BulkAccessResponse:
    """Fan `payload.action` out over the gate-surviving selection items.
    `skipped` arrives holding the gate skips and gains per-item action skips.
    No selection-wide transaction: one item's failure never rolls back another."""
    applied: List[BulkItemRef] = []
    confirmations: List[BulkConfirmationItem] = []

    if payload.action == "add_grant":
        _bulk_add_grant(actor, payload.add_grant, resolved, applied, skipped, confirmations)
    elif payload.action == "set_general":
        _bulk_set_general(actor, payload.set_general, resolved, applied, skipped, confirmations)
    elif payload.action == "toggle_public":
        _bulk_toggle_public(
            actor,
            payload.toggle_public.is_public,
            resolved,
            applied,
            skipped,
            confirmations,
            proceed=bool(payload.toggle_public.proceed),
        )
    else:  # the API layer validates first; this is defense in depth
        raise SharingValidationError(f"invalid action '{payload.action}'")

    return BulkAccessResponse(
        applied=applied,
        skipped=skipped,
        requires_confirmation=confirmations,
        applied_count=len(applied),
        skipped_count=len(skipped),
    )
