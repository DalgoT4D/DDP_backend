"""Business logic for creating, listing, and removing ``ResourceShare`` rows.

Consumers are the ``access_api.py`` endpoints; this module owns the pending-
email flow, dedup rules, and error taxonomy so those endpoints stay thin.

Payload/response shapes live in ``ddpui/schemas/access/resource_share_schema``.
"""

from typing import Optional

from ddpui.core import orguserfunctions
from ddpui.core.access import shareable_types
from ddpui.models.dashboard import Dashboard
from ddpui.models.org import Org
from ddpui.models.org_user import (
    Invitation,
    NewInvitationSchema,
    OrgUser,
    OrgUserGroup,
    OrgUserGroupMember,
)
from ddpui.models.resource_share import (
    AccessLevel,
    LEVEL_RANK,
    ResourceShare,
    ResourceSharePrincipalType,
    ResourceType,
)
from ddpui.models.role_based_access import Role
from ddpui.schemas.access.resource_share_schema import (
    CascadeSourceSchema,
    PendingGrantPayload,
    PrincipalGrantPayload,
    ShareRowSchema,
)


class GrantError(Exception):
    """Business-rule violation with a user-facing message. The API layer
    catches this and returns 400."""


# ---------------------------------------------------------------------------
# Cascade helpers


def _parse_inner_ids(tabs) -> dict:
    """Parse dashboard tabs JSON → {chart_ids: [...], kpi_ids: [...]}.
    Called at write-time only — never on the read path."""
    chart_ids, kpi_ids = [], []
    for tab in tabs or []:
        for comp in (tab.get("components") or {}).values():
            cfg = comp.get("config", {})
            if comp.get("type") == "chart" and cfg.get("chartId"):
                chart_ids.append(int(cfg["chartId"]))
            elif comp.get("type") == "kpi" and cfg.get("kpiId"):
                kpi_ids.append(int(cfg["kpiId"]))
    return {"chart_ids": list(set(chart_ids)), "kpi_ids": list(set(kpi_ids))}


def sync_dashboard_cascade(dashboard: Dashboard) -> None:
    """Full sync of cascade child rows for all shares on a dashboard.

    After this call, cascade children exactly match the dashboard's current
    tabs — correct level, correct set of charts/KPIs, stale rows removed.
    Call after any share change (add/update) or any tabs change.
    Invitation-backed shares are skipped — promoted on acceptance.

    Owner protection: the dashboard's ``created_by`` gets an auto self-share
    at EDIT so their access is materialised in the DB and survives org-floor
    changes. That self-share cascades to inner charts/KPIs at VIEW (not EDIT)
    so the owner never gains modification rights on charts they don't
    personally own via dashboard ownership alone.
    """
    inner = _parse_inner_ids(dashboard.tabs)
    current_chart_ids = [str(cid) for cid in inner["chart_ids"]]
    current_kpi_ids = [str(kid) for kid in inner["kpi_ids"]]

    # Ensure the owner has a top-level self-share on the dashboard — the loop
    # below will populate inner-chart/KPI children for them at VIEW. Idempotent.
    if dashboard.created_by_id:
        ResourceShare.objects.get_or_create(
            org=dashboard.org,
            resource_type=ResourceType.DASHBOARD,
            resource_id=str(dashboard.id),
            principal_type=ResourceSharePrincipalType.USER,
            principal_id=dashboard.created_by_id,
            parent=None,
            defaults={
                "access_level": AccessLevel.EDIT,
                "created_by_id": dashboard.created_by_id,
            },
        )

    dashboard_shares = list(
        ResourceShare.objects.filter(
            org=dashboard.org,
            resource_type=ResourceType.DASHBOARD,
            resource_id=str(dashboard.id),
            parent__isnull=True,
            principal_type__isnull=False,
        )
    )
    if not dashboard_shares:
        return

    for share in dashboard_shares:
        # Owner's auto self-share cascades at VIEW; everyone else at their own
        # level. Keeps the owner protected against floor changes without
        # granting them modification rights on inner charts they don't own.
        is_owner_self_share = (
            share.principal_type == ResourceSharePrincipalType.USER
            and share.principal_id == dashboard.created_by_id
        )
        cascade_level = AccessLevel.VIEW if is_owner_self_share else share.access_level

        ResourceShare.objects.filter(parent=share).update(access_level=cascade_level)

        for chart_id in current_chart_ids:
            ResourceShare.objects.get_or_create(
                org=dashboard.org,
                resource_type=ResourceType.CHART,
                resource_id=chart_id,
                principal_type=share.principal_type,
                principal_id=share.principal_id,
                parent=share,
                defaults={"access_level": cascade_level, "created_by": share.created_by},
            )
        for kpi_id in current_kpi_ids:
            ResourceShare.objects.get_or_create(
                org=dashboard.org,
                resource_type=ResourceType.KPI,
                resource_id=kpi_id,
                principal_type=share.principal_type,
                principal_id=share.principal_id,
                parent=share,
                defaults={"access_level": cascade_level, "created_by": share.created_by},
            )

        ResourceShare.objects.filter(parent=share, resource_type=ResourceType.CHART).exclude(
            resource_id__in=current_chart_ids
        ).delete()

        ResourceShare.objects.filter(parent=share, resource_type=ResourceType.KPI).exclude(
            resource_id__in=current_kpi_ids
        ).delete()


# ---------------------------------------------------------------------------
# Read


def list_grants(org: Org, rtype: str, resource_id) -> list[ShareRowSchema]:
    """Return one ShareRowSchema per principal for a resource.

    Rows are grouped by principal so that a user who has both a direct grant
    and one or more cascade rows appears only once, with:
    - access_level = max across all their rows
    - share_id = the direct row's id (None if cascade-only)
    - cascade_sources = list of source dashboards (for the frontend block message)

    The resource owner is excluded — they are surfaced separately via ``OwnerInfo``
    and must not appear as a duplicate entry in the shares list.
    """
    resource_id_str = str(resource_id)
    resource = shareable_types.get_resource(org, rtype, resource_id)
    owner_orguser_id = getattr(resource, "created_by_id", None) if resource else None

    rows = list(
        ResourceShare.objects.filter(org=org, resource_type=rtype, resource_id=resource_id_str)
        .select_related("invitation__invited_new_role")
        .order_by("created_at")
    )

    # Strip any USER-type rows for the current owner — they are shown in the
    # dedicated Owner section and must not appear again in the shares list.
    if owner_orguser_id is not None:
        rows = [
            r
            for r in rows
            if not (
                r.principal_type == ResourceSharePrincipalType.USER
                and r.principal_id == owner_orguser_id
            )
        ]

    # Resolve principals in bulk.
    user_ids = [r.principal_id for r in rows if r.principal_type == ResourceSharePrincipalType.USER]
    group_ids = [
        r.principal_id for r in rows if r.principal_type == ResourceSharePrincipalType.GROUP
    ]
    users_by_id = {
        u.id: u
        for u in OrgUser.objects.filter(org=org, id__in=user_ids).select_related("user", "new_role")
    }
    groups_by_id = {g.id: g for g in OrgUserGroup.objects.filter(org=org, id__in=group_ids)}

    # Resolve cascade source dashboards in bulk.
    parent_ids = [r.parent_id for r in rows if r.parent_id is not None]
    cascade_source_by_parent_id: dict[int, CascadeSourceSchema] = {}
    if parent_ids:
        parent_rows = ResourceShare.objects.filter(id__in=parent_ids).values("id", "resource_id")
        dashboard_ids = [int(pr["resource_id"]) for pr in parent_rows]
        dash_titles = {
            str(d["id"]): d["title"]
            for d in Dashboard.objects.filter(org=org, id__in=dashboard_ids).values("id", "title")
        }
        for pr in parent_rows:
            cascade_source_by_parent_id[pr["id"]] = CascadeSourceSchema(
                dashboard_id=int(pr["resource_id"]),
                dashboard_title=dash_titles.get(str(pr["resource_id"]), "Unknown"),
            )

    # Group rows by principal key.
    principal_rows: dict[tuple, list[ResourceShare]] = {}
    for row in rows:
        if row.principal_type == ResourceSharePrincipalType.USER and row.principal_id:
            key = ("user", row.principal_id)
        elif row.principal_type == ResourceSharePrincipalType.GROUP and row.principal_id:
            key = ("group", row.principal_id)
        elif row.invitation_id:
            key = ("invitation", row.invitation_id)
        else:
            continue  # orphan row — skip
        principal_rows.setdefault(key, []).append(row)

    shares: list[ShareRowSchema] = []
    for (ptype, pid), prows in principal_rows.items():
        direct_rows = [r for r in prows if r.parent_id is None]
        cascade_rows = [r for r in prows if r.parent_id is not None]

        direct_row = direct_rows[0] if direct_rows else None
        share_id = direct_row.id if direct_row else None

        effective_level = max((r.access_level for r in prows), key=lambda lvl: LEVEL_RANK[lvl])

        cascade_sources = [
            cascade_source_by_parent_id[r.parent_id]
            for r in cascade_rows
            if r.parent_id in cascade_source_by_parent_id
        ]

        if ptype == "user" and pid in users_by_id:
            u = users_by_id[pid]
            shares.append(
                ShareRowSchema(
                    share_id=share_id,
                    principal_type="user",
                    principal_id=pid,
                    email=u.user.email,
                    label=u.user.email,
                    role_or_group=u.new_role.name if u.new_role else None,
                    access_level=effective_level,
                    status="active",
                    cascade_sources=cascade_sources,
                )
            )
        elif ptype == "group" and pid in groups_by_id:
            g = groups_by_id[pid]
            shares.append(
                ShareRowSchema(
                    share_id=share_id,
                    principal_type="group",
                    principal_id=pid,
                    email=None,
                    label=g.name,
                    role_or_group="Group",
                    access_level=effective_level,
                    status="active",
                    cascade_sources=cascade_sources,
                )
            )
        elif ptype == "invitation":
            inv_row = direct_row or prows[0]
            if inv_row.invitation is not None:
                shares.append(
                    ShareRowSchema(
                        share_id=share_id,
                        principal_type="invitation",
                        principal_id=None,
                        email=inv_row.invitation.invited_email,
                        label=inv_row.invitation.invited_email,
                        role_or_group=(
                            inv_row.invitation.invited_new_role.name
                            if inv_row.invitation.invited_new_role
                            else None
                        ),
                        access_level=effective_level,
                        status="pending",
                        cascade_sources=cascade_sources,
                    )
                )
        # Orphans (principal deleted) are silently skipped.

    return shares


# ---------------------------------------------------------------------------
# Write


def add_grants(
    orguser: OrgUser,
    rtype: str,
    resource_id,
    principals: list[PrincipalGrantPayload],
    pending_grants: list[PendingGrantPayload],
    invite_role_uuid: Optional[str],
) -> tuple[list[ResourceShare], list[str]]:
    """Idempotent multi-add.

    Existing rows for the same (org, rtype, resource_id, principal_type,
    principal_id) get their ``access_level`` updated in-place — the modal
    treats staged chips as "make this the level"; it does not create
    duplicate rows.

    Returns ``(written_shares, warnings)`` where ``warnings`` is a list of
    user-facing advisory messages (e.g. the owner is a member of a shared group).
    """
    if orguser.org is None:
        raise GrantError("no associated org")
    org = orguser.org
    resource_id_str = str(resource_id)

    # Resolve the owner so we can block/warn about sharing with them.
    resource = shareable_types.get_resource(org, rtype, resource_id)
    owner_orguser_id = getattr(resource, "created_by_id", None) if resource else None

    written: list[ResourceShare] = []
    warnings: list[str] = []

    # 1) Concrete principals — user or group.
    for grant in principals:
        _check_principal_exists(org, grant.principal_type, grant.principal_id)

        if (
            grant.principal_type == ResourceSharePrincipalType.USER
            and owner_orguser_id is not None
            and grant.principal_id == owner_orguser_id
        ):
            raise GrantError("cannot add a direct share for the resource owner")

        if (
            grant.principal_type == ResourceSharePrincipalType.GROUP
            and owner_orguser_id is not None
            and OrgUserGroupMember.objects.filter(
                group_id=grant.principal_id, orguser_id=owner_orguser_id
            ).exists()
        ):
            warnings.append(
                "The resource owner is a member of this group. "
                "Their access will not be affected by this share."
            )

        # Restrict to direct rows. Cascade child rows share the same principal
        # coordinates but are derived from a parent share (parent set); mutating
        # one directly would break the cascade invariant and get reverted on
        # the next sync_dashboard_cascade.
        existing = ResourceShare.objects.filter(
            org=org,
            resource_type=rtype,
            resource_id=resource_id_str,
            principal_type=grant.principal_type,
            principal_id=grant.principal_id,
            parent__isnull=True,
        ).first()

        if existing is not None:
            if existing.access_level != grant.access_level:
                existing.access_level = grant.access_level
                existing.save(update_fields=["access_level"])
            written.append(existing)
        else:
            written.append(
                ResourceShare.objects.create(
                    org=org,
                    resource_type=rtype,
                    resource_id=resource_id_str,
                    principal_type=grant.principal_type,
                    principal_id=grant.principal_id,
                    access_level=grant.access_level,
                    created_by=orguser,
                )
            )

    if rtype == ResourceType.DASHBOARD and written:
        dashboard = Dashboard.objects.filter(org=org, pk=resource_id).first()
        if dashboard:
            sync_dashboard_cascade(dashboard)

    # 2) Pending emails — either an Invitation (fresh email) or a direct user
    # grant (existing Dalgo user just added to this org). See _resolve_pending_email.
    for pending in pending_grants:
        kind, resolved_id = _resolve_pending_email(org, orguser, pending.email, invite_role_uuid)

        if kind == "user":
            # Existing Dalgo user — create a direct-user share.
            existing = ResourceShare.objects.filter(
                org=org,
                resource_type=rtype,
                resource_id=resource_id_str,
                principal_type=ResourceSharePrincipalType.USER,
                principal_id=resolved_id,
            ).first()
            if existing is not None:
                if existing.access_level != pending.access_level:
                    existing.access_level = pending.access_level
                    existing.save(update_fields=["access_level"])
                written.append(existing)
            else:
                written.append(
                    ResourceShare.objects.create(
                        org=org,
                        resource_type=rtype,
                        resource_id=resource_id_str,
                        principal_type=ResourceSharePrincipalType.USER,
                        principal_id=resolved_id,
                        access_level=pending.access_level,
                        created_by=orguser,
                    )
                )
            continue

        # kind == "invitation" — invitation-linked share
        existing = ResourceShare.objects.filter(
            org=org,
            resource_type=rtype,
            resource_id=resource_id_str,
            invitation_id=resolved_id,
        ).first()

        if existing is not None:
            if existing.access_level != pending.access_level:
                existing.access_level = pending.access_level
                existing.save(update_fields=["access_level"])
            written.append(existing)
        else:
            written.append(
                ResourceShare.objects.create(
                    org=org,
                    resource_type=rtype,
                    resource_id=resource_id_str,
                    invitation_id=resolved_id,
                    access_level=pending.access_level,
                    created_by=orguser,
                )
            )

    return written, warnings


def update_grant(orguser: OrgUser, share_id: int, access_level: AccessLevel) -> ResourceShare:
    if orguser.org is None:
        raise GrantError("no associated org")

    share = ResourceShare.objects.filter(id=share_id, org=orguser.org).first()
    if share is None:
        raise GrantError("share not found")
    if share.parent_id is not None:
        raise GrantError(
            "cascade-derived access cannot be changed directly; update via the parent dashboard"
        )

    share.access_level = access_level
    share.save(update_fields=["access_level"])

    if share.resource_type == ResourceType.DASHBOARD:
        dashboard = Dashboard.objects.filter(org=orguser.org, pk=share.resource_id).first()
        if dashboard:
            sync_dashboard_cascade(dashboard)

    return share


def remove_grant(orguser: OrgUser, share_id: int) -> None:
    if orguser.org is None:
        raise GrantError("no associated org")
    deleted, _ = ResourceShare.objects.filter(id=share_id, org=orguser.org).delete()
    if deleted == 0:
        raise GrantError("share not found")


# ---------------------------------------------------------------------------
# Internal


def _resolve_pending_email(
    org: Org, orguser: OrgUser, email: str, invite_role_uuid: Optional[str]
) -> tuple:
    """Resolve a pending-share email to either an OrgUser or an Invitation.

    Three cases:
    - Email already belongs to an active OrgUser in this org → invalid; the
      caller should have grouped this into ``principals`` (direct grant).
    - Email belongs to an existing Dalgo User but not this org → ``invite_user_v1``
      creates the OrgUser directly (no invitation needed); returns
      ``('user', orguser_id)`` so the caller can write a direct-user share.
    - Email is brand-new to Dalgo → an ``Invitation`` is created (or reused);
      returns ``('invitation', invitation_id)``.

    Raises ``GrantError`` on validation failures.
    """
    normalized = email.strip().lower()

    if OrgUser.objects.filter(org=org, user__email__iexact=normalized).exists():
        raise GrantError(f"{normalized} is already an active user; grant them directly")

    existing_invitation = Invitation.objects.filter(
        invited_by__org=org, invited_email__iexact=normalized
    ).first()
    if existing_invitation is not None:
        return ("invitation", existing_invitation.id)

    if not invite_role_uuid:
        raise GrantError("invite_role_uuid is required to invite new emails")
    if not Role.objects.filter(uuid=invite_role_uuid).exists():
        raise GrantError("invalid invite_role_uuid")

    _, error = orguserfunctions.invite_user_v1(
        orguser,
        NewInvitationSchema(invited_email=normalized, invited_role_uuid=invite_role_uuid),
    )
    if error:
        raise GrantError(f"failed to invite {normalized}: {error}")

    # invite_user_v1 takes one of two paths internally:
    # - Existing Dalgo User → creates an OrgUser directly (no Invitation row).
    # - Brand-new email → creates an Invitation row.
    # Detect which happened by looking up the OrgUser first.
    new_orguser = OrgUser.objects.filter(org=org, user__email__iexact=normalized).first()
    if new_orguser is not None:
        return ("user", new_orguser.id)

    invitation = Invitation.objects.filter(
        invited_by__org=org, invited_email__iexact=normalized
    ).first()
    if invitation is None:
        raise GrantError(f"could not resolve invitation for {normalized}")
    return ("invitation", invitation.id)


def _check_principal_exists(org: Org, principal_type: str, principal_id: int) -> None:
    if principal_type == ResourceSharePrincipalType.USER:
        if not OrgUser.objects.filter(org=org, id=principal_id).exists():
            raise GrantError(f"user {principal_id} not found in this org")
    elif principal_type == ResourceSharePrincipalType.GROUP:
        if not OrgUserGroup.objects.filter(org=org, id=principal_id).exists():
            raise GrantError(f"group {principal_id} not found in this org")
    else:
        raise GrantError(f"unknown principal_type: {principal_type}")
