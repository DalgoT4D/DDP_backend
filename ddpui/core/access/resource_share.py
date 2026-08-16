"""Business logic for creating, listing, and removing ``ResourceShare`` rows.

Consumers are the ``access_api.py`` endpoints; this module owns the pending-
email flow, dedup rules, and error taxonomy so those endpoints stay thin.

Payload/response shapes live in ``ddpui/schemas/access/resource_share_schema``.
"""

from typing import Optional

from ddpui.core import orguserfunctions
from ddpui.models.dashboard import Dashboard
from ddpui.models.org import Org
from ddpui.models.org_user import Invitation, NewInvitationSchema, OrgUser, OrgUserGroup
from ddpui.models.resource_share import (
    LEVEL_RANK,
    ResourceShare,
    ResourceSharePrincipalType,
    ResourceType,
)
from ddpui.models.role_based_access import Role
from ddpui.schemas.access.resource_share_schema import (
    AccessLevel,
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
    """
    inner = _parse_inner_ids(dashboard.tabs)
    current_chart_ids = [str(cid) for cid in inner["chart_ids"]]
    current_kpi_ids = [str(kid) for kid in inner["kpi_ids"]]

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
        ResourceShare.objects.filter(parent=share).update(access_level=share.access_level)

        for chart_id in current_chart_ids:
            ResourceShare.objects.get_or_create(
                org=dashboard.org,
                resource_type=ResourceType.CHART,
                resource_id=chart_id,
                principal_type=share.principal_type,
                principal_id=share.principal_id,
                parent=share,
                defaults={"access_level": share.access_level, "created_by": share.created_by},
            )
        for kpi_id in current_kpi_ids:
            ResourceShare.objects.get_or_create(
                org=dashboard.org,
                resource_type=ResourceType.KPI,
                resource_id=kpi_id,
                principal_type=share.principal_type,
                principal_id=share.principal_id,
                parent=share,
                defaults={"access_level": share.access_level, "created_by": share.created_by},
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
    """
    resource_id_str = str(resource_id)
    rows = list(
        ResourceShare.objects.filter(org=org, resource_type=rtype, resource_id=resource_id_str)
        .select_related("invitation__invited_new_role")
        .order_by("created_at")
    )

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
) -> list[ResourceShare]:
    """Idempotent multi-add.

    Existing rows for the same (org, rtype, resource_id, principal_type,
    principal_id) get their ``access_level`` updated in-place — the modal
    treats staged chips as "make this the level"; it does not create
    duplicate rows.
    """
    if orguser.org is None:
        raise GrantError("no associated org")
    org = orguser.org
    resource_id_str = str(resource_id)

    written: list[ResourceShare] = []

    # 1) Concrete principals — user or group.
    for grant in principals:
        _check_principal_exists(org, grant.principal_type, grant.principal_id)

        existing = ResourceShare.objects.filter(
            org=org,
            resource_type=rtype,
            resource_id=resource_id_str,
            principal_type=grant.principal_type,
            principal_id=grant.principal_id,
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

    # 2) Pending emails — create/reuse Invitation, store share pointing at it.
    for pending in pending_grants:
        invitation_id = _resolve_pending_email_to_invitation_id(
            org, orguser, pending.email, invite_role_uuid
        )

        existing = ResourceShare.objects.filter(
            org=org,
            resource_type=rtype,
            resource_id=resource_id_str,
            invitation_id=invitation_id,
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
                    invitation_id=invitation_id,
                    access_level=pending.access_level,
                    created_by=orguser,
                )
            )

    return written


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


def _resolve_pending_email_to_invitation_id(
    org: Org, orguser: OrgUser, email: str, invite_role_uuid: Optional[str]
) -> int:
    """Ensure an ``Invitation`` exists for ``email`` on this org; return its id.

    Reuses an existing pending invitation when present; otherwise creates
    one via ``invite_user_v1`` using ``invite_role_uuid`` (required for new
    invitations). Raises ``GrantError`` on validation failures.
    """
    normalized = email.strip().lower()

    if OrgUser.objects.filter(org=org, user__email__iexact=normalized).exists():
        raise GrantError(f"{normalized} is already an active user; grant them directly")

    existing = Invitation.objects.filter(
        invited_by__org=org, invited_email__iexact=normalized
    ).first()
    if existing is not None:
        return existing.id

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

    invitation = Invitation.objects.filter(
        invited_by__org=org, invited_email__iexact=normalized
    ).first()
    if invitation is None:
        raise GrantError(f"could not resolve invitation for {normalized}")
    return invitation.id


def _check_principal_exists(org: Org, principal_type: str, principal_id: int) -> None:
    if principal_type == ResourceSharePrincipalType.USER:
        if not OrgUser.objects.filter(org=org, id=principal_id).exists():
            raise GrantError(f"user {principal_id} not found in this org")
    elif principal_type == ResourceSharePrincipalType.GROUP:
        if not OrgUserGroup.objects.filter(org=org, id=principal_id).exists():
            raise GrantError(f"group {principal_id} not found in this org")
    else:
        raise GrantError(f"unknown principal_type: {principal_type}")
