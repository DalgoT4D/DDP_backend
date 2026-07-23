"""Business logic for creating, listing, and removing ``ResourceShare`` rows.

Consumers are the ``access_api.py`` endpoints; this module owns the pending-
email flow, dedup rules, and error taxonomy so those endpoints stay thin.

Payload/response shapes live in ``ddpui/schemas/access/resource_share_schema``.
"""

from typing import Optional

from ddpui.core import orguserfunctions
from ddpui.models.org import Org
from ddpui.models.org_user import Invitation, NewInvitationSchema, OrgUser, OrgUserGroup
from ddpui.models.resource_share import ResourceShare, ResourceSharePrincipalType
from ddpui.models.role_based_access import Role
from ddpui.schemas.access.resource_share_schema import (
    AccessLevel,
    PendingGrantPayload,
    PrincipalGrantPayload,
    ShareRowSchema,
)


class GrantError(Exception):
    """Business-rule violation with a user-facing message. The API layer
    catches this and returns 400."""


# ---------------------------------------------------------------------------
# Read


def list_grants(org: Org, rtype: str, resource_id) -> list[ShareRowSchema]:
    """Return the share rows for a resource. Concrete-principal rows are
    resolved to their labels (email or group name); orphan rows (principal
    deleted) are skipped.
    """
    resource_id_str = str(resource_id)
    rows = (
        ResourceShare.objects.filter(org=org, resource_type=rtype, resource_id=resource_id_str)
        .select_related("invitation__invited_new_role")
        .order_by("created_at")
    )

    user_ids = [
        r.principal_id for r in rows if r.principal_type == ResourceSharePrincipalType.USER
    ]
    group_ids = [
        r.principal_id for r in rows if r.principal_type == ResourceSharePrincipalType.GROUP
    ]
    users_by_id = {
        u.id: u
        for u in OrgUser.objects.filter(org=org, id__in=user_ids).select_related(
            "user", "new_role"
        )
    }
    groups_by_id = {g.id: g for g in OrgUserGroup.objects.filter(org=org, id__in=group_ids)}

    shares: list[ShareRowSchema] = []
    for row in rows:
        if (
            row.principal_type == ResourceSharePrincipalType.USER
            and row.principal_id in users_by_id
        ):
            u = users_by_id[row.principal_id]
            shares.append(
                ShareRowSchema(
                    share_id=row.id,
                    principal_type="user",
                    principal_id=row.principal_id,
                    email=u.user.email,
                    label=u.user.email,
                    role_or_group=u.new_role.name if u.new_role else None,
                    access_level=row.access_level,
                    status="active",
                )
            )
        elif (
            row.principal_type == ResourceSharePrincipalType.GROUP
            and row.principal_id in groups_by_id
        ):
            g = groups_by_id[row.principal_id]
            shares.append(
                ShareRowSchema(
                    share_id=row.id,
                    principal_type="group",
                    principal_id=row.principal_id,
                    email=None,
                    label=g.name,
                    role_or_group="Group",
                    access_level=row.access_level,
                    status="active",
                )
            )
        elif row.invitation is not None:
            shares.append(
                ShareRowSchema(
                    share_id=row.id,
                    principal_type="invitation",
                    principal_id=None,
                    email=row.invitation.invited_email,
                    label=row.invitation.invited_email,
                    role_or_group=(
                        row.invitation.invited_new_role.name
                        if row.invitation.invited_new_role
                        else None
                    ),
                    access_level=row.access_level,
                    status="pending",
                )
            )
        # Silently skip orphans (principal deleted, invitation still SET_NULL'd).

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

    share.access_level = access_level
    share.save(update_fields=["access_level"])
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
