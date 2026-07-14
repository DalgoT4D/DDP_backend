"""
Admin Portal API — cross-org endpoints for the Dalgo ops team.

Every route here is gated by @platform_admin_required (the global
UserAttributes.is_platform_admin flag), not by per-org permission slugs. See
features/admin-portal/v1/plan.md §3 for why cross-org needs its own layer.
"""

import uuid
from datetime import datetime
from typing import List, Optional

from ninja import Router, Schema
from ninja.errors import HttpError
from pydantic import HttpUrl
from django.db import transaction

from ddpui.auth import platform_admin_required
from ddpui.models.org import Org
from ddpui.models.org_user import (
    OrgUser,
    Invitation,
    NewInvitationSchema,
    DeleteOrgUserPayload,
)
from ddpui.models.org_plans import OrgPlans, OrgPlanType
from ddpui.models.dashboard import Dashboard
from ddpui.models.visualization import Chart
from ddpui.models.report import ReportSnapshot
from ddpui.schemas.org_schema import CreateOrgSchema
from ddpui.core import orgfunctions, orguserfunctions
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

admin_router = Router()


class AdminStatsSchema(Schema):
    """platform-wide counts for the admin dashboard"""

    total_orgs: int
    total_users: int


class AdminOrgSchema(Schema):
    """an org as shown in the admin portal"""

    id: int
    name: str
    slug: str | None
    viz_url: str | None
    base_plan: str | None
    is_active: bool
    user_count: int


class AdminCreateOrgSchema(Schema):
    """payload to create an org from the admin portal (slug is derived from name)"""

    name: str
    viz_url: Optional[HttpUrl] = None
    base_plan: str = OrgPlanType.FREE_TRIAL.value
    superset_included: bool = False
    can_upgrade_plan: bool = True
    subscription_duration: str = "Monthly"


class AdminUpdateOrgSchema(Schema):
    """
    payload to edit an org. slug is intentionally absent — it is locked post-create
    because it is used in URLs and the Airbyte workspace (plan.md §8 #4).
    """

    name: Optional[str] = None
    viz_url: Optional[HttpUrl] = None
    base_plan: Optional[str] = None


def _admin_org_response(org: Org) -> AdminOrgSchema:
    return AdminOrgSchema(
        id=org.id,
        name=org.name,
        slug=org.slug,
        viz_url=org.viz_url,
        base_plan=org.base_plan(),
        is_active=org.is_active,
        user_count=OrgUser.objects.filter(org=org).count(),
    )


@admin_router.get("/ping")
@platform_admin_required
def get_admin_ping(request):
    """
    Stub health check for the admin portal — proves the platform-admin gate works.
    Returns 200 for platform admins; @platform_admin_required 403s everyone else.
    """
    return {"detail": "pong"}


@admin_router.get("/stats", response=AdminStatsSchema)
@platform_admin_required
def get_admin_stats(request):
    """
    Dashboard counts: total orgs and total users across the whole platform.

    total_users counts distinct users who belong to at least one org (via OrgUser),
    consistent with total_orgs being real orgs — not every User row.
    """
    return AdminStatsSchema(
        total_orgs=Org.objects.count(),
        total_users=OrgUser.objects.values("user").distinct().count(),
    )


@admin_router.get("/orgs", response=List[AdminOrgSchema])
@platform_admin_required
def get_admin_orgs(request):
    """List every org (active and inactive) with its user count."""
    return [_admin_org_response(org) for org in Org.objects.all().order_by("name")]


@admin_router.post("/orgs", response=AdminOrgSchema)
@platform_admin_required
@transaction.atomic
def post_admin_org(request, payload: AdminCreateOrgSchema):
    """
    Create an org. Reuses create_organization (which provisions an Airbyte workspace
    and rolls the Org back if Airbyte fails) + create_org_plan. No OrgUser is attached
    here — the first admin is invited on the Users tab (M4).
    """
    create_payload = CreateOrgSchema(
        name=payload.name,
        viz_url=payload.viz_url,
        base_plan=payload.base_plan,
        can_upgrade_plan=payload.can_upgrade_plan,
        subscription_duration=payload.subscription_duration,
        superset_included=payload.superset_included,
    )
    org, error = orgfunctions.create_organization(create_payload)
    if error:
        # create_organization already deleted the org on Airbyte failure; nothing persists.
        raise HttpError(400, error)

    _, plan_error = orgfunctions.create_org_plan(create_payload, org)
    if plan_error:
        raise HttpError(400, plan_error)

    logger.info(f"admin created new org {org.name}")
    return _admin_org_response(org)


@admin_router.get("/orgs/{org_id}", response=AdminOrgSchema)
@platform_admin_required
def get_admin_org(request, org_id: int):
    """Org detail (Overview facts)."""
    org = Org.objects.filter(id=org_id).first()
    if org is None:
        raise HttpError(404, "org not found")
    return _admin_org_response(org)


@admin_router.put("/orgs/{org_id}", response=AdminOrgSchema)
@platform_admin_required
def put_admin_org(request, org_id: int, payload: AdminUpdateOrgSchema):
    """Edit an org's name / viz_url / base_plan. slug is never touched (locked)."""
    org = Org.objects.filter(id=org_id).first()
    if org is None:
        raise HttpError(404, "org not found")

    if payload.name is not None:
        org.name = payload.name
    if payload.viz_url is not None:
        org.viz_url = str(payload.viz_url)
    org.save()  # slug intentionally excluded from the update

    if payload.base_plan is not None:
        org_plans = OrgPlans.objects.filter(org=org).first()
        if org_plans:
            org_plans.base_plan = payload.base_plan
            org_plans.save()

    return _admin_org_response(org)


@admin_router.post("/orgs/{org_id}/deactivate", response=AdminOrgSchema)
@platform_admin_required
def post_admin_org_deactivate(request, org_id: int):
    """Deactivate an org (reversible). Its users are then blocked at permission-load."""
    org = Org.objects.filter(id=org_id).first()
    if org is None:
        raise HttpError(404, "org not found")
    org.is_active = False
    org.save()
    logger.info(f"admin deactivated org {org.slug}")
    return _admin_org_response(org)


@admin_router.post("/orgs/{org_id}/reactivate", response=AdminOrgSchema)
@platform_admin_required
def post_admin_org_reactivate(request, org_id: int):
    """Reactivate a deactivated org — its users can use the app again."""
    org = Org.objects.filter(id=org_id).first()
    if org is None:
        raise HttpError(404, "org not found")
    org.is_active = True
    org.save()
    logger.info(f"admin reactivated org {org.slug}")
    return _admin_org_response(org)


# ======================= Users tab (M4) ======================================
# Cross-org user management inside a target org. Every endpoint takes the target
# org in the URL and reuses the org-parameterized core functions in
# orguserfunctions (invite_user_to_org / change_orguser_role_in_org /
# delete_orguser_from_org) with is_platform_admin=True, so the invite-cap and
# role-cap rules that only make sense for an in-org inviter are skipped for a
# platform admin acting cross-org. See plan.md §4.4.


class AdminOrgUserSchema(Schema):
    """a user within an org, as shown in the admin portal Users tab"""

    orguser_id: int
    email: str
    new_role_slug: str | None
    # per-org active flag (OrgUser.is_active) — NOT the global User.is_active
    is_active: bool


class AdminInvitationSchema(Schema):
    """a pending invitation within an org (a row that has not been accepted)"""

    id: int
    invited_email: str
    invited_role_slug: str | None
    invited_on: datetime


class AdminOrgUsersResponse(Schema):
    """the Users tab payload: current members plus pending invites"""

    users: List[AdminOrgUserSchema]
    invitations: List[AdminInvitationSchema]


class AdminInviteUserSchema(Schema):
    """payload to invite a user into an org from the admin portal"""

    invited_email: str
    invited_role_uuid: uuid.UUID


class AdminChangeRoleSchema(Schema):
    """payload to change an org user's role"""

    role_uuid: uuid.UUID


class RemovalImpactSchema(Schema):
    """
    what removing a user would destroy. Drives the confirm dialog's warning.
    Dashboard/Chart created_by are CASCADE (deleted); ReportSnapshot is SET_NULL
    (orphaned, kept). See research §5.
    """

    dashboards_deleted: int
    charts_deleted: int
    reports_orphaned: int


def _get_org_or_404(org_id: int) -> Org:
    org = Org.objects.filter(id=org_id).first()
    if org is None:
        raise HttpError(404, "org not found")
    return org


def _get_orguser_or_404(org: Org, orguser_id: int) -> OrgUser:
    """resolve an OrgUser by id, scoped to the target org (404 if it belongs elsewhere)"""
    orguser = OrgUser.objects.filter(id=orguser_id, org=org).first()
    if orguser is None:
        raise HttpError(404, "user not found in this org")
    return orguser


@admin_router.get("/orgs/{org_id}/users", response=AdminOrgUsersResponse)
@platform_admin_required
def get_admin_org_users(request, org_id: int):
    """List an org's users (with per-org Status) plus its pending invitations."""
    org = _get_org_or_404(org_id)

    users = [
        AdminOrgUserSchema(
            orguser_id=ou.id,
            email=ou.user.email,
            new_role_slug=ou.new_role.slug if ou.new_role else None,
            is_active=ou.is_active,
        )
        for ou in OrgUser.objects.filter(org=org).select_related("user", "new_role")
    ]

    # pending invites are scoped by the explicit target org, so an invite a platform
    # admin created into this org shows here even though invited_by is in another org.
    invitations = [
        AdminInvitationSchema(
            id=inv.id,
            invited_email=inv.invited_email,
            invited_role_slug=inv.invited_new_role.slug if inv.invited_new_role else None,
            invited_on=inv.invited_on,
        )
        for inv in Invitation.objects.filter(invited_in_org=org).select_related(
            "invited_new_role"
        )
    ]

    return AdminOrgUsersResponse(users=users, invitations=invitations)


@admin_router.post("/orgs/{org_id}/users/invite", response=AdminInvitationSchema)
@platform_admin_required
def post_admin_org_user_invite(request, org_id: int, payload: AdminInviteUserSchema):
    """
    Invite a user into the org. A platform admin may invite at ANY role — the
    inviter-level cap is skipped (is_platform_admin=True). The invitation records
    invited_in_org=this org, so accept/cancel resolve the right org even though the
    admin is not a member of it.
    """
    org = _get_org_or_404(org_id)

    invite_payload = NewInvitationSchema(
        invited_email=payload.invited_email,
        invited_role_uuid=payload.invited_role_uuid,
    )
    _, error = orguserfunctions.invite_user_to_org(
        org, request.orguser, invite_payload, is_platform_admin=True
    )
    if error:
        raise HttpError(400, error)

    invitation = (
        Invitation.objects.filter(
            invited_email__iexact=payload.invited_email.lower().strip(),
            invited_in_org=org,
        )
        .select_related("invited_new_role")
        .first()
    )
    if invitation is None:
        # the invitee already had a platform account, so they were added directly as an
        # OrgUser (no pending Invitation row). Report that with a 200-style stub.
        return AdminInvitationSchema(
            id=0,
            invited_email=payload.invited_email.lower().strip(),
            invited_role_slug=None,
            invited_on=datetime.now(),
        )

    logger.info(f"admin invited {invitation.invited_email} into org {org.slug}")
    return AdminInvitationSchema(
        id=invitation.id,
        invited_email=invitation.invited_email,
        invited_role_slug=(
            invitation.invited_new_role.slug if invitation.invited_new_role else None
        ),
        invited_on=invitation.invited_on,
    )


@admin_router.put("/orgs/{org_id}/users/{orguser_id}/role", response=AdminOrgUserSchema)
@platform_admin_required
def put_admin_org_user_role(request, org_id: int, orguser_id: int, payload: AdminChangeRoleSchema):
    """Change a user's role in the org. Role-level cap skipped for the platform admin."""
    org = _get_org_or_404(org_id)
    orguser = _get_orguser_or_404(org, orguser_id)

    _, error = orguserfunctions.change_orguser_role_in_org(
        org, request.orguser, orguser.user.email, payload.role_uuid, is_platform_admin=True
    )
    if error:
        raise HttpError(403 if error == "Insufficient permissions" else 400, error)

    orguser.refresh_from_db()
    return AdminOrgUserSchema(
        orguser_id=orguser.id,
        email=orguser.user.email,
        new_role_slug=orguser.new_role.slug if orguser.new_role else None,
        is_active=orguser.is_active,
    )


@admin_router.post("/orgs/{org_id}/users/{orguser_id}/deactivate", response=AdminOrgUserSchema)
@platform_admin_required
def post_admin_org_user_deactivate(request, org_id: int, orguser_id: int):
    """
    Deactivate a user in THIS org only (sets OrgUser.is_active=False, NOT
    User.is_active). Blocked at permission-load for this org; the user's membership
    of any other org is unaffected. See plan.md §4.1 / §4.2.
    """
    org = _get_org_or_404(org_id)
    orguser = _get_orguser_or_404(org, orguser_id)
    orguser.is_active = False
    orguser.save(update_fields=["is_active"])
    logger.info(f"admin deactivated {orguser.user.email} in org {org.slug}")
    return AdminOrgUserSchema(
        orguser_id=orguser.id,
        email=orguser.user.email,
        new_role_slug=orguser.new_role.slug if orguser.new_role else None,
        is_active=orguser.is_active,
    )


@admin_router.post("/orgs/{org_id}/users/{orguser_id}/reactivate", response=AdminOrgUserSchema)
@platform_admin_required
def post_admin_org_user_reactivate(request, org_id: int, orguser_id: int):
    """Reactivate a user in this org (OrgUser.is_active=True)."""
    org = _get_org_or_404(org_id)
    orguser = _get_orguser_or_404(org, orguser_id)
    orguser.is_active = True
    orguser.save(update_fields=["is_active"])
    logger.info(f"admin reactivated {orguser.user.email} in org {org.slug}")
    return AdminOrgUserSchema(
        orguser_id=orguser.id,
        email=orguser.user.email,
        new_role_slug=orguser.new_role.slug if orguser.new_role else None,
        is_active=orguser.is_active,
    )


@admin_router.get(
    "/orgs/{org_id}/users/{orguser_id}/removal-impact", response=RemovalImpactSchema
)
@platform_admin_required
def get_admin_org_user_removal_impact(request, org_id: int, orguser_id: int):
    """
    Count the content that removing this user would cascade-delete, so the confirm
    dialog can warn before the destructive action. Counts are exact ORM counts on the
    created_by FK. See plan.md §4.6 / research §5.
    """
    org = _get_org_or_404(org_id)
    orguser = _get_orguser_or_404(org, orguser_id)
    return RemovalImpactSchema(
        dashboards_deleted=Dashboard.objects.filter(created_by=orguser).count(),
        charts_deleted=Chart.objects.filter(created_by=orguser).count(),
        reports_orphaned=ReportSnapshot.objects.filter(created_by=orguser).count(),
    )


@admin_router.delete("/orgs/{org_id}/users/{orguser_id}")
@platform_admin_required
def delete_admin_org_user(request, org_id: int, orguser_id: int):
    """
    Remove a user from the org (hard delete; cascades their Dashboards/Charts). The
    role-level cap is skipped for the platform admin. Callers should have shown the
    removal-impact warning first.
    """
    org = _get_org_or_404(org_id)
    orguser = _get_orguser_or_404(org, orguser_id)

    _, error = orguserfunctions.delete_orguser_from_org(
        org,
        request.orguser,
        DeleteOrgUserPayload(email=orguser.user.email),
        is_platform_admin=True,
    )
    if error:
        raise HttpError(400, error)

    logger.info(f"admin removed {orguser.user.email} from org {org.slug}")
    return {"success": 1}


@admin_router.delete("/orgs/{org_id}/invitations/{invitation_id}")
@platform_admin_required
def delete_admin_org_invitation(request, org_id: int, invitation_id: int):
    """
    Cancel a pending invitation, scoped to the target org. Unlike the regular
    DELETE /users/invitations/delete/{id} (which deletes by id with no org check —
    research §8), this refuses to touch an invitation belonging to another org: the
    filter requires invited_in_org == this org, so a wrong-org id yields 404.
    """
    org = _get_org_or_404(org_id)
    invitation = Invitation.objects.filter(id=invitation_id, invited_in_org=org).first()
    if invitation is None:
        raise HttpError(404, "invitation not found in this org")
    invitation.delete()
    logger.info(f"admin cancelled invitation {invitation_id} in org {org.slug}")
    return {"success": 1}
