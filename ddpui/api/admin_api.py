"""
Admin Portal API — cross-org endpoints for the Dalgo ops team, gated by
@platform_admin_required rather than per-org permission slugs.
"""

from typing import List

from ninja import Router
from ninja.errors import HttpError
from django.db import transaction
from django.utils import timezone

from ddpui.auth import platform_admin_required
from ddpui.models.org import Org
from ddpui.models.org_user import (
    OrgUser,
    NewInvitationSchema,
)
from ddpui.schemas.admin_schema import (
    AdminCurrentUserSchema,
    AdminSuccessSchema,
    AdminStatsSchema,
    AdminOrgSchema,
    AdminCreateOrgSchema,
    AdminUpdateOrgSchema,
    AdminOrgUserSchema,
    AdminInvitationSchema,
    AdminOrgUsersResponse,
    AdminChangeRoleSchema,
    RemovalImpactSchema,
)
from ddpui.core.admin import admin_service
from ddpui.core.admin.exceptions import AdminOrgCreateError
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

# No separate admin session: signs in through the normal POST /api/v2/login/, and
# authority comes from @platform_admin_required on each route.
admin_router = Router()


@admin_router.get("/currentuser", response=AdminCurrentUserSchema)
@platform_admin_required
def get_admin_currentuser(request):
    """Identity for the admin portal — read by the frontend AdminGuard. A dedicated
    route rather than /api/currentuserv2, which is gated on a per-org permission."""
    user = request.orguser.user
    return {"email": user.email, "is_platform_admin": True}


def _get_org_or_404(org_id: int) -> Org:
    """resolve the target org from the URL (404 if it does not exist)"""
    org = admin_service.get_org(org_id)
    if org is None:
        raise HttpError(404, "org not found")
    return org


def _admin_org_response(org: Org) -> AdminOrgSchema:
    # the schema owns the field mapping (from_model); the API supplies the user_count,
    # which needs a service call and so isn't the schema's to compute.
    return AdminOrgSchema.from_model(org, admin_service.org_user_count(org))


@admin_router.get("/stats", response=AdminStatsSchema)
@platform_admin_required
def get_admin_stats(request):
    """Dashboard counts: total orgs and total users across the whole platform."""
    total_orgs, total_users = admin_service.get_platform_stats()
    return AdminStatsSchema(total_orgs=total_orgs, total_users=total_users)


@admin_router.get("/orgs", response=List[AdminOrgSchema])
@platform_admin_required
def get_admin_orgs(request):
    """List every org (active and inactive) with its user count."""
    return [_admin_org_response(org) for org in admin_service.list_orgs()]


@admin_router.post("/orgs", response=AdminOrgSchema)
@platform_admin_required
@transaction.atomic
def post_admin_org(request, payload: AdminCreateOrgSchema):
    """Create an org + plan. No OrgUser is attached — the first admin is invited on the
    Users tab."""
    try:
        org = admin_service.create_org(payload)
    except AdminOrgCreateError as err:
        # @transaction.atomic rolls back the Org row; create_org already cleaned up any
        # Airbyte workspace on the failure path that provisioned one.
        raise HttpError(400, err.message) from err

    return _admin_org_response(org)


@admin_router.get("/orgs/{org_id}", response=AdminOrgSchema)
@platform_admin_required
def get_admin_org(request, org_id: int):
    """Org detail (Overview facts)."""
    org = _get_org_or_404(org_id)
    return _admin_org_response(org)


@admin_router.put("/orgs/{org_id}", response=AdminOrgSchema)
@platform_admin_required
def put_admin_org(request, org_id: int, payload: AdminUpdateOrgSchema):
    """Edit an org's name / viz_url / base_plan. slug is never touched (locked)."""
    org = _get_org_or_404(org_id)
    org = admin_service.update_org(org, payload)
    return _admin_org_response(org)


# ======================= Users tab (M4) ======================================
# Cross-org user management: every endpoint takes the target org in the URL and goes
# through admin_service with is_platform_admin=True, skipping the inviter/role caps
# that only make sense for an in-org actor.


def _get_orguser_or_404(org: Org, orguser_id: int) -> OrgUser:
    """resolve an OrgUser by id, scoped to the target org (404 if it belongs elsewhere)"""
    orguser = admin_service.get_orguser_in_org(org, orguser_id)
    if orguser is None:
        raise HttpError(404, "user not found in this org")
    return orguser


@admin_router.get("/orgs/{org_id}/users", response=AdminOrgUsersResponse)
@platform_admin_required
def get_admin_org_users(request, org_id: int):
    """List an org's users plus its pending invitations."""
    org = _get_org_or_404(org_id)

    users = [AdminOrgUserSchema.from_model(ou) for ou in admin_service.list_org_users(org)]

    # pending invites are scoped by the explicit target org, so an invite a platform
    # admin created into this org shows here even though invited_by is in another org.
    invitations = [
        AdminInvitationSchema.from_model(inv) for inv in admin_service.list_org_invitations(org)
    ]

    return AdminOrgUsersResponse(users=users, invitations=invitations)


@admin_router.post("/orgs/{org_id}/users/invite", response=AdminInvitationSchema)
@platform_admin_required
def post_admin_org_user_invite(request, org_id: int, payload: NewInvitationSchema):
    """Invite a user into the org at any role — the inviter-level cap is skipped for a
    platform admin."""
    org = _get_org_or_404(org_id)

    _, error = admin_service.invite_user(org, request.orguser, payload)
    if error:
        raise HttpError(400, error)

    invitation = admin_service.get_pending_invitation(org, payload.invited_email)
    if invitation is None:
        # the invitee already had a platform account, so they were added directly as an
        # OrgUser (no pending Invitation row). Report that with a 200-style stub.
        return AdminInvitationSchema(
            id=0,
            invited_email=payload.invited_email.lower().strip(),
            invited_role_slug=None,
            invited_on=timezone.now(),
        )

    logger.info(f"admin invited {invitation.invited_email} into org {org.slug}")
    return AdminInvitationSchema.from_model(invitation)


@admin_router.put("/orgs/{org_id}/users/{orguser_id}/role", response=AdminOrgUserSchema)
@platform_admin_required
def put_admin_org_user_role(request, org_id: int, orguser_id: int, payload: AdminChangeRoleSchema):
    """Change a user's role in the org. Role-level cap skipped for the platform admin."""
    org = _get_org_or_404(org_id)
    orguser = _get_orguser_or_404(org, orguser_id)

    _, error = admin_service.change_orguser_role(org, request.orguser, orguser, payload.role_uuid)
    if error:
        # 403 is unreachable here (role-cap is skipped for is_platform_admin=True); any
        # remaining error is a bad request.
        raise HttpError(400, error)

    orguser.refresh_from_db()
    return AdminOrgUserSchema.from_model(orguser)


@admin_router.get("/orgs/{org_id}/users/{orguser_id}/removal-impact", response=RemovalImpactSchema)
@platform_admin_required
def get_admin_org_user_removal_impact(request, org_id: int, orguser_id: int):
    """Count the content removing this user would orphan, so the confirm dialog can
    warn before the action."""
    org = _get_org_or_404(org_id)
    orguser = _get_orguser_or_404(org, orguser_id)
    dashboards_orphaned, charts_orphaned, reports_orphaned = admin_service.removal_impact(orguser)
    return RemovalImpactSchema(
        dashboards_orphaned=dashboards_orphaned,
        charts_orphaned=charts_orphaned,
        reports_orphaned=reports_orphaned,
    )


@admin_router.delete("/orgs/{org_id}/users/{orguser_id}", response=AdminSuccessSchema)
@platform_admin_required
def delete_admin_org_user(request, org_id: int, orguser_id: int):
    """Remove a user from the org. Their created content is orphaned (created_by
    SET_NULL), not deleted. Callers should show the removal-impact warning first."""
    org = _get_org_or_404(org_id)
    orguser = _get_orguser_or_404(org, orguser_id)

    _, error = admin_service.remove_orguser(org, request.orguser, orguser)
    if error:
        raise HttpError(400, error)

    logger.info(f"admin removed {orguser.user.email} from org {org.slug}")
    return {"success": 1}


@admin_router.delete("/orgs/{org_id}/invitations/{invitation_id}", response=AdminSuccessSchema)
@platform_admin_required
def delete_admin_org_invitation(request, org_id: int, invitation_id: int):
    """Cancel a pending invitation, scoped to the target org (404 on a wrong-org id)."""
    org = _get_org_or_404(org_id)
    invitation = admin_service.get_invitation_in_org(org, invitation_id)
    if invitation is None:
        raise HttpError(404, "invitation not found in this org")
    admin_service.delete_invitation(invitation)
    logger.info(f"admin cancelled invitation {invitation_id} in org {org.slug}")
    return {"success": 1}
