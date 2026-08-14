"""
Admin Portal API — cross-org endpoints for the Dalgo ops team.

Every route here is gated by @platform_admin_required (the global
UserAttributes.is_platform_admin flag), not by per-org permission slugs. See
features/admin-portal/v1/plan.md §3 for why cross-org needs its own layer.
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
    AdminPingSchema,
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

# Router-level auth is the API-wide CustomJwtAuthMiddleware (the normal access_token
# cookie), same as every other router. There is no separate admin session: the admin app
# signs in through POST /api/v2/login/ and authority comes from @platform_admin_required
# on each route.
admin_router = Router()


@admin_router.get("/currentuser", response=AdminCurrentUserSchema)
@platform_admin_required
def get_admin_currentuser(request):
    """
    Identity for the admin portal — read by the frontend AdminGuard.

    Authenticated by the shared session cookie, then gated by
    @platform_admin_required, so a signed-in non-admin gets 403 and a signed-out
    visitor gets 401. AdminGuard treats both the same: send them to the admin sign-in.
    Kept as a dedicated route (rather than reusing /api/currentuserv2) because
    currentuserv2 returns a LIST of OrgUsers and is gated on the per-org
    can_view_orgusers permission, which a platform admin need not hold.
    """
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


@admin_router.get("/ping", response=AdminPingSchema)
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
    """
    Create an org. Reuses create_organization (which provisions an Airbyte workspace
    and rolls the Org back if Airbyte fails) + create_org_plan. No OrgUser is attached
    here — the first admin is invited on the Users tab (M4).
    """
    try:
        org = admin_service.create_org(payload)
    except AdminOrgCreateError as err:
        # On Airbyte failure create_org already deleted the Org. On plan failure the Org
        # persists until this @transaction.atomic view unwinds — raising here triggers that
        # rollback, so either way nothing is left behind.
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
# Cross-org user management inside a target org. Every endpoint takes the target
# org in the URL and goes through admin_service (invite_user / change_orguser_role /
# remove_orguser), which delegates to the org-parameterized core functions in
# orguserfunctions with is_platform_admin=True — so the invite-cap and role-cap rules
# that only make sense for an in-org inviter are skipped for a platform admin acting
# cross-org. See plan.md §4.4.


def _get_orguser_or_404(org: Org, orguser_id: int) -> OrgUser:
    """resolve an OrgUser by id, scoped to the target org (404 if it belongs elsewhere)"""
    orguser = admin_service.get_orguser_in_org(org, orguser_id)
    if orguser is None:
        raise HttpError(404, "user not found in this org")
    return orguser


@admin_router.get("/orgs/{org_id}/users", response=AdminOrgUsersResponse)
@platform_admin_required
def get_admin_org_users(request, org_id: int):
    """List an org's users (with per-org Status) plus its pending invitations."""
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
    """
    Invite a user into the org. A platform admin may invite at ANY role — the
    inviter-level cap is skipped (is_platform_admin=True). The invitation records
    invited_in_org=this org, so accept/cancel resolve the right org even though the
    admin is not a member of it.
    """
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
        # is_platform_admin=True skips the role-level cap inside change_orguser_role_in_org
        # — both of its "Insufficient permissions" returns are guarded by `not
        # is_platform_admin` — so a 403 is structurally unreachable on this path. Every
        # error it can still return here ("Invalid role", "User does not exist") is a bad
        # request, so we map to 400 without matching on the (fragile) error string.
        raise HttpError(400, error)

    orguser.refresh_from_db()
    return AdminOrgUserSchema.from_model(orguser)


@admin_router.get("/orgs/{org_id}/users/{orguser_id}/removal-impact", response=RemovalImpactSchema)
@platform_admin_required
def get_admin_org_user_removal_impact(request, org_id: int, orguser_id: int):
    """
    Count the content that removing this user would orphan (its created_by set to NULL —
    the content is kept, not deleted), so the confirm dialog can warn before the action.
    Counts are exact ORM counts on the created_by FK. See plan.md §4.6 / research §5.
    """
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
    """
    Remove a user from the org. This deletes the OrgUser row but ORPHANS the content
    they created rather than deleting it: Dashboard / Chart / ReportSnapshot.created_by
    are SET_NULL, so the content is kept and only the creator link is cleared. The
    role-level cap is skipped for the platform admin. Callers should have shown the
    removal-impact warning first.
    """
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
    """
    Cancel a pending invitation, scoped to the target org. Unlike the regular
    DELETE /users/invitations/delete/{id} (which deletes by id with no org check —
    research §8), this refuses to touch an invitation belonging to another org: the
    filter requires invited_in_org == this org, so a wrong-org id yields 404.
    """
    org = _get_org_or_404(org_id)
    invitation = admin_service.get_invitation_in_org(org, invitation_id)
    if invitation is None:
        raise HttpError(404, "invitation not found in this org")
    admin_service.delete_invitation(invitation)
    logger.info(f"admin cancelled invitation {invitation_id} in org {org.slug}")
    return {"success": 1}
