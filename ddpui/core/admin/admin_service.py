"""
Admin Portal service layer — the ORM/business logic behind the cross-org admin
endpoints (ddpui/api/admin_api.py).

The admin_api handlers stay thin (parse -> call service -> convert to response
schema -> return); everything that touches the ORM or mutates state lives here,
mirroring how the invite / role-change / remove paths already delegate to
orguserfunctions. This module deliberately knows nothing about HTTP or the
admin_api response schemas — it returns models / primitive values, and the API
layer builds the response and maps errors to status codes.
"""

from datetime import timedelta
from typing import List, Optional, Tuple

from django.conf import settings
from django.contrib.auth import authenticate
from rest_framework_simplejwt.exceptions import TokenError
from rest_framework_simplejwt.tokens import RefreshToken

from ddpui.auth import CustomTokenObtainSerializer
from ddpui.core.admin.exceptions import (
    AdminInvalidCredentialsError,
    AdminNotPlatformAdminError,
    AdminSessionError,
    AdminOrgCreateError,
)
from ddpui.utils.redis_client import RedisClient
from ddpui.models.org import Org
from ddpui.models.org_user import (
    OrgUser,
    Invitation,
    UserAttributes,
    LoginPayload,
    NewInvitationSchema,
    DeleteOrgUserPayload,
)
from ddpui.models.org_plans import OrgPlans
from ddpui.models.dashboard import Dashboard
from ddpui.models.visualization import Chart
from ddpui.models.report import ReportSnapshot
from ddpui.schemas.org_schema import CreateOrgSchema
from ddpui.schemas.admin_schema import AdminCreateOrgSchema, AdminUpdateOrgSchema
from ddpui.core import orgfunctions, orguserfunctions
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.admin")


# --------------------------------------------------------------------------- #
# Independent admin session
# --------------------------------------------------------------------------- #


def issue_admin_session(payload: LoginPayload) -> dict:
    """
    Verify credentials AND platform-admin privilege, then mint a distinct admin token.
    Takes the existing LoginPayload request payload (not loose username/password args), per
    the service-signature convention — the admin sign-in body is the same
    {username, password} shape as the normal login, so it reuses that schema rather than
    duplicating it. Returns {"access": ..., "refresh": ...} on success.
    Raises AdminInvalidCredentialsError (wrong username/password) or
    AdminNotPlatformAdminError (valid creds but not a platform admin) so the API can map
    the exception TYPE to a status code without inspecting the message. Knows nothing
    about HTTP.
    """
    user = authenticate(username=payload.username, password=payload.password)
    if user is None:
        raise AdminInvalidCredentialsError()

    user_attributes = UserAttributes.objects.filter(user=user).first()
    if not (user_attributes and user_attributes.is_platform_admin):
        raise AdminNotPlatformAdminError()

    # A distinct admin token: the session="admin" claim is what AdminJwtAuthMiddleware
    # requires, so a normal login token (which lacks it) can never satisfy the admin API.
    # Setting the claim on the refresh token before deriving the access token propagates
    # it to both.
    refresh = CustomTokenObtainSerializer.get_token(user)
    refresh["session"] = "admin"
    refresh.set_exp(lifetime=timedelta(hours=settings.JWT_ADMIN_REFRESH_TOKEN_EXPIRY_HOURS))
    access = refresh.access_token
    access.set_exp(lifetime=timedelta(minutes=settings.JWT_ADMIN_ACCESS_TOKEN_EXPIRY_MINUTES))

    return {"access": str(access), "refresh": str(refresh)}


def refresh_admin_session(refresh_token: str) -> dict:
    """
    Mint a fresh admin access token from an admin refresh token, preserving the
    session="admin" claim and the short admin access lifetime. Returns {"access": ...}
    on success. Raises AdminSessionError when the token is unreadable, is not an admin
    session, or was blacklisted by logout. This function knows nothing about HTTP.
    """
    try:
        refresh = RefreshToken(refresh_token)
    except TokenError as err:
        raise AdminSessionError("Invalid token") from err

    # A normal refresh token lacks the claim, so it can never be upgraded into an
    # admin session here.
    if refresh.payload.get("session") != "admin":
        raise AdminSessionError("not an admin session")

    jti = refresh.payload.get("jti")
    if jti and RedisClient.get_instance().get(f"blacklisted_jti:{jti}"):
        raise AdminSessionError("Refresh token has been invalidated")

    access = refresh.access_token
    access.set_exp(lifetime=timedelta(minutes=settings.JWT_ADMIN_ACCESS_TOKEN_EXPIRY_MINUTES))

    return {"access": str(access)}


# --------------------------------------------------------------------------- #
# Dashboard / org reads
# --------------------------------------------------------------------------- #


def get_platform_stats() -> Tuple[int, int]:
    """
    (total_orgs, total_users) across the whole platform.

    total_users counts distinct users who belong to at least one org (via OrgUser),
    consistent with total_orgs being real orgs — not every User row.
    """
    total_orgs = Org.objects.count()
    total_users = OrgUser.objects.values("user").distinct().count()
    return total_orgs, total_users


def list_orgs() -> List[Org]:
    """Every org (active and inactive), ordered by name."""
    return list(Org.objects.all().order_by("name"))


def get_org(org_id: int) -> Optional[Org]:
    """Resolve an org by id, or None. The API layer maps None -> 404."""
    return Org.objects.filter(id=org_id).first()


def org_user_count(org: Org) -> int:
    """How many OrgUsers belong to this org."""
    return OrgUser.objects.filter(org=org).count()


# --------------------------------------------------------------------------- #
# Org create / update / (de)activate
# --------------------------------------------------------------------------- #


def create_org(payload: AdminCreateOrgSchema) -> Org:
    """
    Create an org and its plan. Takes the AdminCreateOrgSchema the API validated — the
    widening to the full CreateOrgSchema that orgfunctions expects happens HERE, not in
    the handler, so the API layer stays parse -> call service -> respond. The admin
    payload deliberately omits slug / airbyte_workspace_id / is_demo etc.; those keep
    CreateOrgSchema's own defaults (slug is derived downstream from name).

    Reuses orgfunctions.create_organization (which provisions an Airbyte workspace and
    rolls the Org back if Airbyte fails) plus create_org_plan. No OrgUser is attached
    here — the first admin is invited on the Users tab (M4). Returns the Org on success;
    raises AdminOrgCreateError on failure so the API maps the exception TYPE to 400.

    Rollback differs by failure point: on Airbyte failure create_organization has
    already deleted the Org itself, so nothing persists. On PLAN failure the Org row
    DOES persist here — this function just raises — and it is the caller's
    @transaction.atomic (post_admin_org) that rolls the Org back. See the M16 test.
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
        raise AdminOrgCreateError(error)

    _, plan_error = orgfunctions.create_org_plan(create_payload, org)
    if plan_error:
        # the Org persists at this point — the caller's @transaction.atomic rolls it back.
        raise AdminOrgCreateError(plan_error)

    logger.info(f"admin created new org {org.name}")
    return org


def update_org(org: Org, payload: AdminUpdateOrgSchema) -> Org:
    """
    Partial-update an org's name / viz_url / base_plan from the AdminUpdateOrgSchema
    payload (not loose kwargs), per the service-signature convention. slug is never
    touched — it is locked post-create because it is used in URLs and the Airbyte
    workspace. Only fields passed as non-None are changed. base_plan lives on OrgPlans,
    not Org. viz_url is a pydantic HttpUrl on the schema, coerced to str for the model.
    """
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

    return org


def set_org_active(org: Org, is_active: bool) -> Org:
    """
    Deactivate (reversible) or reactivate an org. A deactivated org blocks all of its
    users at permission-load; reactivating restores their access.
    """
    org.is_active = is_active
    org.save()
    logger.info("admin %s org %s", "reactivated" if is_active else "deactivated", org.slug)
    return org


# --------------------------------------------------------------------------- #
# Org users + invitations
# --------------------------------------------------------------------------- #


def invite_user(
    org: Org, inviter: OrgUser, payload: NewInvitationSchema
) -> Tuple[Optional[object], Optional[str]]:
    """
    Invite a user into the target org on behalf of a platform admin. Thin delegation to
    orguserfunctions.invite_user_to_org with is_platform_admin=True, so the inviter-level
    role cap is skipped — a platform admin acting cross-org has no role in the target org
    to compare against. Returns orguserfunctions' (result, error) tuple; the API maps the
    error to a status code.
    """
    return orguserfunctions.invite_user_to_org(org, inviter, payload, is_platform_admin=True)


def change_orguser_role(
    org: Org, requestor: OrgUser, orguser: OrgUser, role_uuid
) -> Tuple[Optional[dict], Optional[str]]:
    """
    Change a user's role within the target org on behalf of a platform admin. Thin
    delegation to orguserfunctions.change_orguser_role_in_org with is_platform_admin=True,
    which skips the "can't assign a role higher than your own" cap. Returns the
    (result, error) tuple.
    """
    return orguserfunctions.change_orguser_role_in_org(
        org, requestor, orguser.user.email, role_uuid, is_platform_admin=True
    )


def remove_orguser(
    org: Org, requestor: OrgUser, orguser: OrgUser
) -> Tuple[Optional[object], Optional[str]]:
    """
    Remove a user from the target org on behalf of a platform admin. Thin delegation to
    orguserfunctions.delete_orguser_from_org with is_platform_admin=True (role-level cap
    skipped). The user's created content is ORPHANED, not deleted — Dashboard / Chart /
    ReportSnapshot.created_by are SET_NULL. Returns the (result, error) tuple.
    """
    return orguserfunctions.delete_orguser_from_org(
        org,
        requestor,
        DeleteOrgUserPayload(email=orguser.user.email),
        is_platform_admin=True,
    )


def get_orguser_in_org(org: Org, orguser_id: int) -> Optional[OrgUser]:
    """Resolve an OrgUser by id, scoped to the target org (None if it belongs elsewhere)."""
    return OrgUser.objects.filter(id=orguser_id, org=org).first()


def list_org_users(org: Org) -> List[OrgUser]:
    """The org's members, with user + role prefetched for the Users tab."""
    return list(OrgUser.objects.filter(org=org).select_related("user", "new_role"))


def list_org_invitations(org: Org) -> List[Invitation]:
    """
    The org's pending invitations, scoped by the explicit target org — so an invite a
    platform admin created into this org shows even though invited_by is in another org.
    """
    return list(Invitation.objects.filter(invited_in_org=org).select_related("invited_new_role"))


def get_pending_invitation(org: Org, invited_email: str) -> Optional[Invitation]:
    """
    The pending Invitation row for an email in this org, if any. Returns None when the
    invitee already had a platform account (they were added directly as an OrgUser, so
    no Invitation row exists).
    """
    return (
        Invitation.objects.filter(
            invited_email__iexact=invited_email.lower().strip(),
            invited_in_org=org,
        )
        .select_related("invited_new_role")
        .first()
    )


def get_invitation_in_org(org: Org, invitation_id: int) -> Optional[Invitation]:
    """
    Resolve a pending invitation by id, scoped to the target org (None if it belongs
    elsewhere) — so a wrong-org id yields 404 rather than touching another org's invite.
    """
    return Invitation.objects.filter(id=invitation_id, invited_in_org=org).first()


def delete_invitation(invitation: Invitation) -> None:
    """Cancel (delete) a pending invitation."""
    invitation.delete()


def set_orguser_active(orguser: OrgUser, is_active: bool) -> OrgUser:
    """
    Deactivate/reactivate a user in THIS org only (OrgUser.is_active, NOT User.is_active).
    Their membership of any other org is unaffected.
    """
    orguser.is_active = is_active
    orguser.save(update_fields=["is_active"])
    logger.info(
        "admin %s %s in org %s",
        "reactivated" if is_active else "deactivated",
        orguser.user.email,
        orguser.org.slug,
    )
    return orguser


def removal_impact(orguser: OrgUser) -> Tuple[int, int, int]:
    """
    (dashboards, charts, reports) that removing this user would orphan — its created_by
    set to NULL (the content is kept, not deleted). Exact ORM counts on the created_by FK.
    """
    return (
        Dashboard.objects.filter(created_by=orguser).count(),
        Chart.objects.filter(created_by=orguser).count(),
        ReportSnapshot.objects.filter(created_by=orguser).count(),
    )
