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

from ddpui.auth import CustomTokenObtainSerializer
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser, Invitation, UserAttributes
from ddpui.models.org_plans import OrgPlans
from ddpui.models.dashboard import Dashboard
from ddpui.models.visualization import Chart
from ddpui.models.report import ReportSnapshot
from ddpui.schemas.org_schema import CreateOrgSchema
from ddpui.core import orgfunctions
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.admin")


# --------------------------------------------------------------------------- #
# Independent admin session
# --------------------------------------------------------------------------- #


def issue_admin_session(username: str, password: str) -> Tuple[Optional[dict], Optional[str]]:
    """
    Verify credentials AND platform-admin privilege, then mint a distinct admin
    token. Returns (token_data, None) on success, or (None, error) when the
    credentials are wrong or the user is not a platform admin. The API layer sets
    the cookies; this function knows nothing about HTTP.
    """
    user = authenticate(username=username, password=password)
    if user is None:
        return None, "invalid credentials"

    user_attributes = UserAttributes.objects.filter(user=user).first()
    if not (user_attributes and user_attributes.is_platform_admin):
        return None, "not a platform admin"

    # A distinct admin token: the session="admin" claim is what AdminJwtAuthMiddleware
    # requires, so a normal login token (which lacks it) can never satisfy the admin API.
    # Setting the claim on the refresh token before deriving the access token propagates
    # it to both.
    refresh = CustomTokenObtainSerializer.get_token(user)
    refresh["session"] = "admin"
    refresh.set_exp(lifetime=timedelta(hours=settings.JWT_ADMIN_REFRESH_TOKEN_EXPIRY_HOURS))
    access = refresh.access_token
    access.set_exp(lifetime=timedelta(minutes=settings.JWT_ADMIN_ACCESS_TOKEN_EXPIRY_MINUTES))

    return {"access": str(access), "refresh": str(refresh)}, None


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


def create_org(create_payload: CreateOrgSchema) -> Tuple[Optional[Org], Optional[str]]:
    """
    Create an org and its plan. Reuses orgfunctions.create_organization (which
    provisions an Airbyte workspace and rolls the Org back if Airbyte fails) plus
    create_org_plan. No OrgUser is attached here — the first admin is invited on the
    Users tab (M4). Returns (org, error); the caller maps a non-null error to 400.
    """
    org, error = orgfunctions.create_organization(create_payload)
    if error:
        # create_organization already deleted the org on Airbyte failure; nothing persists.
        return None, error

    _, plan_error = orgfunctions.create_org_plan(create_payload, org)
    if plan_error:
        return None, plan_error

    logger.info(f"admin created new org {org.name}")
    return org, None


def update_org(
    org: Org,
    *,
    name: Optional[str] = None,
    viz_url: Optional[str] = None,
    base_plan: Optional[str] = None,
) -> Org:
    """
    Partial-update an org's name / viz_url / base_plan. slug is never touched — it is
    locked post-create because it is used in URLs and the Airbyte workspace. Only
    fields passed as non-None are changed. base_plan lives on OrgPlans, not Org.
    """
    if name is not None:
        org.name = name
    if viz_url is not None:
        org.viz_url = viz_url
    org.save()  # slug intentionally excluded from the update

    if base_plan is not None:
        org_plans = OrgPlans.objects.filter(org=org).first()
        if org_plans:
            org_plans.base_plan = base_plan
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
