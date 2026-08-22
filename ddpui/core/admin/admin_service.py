"""
Admin Portal service layer: the ORM/business logic behind ddpui/api/admin_api.py.
Handlers stay thin (parse -> call service -> respond); this module owns the ORM.
"""

from typing import List, Optional, Tuple

from ddpui.core.admin.exceptions import AdminOrgCreateError, AdminOrgDeleteError
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import (
    OrgUser,
    Invitation,
    NewInvitationSchema,
    DeleteOrgUserPayload,
)
from ddpui.models.org_plans import OrgPlans
from ddpui.models.tasks import OrgTask, OrgDataFlowv1
from ddpui.models.dashboard import Dashboard
from ddpui.models.visualization import Chart
from ddpui.models.report import ReportSnapshot
from ddpui.schemas.admin_schema import (
    AdminCreateOrgSchema,
    AdminUpdateOrgSchema,
    AdminFeatureFlagCatalogItem,
)
from ddpui.core import orgfunctions, orguserfunctions
from ddpui.ddpairbyte import airbyte_service
from ddpui.services.org_cleanup_service import OrgCleanupService, OrgCleanupServiceError
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.feature_flags import (
    FEATURE_FLAGS,
    enable_feature_flag,
    disable_feature_flag,
    clear_org_flag as _clear_org_flag_row,
    get_all_feature_flags_for_org,
    bulk_set_feature_flag,
)

logger = CustomLogger("ddpui.core.admin")


# --------------------------------------------------------------------------- #
# Dashboard / org reads
# --------------------------------------------------------------------------- #


def get_platform_stats() -> Tuple[int, int]:
    """(total_orgs, total_users) across the whole platform. total_users counts distinct
    users who belong to at least one org."""
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
    """Create an org + its plan (AdminCreateOrgSchema IS-A CreateOrgSchema). Raises
    AdminOrgCreateError on failure, for the API to map to 400."""
    org, error = orgfunctions.create_organization(payload)
    if error:
        # create_organization already deleted the org on Airbyte failure; nothing persists.
        raise AdminOrgCreateError(error)

    _, plan_error = orgfunctions.create_org_plan(payload, org)
    if plan_error:
        # the Org DB row persists at this point but the Airbyte workspace it already
        # provisioned is not transactional — delete both explicitly rather than leaving
        # an orphaned workspace behind when the caller's @transaction.atomic rolls the
        # Org row back.
        try:
            airbyte_service.delete_workspace(org.airbyte_workspace_id)
        except Exception:
            logger.error(
                f"failed to delete orphaned airbyte workspace {org.airbyte_workspace_id} "
                f"for org {org.slug} after plan creation failure"
            )
        raise AdminOrgCreateError(plan_error)

    logger.info(f"admin created new org {org.name}")
    return org


def update_org(org: Org, payload: AdminUpdateOrgSchema) -> Org:
    """Partial-update name/viz_url/base_plan. slug is locked post-create (used in URLs
    and the Airbyte workspace); only non-None fields are changed."""
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


def delete_org_impact(org: Org) -> Tuple[int, int, int, int, int, int, int]:
    """(users, warehouses, connections, pipelines, dashboards, charts, reports) that
    deleting this org would destroy. Unlike removal_impact (SET_NULL, content kept),
    every one of these is a hard CASCADE delete."""
    return (
        org_user_count(org),
        OrgWarehouse.objects.filter(org=org).count(),
        OrgTask.objects.filter(org=org, task__type="airbyte").count(),
        OrgDataFlowv1.objects.filter(org=org, dataflow_type="orchestrate").count(),
        Dashboard.objects.filter(org=org).count(),
        Chart.objects.filter(org=org).count(),
        ReportSnapshot.objects.filter(org=org).count(),
    )


def delete_org(org: Org) -> None:
    """Hard-delete an org and everything it owns: Airbyte workspace, Prefect
    deployments/blocks, warehouse credentials, dbt/git setup, and org users — the same
    OrgCleanupService the `deleteorg` management command uses, run for real
    (dry_run=False)."""
    try:
        OrgCleanupService(org, dry_run=False).delete_org()
    except OrgCleanupServiceError as err:
        raise AdminOrgDeleteError(str(err)) from err


# --------------------------------------------------------------------------- #
# Org users + invitations
# --------------------------------------------------------------------------- #


def invite_user(
    org: Org, inviter: OrgUser, payload: NewInvitationSchema
) -> Tuple[Optional[object], Optional[str]]:
    """Invite a user into the target org as a platform admin (skips the inviter-level
    role cap, since the admin has no role in the target org)."""
    return orguserfunctions.invite_user_to_org(org, inviter, payload, is_platform_admin=True)


def change_orguser_role(
    org: Org, requestor: OrgUser, orguser: OrgUser, role_uuid
) -> Tuple[Optional[dict], Optional[str]]:
    """Change a user's role in the target org as a platform admin (skips the "can't
    assign higher than your own role" cap)."""
    return orguserfunctions.change_orguser_role_in_org(
        org, requestor, orguser.user.email, role_uuid, is_platform_admin=True
    )


def remove_orguser(
    org: Org, requestor: OrgUser, orguser: OrgUser
) -> Tuple[Optional[object], Optional[str]]:
    """Remove a user from the target org as a platform admin. Their created content is
    orphaned (created_by SET_NULL), not deleted."""
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
    """The org's pending invitations, scoped by the explicit target org (not invited_by,
    which may be a different org for an admin-created invite)."""
    return list(Invitation.objects.filter(invited_in_org=org).select_related("invited_new_role"))


def get_pending_invitation(org: Org, invited_email: str) -> Optional[Invitation]:
    """The pending Invitation for an email in this org, or None if the invitee already
    had an account and was added directly (no Invitation row)."""
    return (
        Invitation.objects.filter(
            invited_email__iexact=invited_email.lower().strip(),
            invited_in_org=org,
        )
        .select_related("invited_new_role")
        .first()
    )


def get_invitation_in_org(org: Org, invitation_id: int) -> Optional[Invitation]:
    """Resolve a pending invitation by id, scoped to the target org (None if it belongs
    to a different org)."""
    return Invitation.objects.filter(id=invitation_id, invited_in_org=org).first()


def delete_invitation(invitation: Invitation) -> None:
    """Cancel (delete) a pending invitation."""
    invitation.delete()


def removal_impact(orguser: OrgUser) -> Tuple[int, int, int]:
    """(dashboards, charts, reports) that removing this user would orphan (created_by
    set to NULL; content is kept, not deleted)."""
    return (
        Dashboard.objects.filter(created_by=orguser).count(),
        Chart.objects.filter(created_by=orguser).count(),
        ReportSnapshot.objects.filter(created_by=orguser).count(),
    )


# --------------------------------------------------------------------------- #
# Feature flags tab (M3) -- per-org and multi-org on/off
# --------------------------------------------------------------------------- #


def get_flag_catalog() -> List[AdminFeatureFlagCatalogItem]:
    """The fixed FEATURE_FLAGS registry, served as the catalog the frontend renders
    instead of a hand-maintained TS enum."""
    return [
        AdminFeatureFlagCatalogItem(flag_name=name, description=description)
        for name, description in FEATURE_FLAGS.items()
    ]


def get_org_flags(org: Org) -> dict:
    """All flags for this org: global default merged with any org-specific override."""
    return get_all_feature_flags_for_org(org)


def set_org_flag(org: Org, flag_name: str, enabled: bool) -> Optional[bool]:
    """Turn one flag on/off for a single org. None if flag_name is not in the registry."""
    if enabled:
        return enable_feature_flag(flag_name, org)
    return disable_feature_flag(flag_name, org)


def clear_org_flag(org: Org, flag_name: str) -> Optional[bool]:
    """Clear this org's override for a flag, falling back to the global default. None
    if flag_name is not in the registry."""
    return _clear_org_flag_row(flag_name, org)


def bulk_set_org_flags(flag_name: str, org_ids: List[int], enabled: bool) -> Optional[List[dict]]:
    """Turn one flag on/off for several orgs at once. None if flag_name is not in the
    registry -- validated once, up front, never a per-org concern."""
    if flag_name not in FEATURE_FLAGS:
        return None
    return bulk_set_feature_flag(flag_name, org_ids, enabled)
