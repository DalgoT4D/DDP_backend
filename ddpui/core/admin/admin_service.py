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
from ddpui.models.notifications import Notification
from ddpui.schemas.admin_schema import (
    AdminCreateOrgSchema,
    AdminUpdateOrgSchema,
    AdminFeatureFlagCatalogItem,
    AdminCreateNotificationSchema,
    AdminNotificationSchema,
    OrgDeletionImpactSchema,
)
from ddpui.schemas.notifications_api_schemas import SentToEnum, NotificationDataSchema
from ddpui.core import orgfunctions, orguserfunctions
from ddpui.core.notifications.notifications_functions import get_recipients, create_notification
from ddpui.ddpairbyte import airbyte_service
from ddpui.services.org_cleanup_service import OrgCleanupService, OrgCleanupServiceError
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.feature_flags import (
    FEATURE_FLAGS,
    enable_feature_flag,
    disable_feature_flag,
    clear_org_flag as _clear_org_flag_row,
    get_all_feature_flags_for_org,
    get_flag_value_for_orgs,
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


def delete_org_impact(org: Org) -> OrgDeletionImpactSchema:
    """What deleting this org would destroy. Unlike removal_impact (SET_NULL, content
    kept), every one of these is a hard CASCADE delete. Returns the response schema
    directly -- named fields, so adding a count can't silently shift the others the way
    a positional tuple could (same shape as notification_response)."""
    return OrgDeletionImpactSchema(
        user_count=org_user_count(org),
        warehouse_count=OrgWarehouse.objects.filter(org=org).count(),
        connection_count=OrgTask.objects.filter(org=org, task__type="airbyte").count(),
        pipeline_count=OrgDataFlowv1.objects.filter(org=org, dataflow_type="orchestrate").count(),
        dashboard_count=Dashboard.objects.filter(org=org).count(),
        chart_count=Chart.objects.filter(org=org).count(),
        report_count=ReportSnapshot.objects.filter(org=org).count(),
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
# Feature flags tab (M3) -- every write is single-org; the portal-wide view is a read
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


def get_flag_status_for_orgs(flag_name: str) -> Optional[List[dict]]:
    """One row per org -- {org_id, org_name, enabled} -- for a single flag. The
    global-default/org-override resolution is delegated to feature_flags rather than
    rebuilt here; get_flag_value_for_orgs resolves every org in ONE query, where
    get_org_flags per org would cost one query per org. None if flag_name is not in
    the registry -- validated inside that call, same as the other flag routes."""
    orgs = list(Org.objects.all().order_by("name"))
    statuses = get_flag_value_for_orgs(flag_name, orgs)
    if statuses is None:
        return None
    return [{"org_id": org.id, "org_name": org.name, "enabled": statuses[org.id]} for org in orgs]


def set_org_flag(org: Org, flag_name: str, enabled: bool) -> Optional[bool]:
    """Turn one flag on/off for a single org. None if flag_name is not in the registry."""
    if enabled:
        return enable_feature_flag(flag_name, org)
    return disable_feature_flag(flag_name, org)


def clear_org_flag(org: Org, flag_name: str) -> Optional[bool]:
    """Clear this org's override for a flag, falling back to the global default. None
    if flag_name is not in the registry."""
    return _clear_org_flag_row(flag_name, org)


# --------------------------------------------------------------------------- #
# Notifications tab (M2) -- broadcast: whole platform, one org, or several orgs
# at once, admin-chosen channels. Notification/NotificationRecipient are reused
# as-is; the new work is these admin-facing wrappers around get_recipients /
# create_notification (plan.md §4.3).
# --------------------------------------------------------------------------- #


def _resolve_recipient_ids(org_ids: Optional[List[int]]) -> List[int]:
    """Merge recipients across a whole-platform or multi-org audience into one
    list -- never a per-org breakdown. A bogus org_id resolves to no slug and
    contributes nothing, rather than erroring (plan.md §4.3, §5)."""
    if org_ids:
        org_slugs = list(Org.objects.filter(id__in=org_ids).values_list("slug", flat=True))
        if not org_slugs:
            return []
        error, recipient_ids = get_recipients(
            SentToEnum.ALL_ORG_USERS, None, None, False, org_slugs=org_slugs
        )
    else:
        error, recipient_ids = get_recipients(SentToEnum.ALL_USERS, None, None, False)

    if error:
        return []
    return recipient_ids


def preview_notification_recipients(org_ids: Optional[List[int]]) -> int:
    """One combined recipient count across the whole chosen audience."""
    return len(_resolve_recipient_ids(org_ids))


def create_admin_notification(
    author_email: str, payload: AdminCreateNotificationSchema
) -> Tuple[Optional[str], Optional[Notification]]:
    """Send a broadcast immediately. Blocks a 0-recipient audience and a
    no-channel-selected broadcast before creating anything; author is server-derived,
    never taken from the client."""
    if not payload.send_in_app and not payload.send_email:
        # the composer disables Send in this state, but the rule has to hold for any
        # client: with both channels off the broadcast would reach nobody while still
        # persisting a Notification + a NotificationRecipient row per recipient.
        return "Select at least one channel: in-app or email", None

    recipient_ids = _resolve_recipient_ids(payload.org_ids)
    if not recipient_ids:
        return "No recipients found for the given audience", None

    notification_data = NotificationDataSchema(
        author=author_email,
        message=payload.message,
        email_subject=payload.email_subject,
        urgent=payload.urgent,
        scheduled_time=None,
        recipients=recipient_ids,
        target_org_ids=payload.org_ids,
        send_in_app=payload.send_in_app,
        send_email=payload.send_email,
    )
    error, result = create_notification(notification_data)
    if error:
        return error.get("message", "Failed to send notification"), None

    return None, Notification.objects.get(id=result["res"]["notification_id"])


def notification_response(notification: Notification) -> AdminNotificationSchema:
    """Build the response schema for one notification -- shared by the create
    route and the history list, mirroring _admin_org_response's role for orgs."""
    target_org_names = None
    if notification.target_org_ids:
        target_org_names = list(
            Org.objects.filter(id__in=notification.target_org_ids).values_list("name", flat=True)
        )
    recipient_count = notification.notifications_received.count()
    return AdminNotificationSchema.from_model(notification, target_org_names, recipient_count)


def get_admin_notification_history() -> List[AdminNotificationSchema]:
    """Review sent broadcasts: audience, channels, time, recipient count only."""
    notifications = Notification.objects.filter(sent_time__isnull=False).order_by("-timestamp")
    return [notification_response(n) for n in notifications]
