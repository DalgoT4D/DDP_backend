"""
Pydantic/Ninja schemas for the Admin Portal API (ddpui/api/admin_api.py).

Kept out of the API module so the service layer can type its signatures against the
same request schemas the API validates (e.g. issue_admin_session takes AdminLoginSchema),
per the API -> schemas/ convention used by every other feature module.
"""

import uuid
from datetime import datetime
from typing import List, Optional

from ninja import Schema
from pydantic import HttpUrl

from ddpui.models.org import Org
from ddpui.models.org_plans import OrgPlanType
from ddpui.models.org_user import Invitation, OrgUser
from ddpui.models.notifications import Notification
from ddpui.schemas.org_schema import CreateOrgSchema


# ── Session ──────────────────────────────────────────────────────────────────
# The admin sign-in body is the existing LoginPayload (ddpui/models/org_user.py) —
# same {username, password} shape as the normal login, so no admin-specific schema.


class AdminCurrentUserSchema(Schema):
    """identity for the admin session — read by the frontend AdminGuard"""

    email: str
    is_platform_admin: bool


class AdminSuccessSchema(Schema):
    """
    acknowledgement for admin mutations that return no entity. success is an int (1),
    not a bool — it matches the existing {"success": 1} wire format the frontend reads.
    """

    success: int


# ── Dashboard / org ──────────────────────────────────────────────────────────


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
    user_count: int

    @classmethod
    def from_model(cls, org: Org, user_count: int) -> "AdminOrgSchema":
        """Build the response from an Org plus its (API-supplied) user_count."""
        return cls(
            id=org.id,
            name=org.name,
            slug=org.slug,
            viz_url=org.viz_url,
            base_plan=org.base_plan(),
            user_count=user_count,
        )


class AdminCreateOrgSchema(CreateOrgSchema):
    """CreateOrgSchema with admin-friendly defaults, so the portal form only requires name"""

    base_plan: str = OrgPlanType.FREE_TRIAL.value
    can_upgrade_plan: bool = True
    subscription_duration: str = "Monthly"
    superset_included: bool = False


class AdminUpdateOrgSchema(Schema):
    """
    payload to edit an org. slug is intentionally absent — it is locked post-create
    because it is used in URLs and the Airbyte workspace (plan.md §8 #4).
    """

    name: Optional[str] = None
    viz_url: Optional[HttpUrl] = None
    base_plan: Optional[str] = None


# ── Users tab (M4) ───────────────────────────────────────────────────────────


class AdminOrgUserSchema(Schema):
    """a user within an org, as shown in the admin portal Users tab"""

    orguser_id: int
    email: str
    new_role_slug: str | None

    @classmethod
    def from_model(cls, orguser: OrgUser) -> "AdminOrgUserSchema":
        """Build the response from an OrgUser."""
        return cls(
            orguser_id=orguser.id,
            email=orguser.user.email,
            new_role_slug=orguser.new_role.slug if orguser.new_role else None,
        )


class AdminInvitationSchema(Schema):
    """a pending invitation within an org (a row that has not been accepted)"""

    id: int
    invited_email: str
    invited_role_slug: str | None
    invited_on: datetime

    @classmethod
    def from_model(cls, invitation: Invitation) -> "AdminInvitationSchema":
        """Build the response from a pending Invitation row."""
        return cls(
            id=invitation.id,
            invited_email=invitation.invited_email,
            invited_role_slug=(
                invitation.invited_new_role.slug if invitation.invited_new_role else None
            ),
            invited_on=invitation.invited_on,
        )


class AdminOrgUsersResponse(Schema):
    """the Users tab payload: current members plus pending invites"""

    users: List[AdminOrgUserSchema]
    invitations: List[AdminInvitationSchema]


class AdminChangeRoleSchema(Schema):
    """payload to change an org user's role"""

    role_uuid: uuid.UUID


class RemovalImpactSchema(Schema):
    """
    what removing a user would orphan. Drives the confirm dialog's warning.
    Dashboard/Chart/ReportSnapshot created_by are all SET_NULL — the content is KEPT,
    only the creator link is cleared (its created_by becomes NULL). Nothing is deleted.
    (Access Control v2 / PR #1428 switched Dashboard & Chart from CASCADE to SET_NULL;
    ReportSnapshot was already SET_NULL.) See research §5.
    """

    dashboards_orphaned: int
    charts_orphaned: int
    reports_orphaned: int


# ── Feature flags tab (M3) ───────────────────────────────────────────────────


class AdminFeatureFlagCatalogItem(Schema):
    """one entry in the fixed FEATURE_FLAGS registry"""

    flag_name: str
    description: str


class AdminSetOrgFlagSchema(Schema):
    """payload to turn one flag on/off for a single org"""

    enabled: bool


class AdminBulkSetFlagSchema(Schema):
    """payload to turn one flag on/off for several orgs at once"""

    org_ids: List[int]
    enabled: bool


class AdminBulkFlagResultItem(Schema):
    """
    one org's outcome from a bulk flag set. Deliberately success-only -- no message
    field -- so a failed org_id can never be told apart from a different failure
    cause (e.g. "doesn't exist" vs "write failed") by whoever reads the response
    (plan.md §5: the bulk endpoint must not become an org-existence oracle).
    """

    org_id: int
    success: bool


# ── Notifications tab (M2) ───────────────────────────────────────────────────
# Broadcast notifications: whole platform, one org, or several orgs at once, with
# admin-chosen channels. Reuses Notification/NotificationRecipient as-is (§4.1) --
# the new work here is these admin-facing HTTP routes plus the additive
# target_org_ids/send_in_app/send_email fields on the existing model.


class AdminNotificationAudienceSchema(Schema):
    """shared audience shape for preview and create -- null/empty org_ids is the
    whole platform; one or several org_ids is a merged multi-org audience, never a
    per-org breakdown (plan.md §4.3, §5)"""

    org_ids: Optional[List[int]] = None


class AdminNotificationPreviewResponseSchema(Schema):
    """one combined recipient count across the whole chosen audience -- never the
    recipient list itself (would leak a cross-org email roster) and never a
    per-org count"""

    recipient_count: int


class AdminCreateNotificationSchema(AdminNotificationAudienceSchema):
    """payload to send a broadcast immediately. author is deliberately absent --
    it is derived server-side from the signed-in platform admin, never taken from
    the client (plan.md §4.3, §5)."""

    message: str
    email_subject: str
    urgent: bool = False
    send_in_app: bool = True
    send_email: bool = True


class AdminNotificationSchema(Schema):
    """one broadcast in admin history: audience (resolved to org names), channels,
    time, and recipient count only -- no read status, no recipient list, no audit
    field beyond what the model already carries (plan.md §3.3, §4.3)."""

    id: int
    message: str
    urgent: bool
    timestamp: datetime
    sent_time: Optional[datetime]
    target_org_names: Optional[List[str]]
    send_in_app: bool
    send_email: bool
    recipient_count: int

    @classmethod
    def from_model(
        cls, notification: Notification, target_org_names: Optional[List[str]], recipient_count: int
    ) -> "AdminNotificationSchema":
        """Build the response from a Notification plus its (API-supplied)
        resolved org names and recipient count -- both need a service call, so
        aren't the schema's to compute."""
        return cls(
            id=notification.id,
            message=notification.message,
            urgent=notification.urgent,
            timestamp=notification.timestamp,
            sent_time=notification.sent_time,
            target_org_names=target_org_names,
            send_in_app=notification.send_in_app,
            send_email=notification.send_email,
            recipient_count=recipient_count,
        )


class OrgDeletionImpactSchema(Schema):
    """
    what deleting an org would destroy. Drives the confirm dialog's warning.
    Unlike RemovalImpactSchema (SET_NULL, content kept), every count here is a hard
    CASCADE delete — the org itself, its warehouse/connections/pipelines, all its
    users, and all its dashboards/charts/report snapshots are gone, not orphaned.
    """

    user_count: int
    warehouse_count: int
    connection_count: int
    pipeline_count: int
    dashboard_count: int
    chart_count: int
    report_count: int
