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
from ddpui.models.org_user import OrgUser


# ── Session ──────────────────────────────────────────────────────────────────


class AdminLoginSchema(Schema):
    """credentials for the admin portal's own sign-in"""

    username: str
    password: str


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
    is_active: bool
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
            is_active=org.is_active,
            user_count=user_count,
        )


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


# ── Users tab (M4) ───────────────────────────────────────────────────────────


class AdminOrgUserSchema(Schema):
    """a user within an org, as shown in the admin portal Users tab"""

    orguser_id: int
    email: str
    new_role_slug: str | None
    # per-org active flag (OrgUser.is_active) — NOT the global User.is_active
    is_active: bool

    @classmethod
    def from_model(cls, orguser: OrgUser) -> "AdminOrgUserSchema":
        """Build the response from an OrgUser (per-org is_active, not the global flag)."""
        return cls(
            orguser_id=orguser.id,
            email=orguser.user.email,
            new_role_slug=orguser.new_role.slug if orguser.new_role else None,
            is_active=orguser.is_active,
        )


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
    what removing a user would orphan. Drives the confirm dialog's warning.
    Dashboard/Chart/ReportSnapshot created_by are all SET_NULL — the content is KEPT,
    only the creator link is cleared (its created_by becomes NULL). Nothing is deleted.
    (Access Control v2 / PR #1428 switched Dashboard & Chart from CASCADE to SET_NULL;
    ReportSnapshot was already SET_NULL.) See research §5.
    """

    dashboards_orphaned: int
    charts_orphaned: int
    reports_orphaned: int
