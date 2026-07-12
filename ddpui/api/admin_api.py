"""
Admin Portal API — cross-org endpoints for the Dalgo ops team.

Every route here is gated by @platform_admin_required (the global
UserAttributes.is_platform_admin flag), not by per-org permission slugs. See
features/admin-portal/v1/plan.md §3 for why cross-org needs its own layer.
"""

from ninja import Router, Schema

from ddpui.auth import platform_admin_required
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

admin_router = Router()


class AdminStatsSchema(Schema):
    """platform-wide counts for the admin dashboard"""

    total_orgs: int
    total_users: int


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
