"""
Admin Portal API — cross-org endpoints for the Dalgo ops team.

Every route here is gated by @platform_admin_required (the global
UserAttributes.is_platform_admin flag), not by per-org permission slugs. See
features/admin-portal/v1/plan.md §3 for why cross-org needs its own layer.
"""

from ninja import Router

from ddpui.auth import platform_admin_required
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

admin_router = Router()


@admin_router.get("/ping")
@platform_admin_required
def get_admin_ping(request):
    """
    Stub health check for the admin portal — proves the platform-admin gate works.
    Returns 200 for platform admins; @platform_admin_required 403s everyone else.
    """
    return {"detail": "pong"}
