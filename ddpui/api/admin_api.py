"""
Admin Portal API — cross-org endpoints for the Dalgo ops team.

Every route here is gated by @platform_admin_required (the global
UserAttributes.is_platform_admin flag), not by per-org permission slugs. See
features/admin-portal/v1/plan.md §3 for why cross-org needs its own layer.
"""

from typing import List, Optional

from ninja import Router, Schema
from ninja.errors import HttpError
from pydantic import HttpUrl
from django.db import transaction

from ddpui.auth import platform_admin_required
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.org_plans import OrgPlans, OrgPlanType
from ddpui.schemas.org_schema import CreateOrgSchema
from ddpui.core import orgfunctions
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

admin_router = Router()


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


def _admin_org_response(org: Org) -> AdminOrgSchema:
    return AdminOrgSchema(
        id=org.id,
        name=org.name,
        slug=org.slug,
        viz_url=org.viz_url,
        base_plan=org.base_plan(),
        is_active=org.is_active,
        user_count=OrgUser.objects.filter(org=org).count(),
    )


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


@admin_router.get("/orgs", response=List[AdminOrgSchema])
@platform_admin_required
def get_admin_orgs(request):
    """List every org (active and inactive) with its user count."""
    return [_admin_org_response(org) for org in Org.objects.all().order_by("name")]


@admin_router.post("/orgs", response=AdminOrgSchema)
@platform_admin_required
@transaction.atomic
def post_admin_org(request, payload: AdminCreateOrgSchema):
    """
    Create an org. Reuses create_organization (which provisions an Airbyte workspace
    and rolls the Org back if Airbyte fails) + create_org_plan. No OrgUser is attached
    here — the first admin is invited on the Users tab (M4).
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
        raise HttpError(400, error)

    _, plan_error = orgfunctions.create_org_plan(create_payload, org)
    if plan_error:
        raise HttpError(400, plan_error)

    logger.info(f"admin created new org {org.name}")
    return _admin_org_response(org)


@admin_router.get("/orgs/{org_id}", response=AdminOrgSchema)
@platform_admin_required
def get_admin_org(request, org_id: int):
    """Org detail (Overview facts)."""
    org = Org.objects.filter(id=org_id).first()
    if org is None:
        raise HttpError(404, "org not found")
    return _admin_org_response(org)


@admin_router.put("/orgs/{org_id}", response=AdminOrgSchema)
@platform_admin_required
def put_admin_org(request, org_id: int, payload: AdminUpdateOrgSchema):
    """Edit an org's name / viz_url / base_plan. slug is never touched (locked)."""
    org = Org.objects.filter(id=org_id).first()
    if org is None:
        raise HttpError(404, "org not found")

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

    return _admin_org_response(org)


@admin_router.post("/orgs/{org_id}/deactivate", response=AdminOrgSchema)
@platform_admin_required
def post_admin_org_deactivate(request, org_id: int):
    """Deactivate an org (reversible). Its users are then blocked at permission-load."""
    org = Org.objects.filter(id=org_id).first()
    if org is None:
        raise HttpError(404, "org not found")
    org.is_active = False
    org.save()
    logger.info(f"admin deactivated org {org.slug}")
    return _admin_org_response(org)


@admin_router.post("/orgs/{org_id}/reactivate", response=AdminOrgSchema)
@platform_admin_required
def post_admin_org_reactivate(request, org_id: int):
    """Reactivate a deactivated org — its users can use the app again."""
    org = Org.objects.filter(id=org_id).first()
    if org is None:
        raise HttpError(404, "org not found")
    org.is_active = True
    org.save()
    logger.info(f"admin reactivated org {org.slug}")
    return _admin_org_response(org)
