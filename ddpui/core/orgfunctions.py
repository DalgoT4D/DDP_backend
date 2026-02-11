"""functions for working with Orgs"""

from django.utils.text import slugify

from ddpui.ddpairbyte import airbytehelpers
from ddpui.utils.custom_logger import CustomLogger
from ddpui import settings
from ddpui.models.org import (
    Org,
)
from ddpui.schemas.org_schema import CreateOrgSchema
from ddpui.utils.constants import DALGO_WITH_SUPERSET, DALGO, FREE_TRIAL
from ddpui.models.org_plans import OrgPlans, OrgPlanType
from ddpui.celeryworkers.tasks import add_custom_connectors_to_workspace

logger = CustomLogger("ddpui")


def create_organization(payload: CreateOrgSchema):
    """creates a new Org"""
    org = Org.objects.filter(name__iexact=payload.name).first()
    if org:
        return None, "client org with this name already exists"

    org = Org(
        name=payload.name,
        viz_url=str(payload.viz_url) if payload.viz_url else None,
        website=str(payload.website) if payload.website else None,
    )
    org.slug = slugify(org.name)[:20]
    org.save()

    try:
        workspace = airbytehelpers.setup_airbyte_workspace_v1(org.slug, org)
        # add custom sources to this workspace
        add_custom_connectors_to_workspace.delay(
            workspace.workspaceId, list(settings.AIRBYTE_CUSTOM_SOURCES.values())
        )

    except Exception as err:
        logger.error("could not create airbyte workspace: " + str(err))
        # delete the org or we won't be able to create it once airbyte comes back up
        org.delete()
        return None, "could not create airbyte workspace"

    org.refresh_from_db()

    return org, None


def create_org_plan(payload: CreateOrgSchema, org):
    """creates a new Org's plan"""
    existing_plan = OrgPlans.objects.filter(org=org).first()
    if existing_plan:
        return None, "client org's plan already exists"

    plan_payload = {
        "base_plan": payload.base_plan,
        "can_upgrade_plan": payload.can_upgrade_plan,
        "subscription_duration": payload.subscription_duration,
        "superset_included": payload.superset_included,
        "start_date": payload.start_date,
        "end_date": payload.end_date,
        "features": {},
    }

    if payload.base_plan == OrgPlanType.FREE_TRIAL:
        plan_payload["features"] = FREE_TRIAL
    elif payload.base_plan == OrgPlanType.INTERNAL:
        plan_payload["features"] = DALGO_WITH_SUPERSET
    elif payload.base_plan == OrgPlanType.DALGO and payload.superset_included:
        plan_payload["features"] = DALGO_WITH_SUPERSET
    elif payload.base_plan == OrgPlanType.DALGO and not payload.superset_included:
        plan_payload["features"] = DALGO

    org_plan = OrgPlans.objects.create(org=org, **plan_payload)

    return org_plan, None
