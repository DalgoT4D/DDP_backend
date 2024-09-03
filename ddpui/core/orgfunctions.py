"""functions for working with Orgs"""

import json
from django.utils.text import slugify

from ddpui.ddpairbyte import airbyte_service, airbytehelpers
from ddpui.utils import secretsmanager
from ddpui.utils.custom_logger import CustomLogger
from ddpui import settings
from ddpui.models.org import (
    Org,
    OrgSchema,
    OrgWarehouse,
    OrgWarehouseSchema,
)
from ddpui.celeryworkers.tasks import add_custom_connectors_to_workspace

logger = CustomLogger("ddpui")


def create_organization(payload: OrgSchema):
    """creates a new Org"""
    org = Org.objects.filter(name__iexact=payload.name).first()
    if org:
        return None, "client org with this name already exists"

    org = Org(name=payload.name)
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
