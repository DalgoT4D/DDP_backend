"""functions for working with Orgs"""

import json

from ddpui.ddpairbyte import airbyte_service
from ddpui.utils import secretsmanager
from ddpui.utils.custom_logger import CustomLogger

from ddpui.models.org import (
    Org,
    OrgWarehouse,
    OrgWarehouseSchema,
)

logger = CustomLogger("ddpui")


def create_warehouse(org: Org, payload: OrgWarehouseSchema):
    """creates a warehouse for an org"""

    destination = airbyte_service.create_destination(
        org.airbyte_workspace_id,
        f"{payload.wtype}-warehouse",
        payload.destinationDefId,
        payload.airbyteConfig,
    )
    logger.info("created destination having id " + destination["destinationId"])

    # prepare the dbt credentials from airbyteConfig
    dbt_credentials = None
    if payload.wtype == "postgres":
        dbt_credentials = {
            "host": payload.airbyteConfig["host"],
            "port": payload.airbyteConfig["port"],
            "username": payload.airbyteConfig["username"],
            "password": payload.airbyteConfig["password"],
            "database": payload.airbyteConfig["database"],
        }

    elif payload.wtype == "bigquery":
        dbt_credentials = json.loads(payload.airbyteConfig["credentials_json"])
    elif payload.wtype == "snowflake":
        dbt_credentials = {
            "host": payload.airbyteConfig["host"],
            "role": payload.airbyteConfig["role"],
            "warehouse": payload.airbyteConfig["warehouse"],
            "database": payload.airbyteConfig["database"],
            "schema": payload.airbyteConfig["schema"],
            "username": payload.airbyteConfig["username"],
            "credentials": payload.airbyteConfig["credentials"],
        }

    warehouse = OrgWarehouse(
        org=org,
        name=payload.name,
        wtype=payload.wtype,
        credentials="",
        airbyte_destination_id=destination["destinationId"],
    )
    credentials_lookupkey = secretsmanager.save_warehouse_credentials(
        warehouse, dbt_credentials
    )
    warehouse.credentials = credentials_lookupkey
    warehouse.save()

    return None, None
