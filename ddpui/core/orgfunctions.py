"""functions for working with Orgs"""

import json
from django.utils.text import slugify

from ddpui.ddpairbyte import airbyte_service, airbytehelpers
from ddpui.utils import secretsmanager
from ddpui.utils.custom_logger import CustomLogger

from ddpui.models.org import (
    Org,
    OrgSchema,
    OrgWarehouse,
    OrgWarehouseSchema,
)

logger = CustomLogger("ddpui")


def create_warehouse(org: Org, payload: OrgWarehouseSchema):
    """creates a warehouse for an org"""

    if payload.wtype not in ["postgres", "bigquery", "snowflake"]:
        return None, "unrecognized warehouse type " + payload.wtype

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
        # host, database, port, username, password
        # jdbc_url_params
        # ssl: true | false
        # ssl_mode:
        #   mode: disable | allow | prefer | require | verify-ca | verify-full
        #   ca_certificate: string if mode is require, verify-ca, or verify-full
        #   client_key_password: string if mode is verify-full
        # tunnel_method:
        #   tunnel_method = NO_TUNNEL | SSH_KEY_AUTH | SSH_PASSWORD_AUTH
        #   tunnel_host: string if SSH_KEY_AUTH | SSH_PASSWORD_AUTH
        #   tunnel_port: int if SSH_KEY_AUTH | SSH_PASSWORD_AUTH
        #   tunnel_user: string if SSH_KEY_AUTH | SSH_PASSWORD_AUTH
        #   ssh_key: string if SSH_KEY_AUTH
        #   tunnel_user_password: string if SSH_PASSWORD_AUTH
        dbt_credentials = payload.airbyteConfig
    elif payload.wtype == "bigquery":
        credentials_json = json.loads(payload.airbyteConfig["credentials_json"])
        dbt_credentials = credentials_json
    elif payload.wtype == "snowflake":
        dbt_credentials = payload.airbyteConfig

    destination_definition = airbyte_service.get_destination_definition(
        org.airbyte_workspace_id, payload.destinationDefId
    )

    warehouse = OrgWarehouse(
        org=org,
        name=payload.name,
        wtype=payload.wtype,
        credentials="",
        airbyte_destination_id=destination["destinationId"],
        airbyte_docker_repository=destination_definition["dockerRepository"],
        airbyte_docker_image_tag=destination_definition["dockerImageTag"],
    )
    credentials_lookupkey = secretsmanager.save_warehouse_credentials(
        warehouse, dbt_credentials
    )
    warehouse.credentials = credentials_lookupkey
    if "dataset_location" in destination["connectionConfiguration"]:
        warehouse.bq_location = destination["connectionConfiguration"][
            "dataset_location"
        ]
    warehouse.save()

    return None, None


def get_warehouses(org: Org):
    """return list of warehouses for an Org"""
    warehouses = [
        {
            "wtype": warehouse.wtype,
            # "credentials": warehouse.credentials,
            "name": warehouse.name,
            "airbyte_destination": airbyte_service.get_destination(
                org.airbyte_workspace_id, warehouse.airbyte_destination_id
            ),
            "airbyte_docker_repository": warehouse.airbyte_docker_repository,
            "airbyte_docker_image_tag": warehouse.airbyte_docker_image_tag,
        }
        for warehouse in OrgWarehouse.objects.filter(org=org)
    ]
    return warehouses, None


def create_organization(payload: OrgSchema):
    """creates a new Org"""
    org = Org.objects.filter(name__iexact=payload.name).first()
    if org:
        return None, "client org with this name already exists"

    org = Org(name=payload.name)
    org.slug = slugify(org.name)[:20]
    org.save()

    try:
        airbytehelpers.setup_airbyte_workspace_v1(org.slug, org)
    except Exception:
        # delete the org or we won't be able to create it once airbyte comes back up
        org.delete()
        return None, "could not create airbyte workspace"

    org.refresh_from_db()

    return org, None
