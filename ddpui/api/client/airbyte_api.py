import os
import json
from datetime import datetime
from typing import List
from ninja import NinjaAPI
from ninja.errors import HttpError

from ninja.errors import ValidationError
from ninja.responses import Response

from pydantic.error_wrappers import ValidationError as PydanticValidationError
from django.utils.text import slugify
from ddpui import auth
from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpairbyte.schema import (
    AirbyteConnectionCreate,
    AirbyteDestinationCreate,
    AirbyteDestinationUpdate,
    AirbyteSourceCreate,
    AirbyteSourceUpdate,
    AirbyteWorkspace,
    AirbyteWorkspaceCreate,
    AirbyteSourceUpdateCheckConnection,
    AirbyteDestinationUpdateCheckConnection,
    AirbyteConnectionUpdate,
)
from ddpui.utils.deploymentblocks import write_dataflowblocks
from ddpui.ddpprefect.schema import (
    PrefectFlowAirbyteConnection,
    PrefectAirbyteConnectionBlockSchema,
    PrefectAirbyteSync,
    PrefectDataFlowCreateSchema2,
)

from ddpui.ddpprefect import (
    AIRBYTESERVER,
    AIRBYTECONNECTION,
    DBTCORE,
)
from ddpui.ddpprefect import prefect_service
from ddpui.models.org import OrgPrefectBlock, OrgWarehouse, OrgDataFlow
from ddpui.models.orgjobs import DataflowBlock, BlockLock
from ddpui.models.org_user import OrgUser
from ddpui.ddpairbyte import airbytehelpers
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager


airbyteapi = NinjaAPI(urls_namespace="airbyte")
logger = CustomLogger("airbyte")


@airbyteapi.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """
    Handle any ninja validation errors raised in the apis
    These are raised during request payload validation
    exc.errors is correct
    """
    return Response({"detail": exc.errors}, status=422)


@airbyteapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """
    Handle any pydantic errors raised in the apis
    These are raised during response payload validation
    exc.errors() is correct
    """
    return Response({"detail": exc.errors()}, status=500)


@airbyteapi.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception
):  # pylint: disable=unused-argument
    """Handle any other exception raised in the apis"""
    logger.exception(exc)
    return Response({"detail": "something went wrong"}, status=500)


@airbyteapi.post("/workspace/detach/", auth=auth.CanManagePipelines())
def post_airbyte_detach_workspace(request):
    """Detach airbyte workspace from organization"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "org already has no workspace")

    orguser.org.airbyte_workspace_id = None
    orguser.org.save()

    org_airbyte_server_block = OrgPrefectBlock.objects.filter(
        org=orguser.org, block_type=AIRBYTESERVER
    ).first()

    if org_airbyte_server_block:
        # delete the prefect AirbyteServer block
        prefect_service.delete_airbyte_server_block(org_airbyte_server_block.block_id)

        # delete all prefect airbyteconnection blocks fo this org
        for org_airbyte_connection_block in OrgPrefectBlock.objects.filter(
            org=orguser.org, block_type=AIRBYTECONNECTION
        ):
            prefect_service.delete_airbyte_connection_block(
                org_airbyte_connection_block.block_id
            )
            org_airbyte_connection_block.delete()

        org_airbyte_server_block.delete()

    return {"success": 1}


@airbyteapi.post(
    "/workspace/", response=AirbyteWorkspace, auth=auth.CanManagePipelines()
)
def post_airbyte_workspace(request, payload: AirbyteWorkspaceCreate):
    """Create an airbyte workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is not None:
        raise HttpError(400, "org already has a workspace")

    workspace = airbytehelpers.setup_airbyte_workspace(payload.name, orguser.org)

    return workspace


@airbyteapi.get("/source_definitions", auth=auth.CanManagePipelines())
def get_airbyte_source_definitions(request):
    """Fetch airbyte source definitions in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source_definitions(orguser.org.airbyte_workspace_id)
    logger.debug(res)
    return res["sourceDefinitions"]


@airbyteapi.get(
    "/source_definitions/{sourcedef_id}/specifications",
    auth=auth.CanManagePipelines(),
)
def get_airbyte_source_definition_specifications(request, sourcedef_id):
    """
    Fetch definition specifications for a particular
    source definition in the user organization workspace
    """
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source_definition_specification(
        orguser.org.airbyte_workspace_id, sourcedef_id
    )
    logger.debug(res)
    return res["connectionSpecification"]


@airbyteapi.post("/sources/", auth=auth.CanManagePipelines())
def post_airbyte_source(request, payload: AirbyteSourceCreate):
    """Create airbyte source in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    source = airbyte_service.create_source(
        orguser.org.airbyte_workspace_id,
        payload.name,
        payload.sourceDefId,
        payload.config,
    )
    logger.info("created source having id " + source["sourceId"])
    return {"sourceId": source["sourceId"]}


@airbyteapi.put("/sources/{source_id}", auth=auth.CanManagePipelines())
def put_airbyte_source(request, source_id: str, payload: AirbyteSourceUpdate):
    """Update airbyte source in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    source = airbyte_service.update_source(
        source_id, payload.name, payload.config, payload.sourceDefId
    )
    logger.info("updated source having id " + source["sourceId"])
    return {"sourceId": source["sourceId"]}


@airbyteapi.post("/sources/check_connection/", auth=auth.CanManagePipelines())
def post_airbyte_check_source(request, payload: AirbyteSourceCreate):
    """Test the source connection in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    response = airbyte_service.check_source_connection(
        orguser.org.airbyte_workspace_id, payload
    )
    return {
        "status": "succeeded" if response["jobInfo"]["succeeded"] else "failed",
        "logs": response["jobInfo"]["logs"]["logLines"],
    }


@airbyteapi.post(
    "/sources/{source_id}/check_connection_for_update/", auth=auth.CanManagePipelines()
)
def post_airbyte_check_source_for_update(
    request, source_id: str, payload: AirbyteSourceUpdateCheckConnection
):
    """Test the source connection in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    response = airbyte_service.check_source_connection_for_update(source_id, payload)
    return {
        "status": "succeeded" if response["jobInfo"]["succeeded"] else "failed",
        "logs": response["jobInfo"]["logs"]["logLines"],
    }


@airbyteapi.get("/sources", auth=auth.CanManagePipelines())
def get_airbyte_sources(request):
    """Fetch all airbyte sources in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_sources(orguser.org.airbyte_workspace_id)["sources"]
    logger.debug(res)
    return res


@airbyteapi.get("/sources/{source_id}", auth=auth.CanManagePipelines())
def get_airbyte_source(request, source_id):
    """Fetch a single airbyte source in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source(orguser.org.airbyte_workspace_id, source_id)
    logger.debug(res)
    return res


@airbyteapi.delete("/sources/{source_id}", auth=auth.CanManagePipelines())
def delete_airbyte_source(request, source_id):
    """Fetch a single airbyte source in the user organization workspace"""
    logger.info("deleting source started")

    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    # Fetch all org prefect connection blocks
    org_blocks = OrgPrefectBlock.objects.filter(
        org=orguser.org,
        block_type=AIRBYTECONNECTION,
    ).all()

    logger.info("fetched airbyte connections block of this org")

    connections = airbyte_service.get_connections(orguser.org.airbyte_workspace_id)[
        "connections"
    ]
    connections_of_source = [
        conn["connectionId"] for conn in connections if conn["sourceId"] == source_id
    ]

    # delete the connection prefect blocks that has connections
    # built on the source i.e. connections_of_source
    prefect_conn_blocks = prefect_service.get_airbye_connection_blocks(
        block_names=[block.block_name for block in org_blocks]
    )
    logger.info(
        "fetched prefect connection blocks based on the names stored in "
        "django orgprefectblocks"
    )
    delete_block_ids = []
    for block in prefect_conn_blocks:
        if block["connectionId"] in connections_of_source:
            delete_block_ids.append(block["id"])

    # delete the prefect conn blocks
    prefect_service.post_prefect_blocks_bulk_delete(delete_block_ids)
    logger.info("deleted prefect blocks")

    # delete airbyte connection blocks in django orgprefectblock table
    for block in OrgPrefectBlock.objects.filter(
        org=orguser.org, block_type=AIRBYTECONNECTION, block_id__in=delete_block_ids
    ).all():
        block.delete()
    logger.info("deleted airbyte connection blocks from django database")

    # delete the source
    airbyte_service.delete_source(orguser.org.airbyte_workspace_id, source_id)
    logger.info(f"deleted airbyte source {source_id}")

    return {"success": 1}


@airbyteapi.get("/sources/{source_id}/schema_catalog", auth=auth.CanManagePipelines())
def get_airbyte_source_schema_catalog(request, source_id):
    """Fetch schema catalog for a source in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source_schema_catalog(
        orguser.org.airbyte_workspace_id, source_id
    )
    logger.debug(res)
    return res


@airbyteapi.get("/destination_definitions", auth=auth.CanManagePipelines())
def get_airbyte_destination_definitions(request):
    """Fetch destination definitions in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_destination_definitions(orguser.org.airbyte_workspace_id)[
        "destinationDefinitions"
    ]
    allowed_destinations = os.getenv("AIRBYTE_DESTINATION_TYPES")
    if allowed_destinations:
        res = [
            destdef
            for destdef in res
            if destdef["name"] in allowed_destinations.split(",")
        ]
    logger.debug(res)
    return res


@airbyteapi.get(
    "/destination_definitions/{destinationdef_id}/specifications",
    auth=auth.CanManagePipelines(),
)
def get_airbyte_destination_definition_specifications(request, destinationdef_id):
    """
    Fetch specifications for a destination
    definition in the user organization workspace
    """
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_destination_definition_specification(
        orguser.org.airbyte_workspace_id, destinationdef_id
    )["connectionSpecification"]
    logger.debug(res)
    return res


@airbyteapi.post("/destinations/", auth=auth.CanManagePipelines())
def post_airbyte_destination(request, payload: AirbyteDestinationCreate):
    """Create an airbyte destination in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    destination = airbyte_service.create_destination(
        orguser.org.airbyte_workspace_id,
        payload.name,
        payload.destinationDefId,
        payload.config,
    )
    logger.info("created destination having id " + destination["destinationId"])
    return {"destinationId": destination["destinationId"]}


@airbyteapi.post("/destinations/check_connection/", auth=auth.CanManagePipelines())
def post_airbyte_check_destination(request, payload: AirbyteDestinationCreate):
    """Test connection to destination in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    response = airbyte_service.check_destination_connection(
        orguser.org.airbyte_workspace_id, payload
    )
    return {
        "status": "succeeded" if response["jobInfo"]["succeeded"] else "failed",
        "logs": response["jobInfo"]["logs"]["logLines"],
    }


@airbyteapi.post(
    "/destinations/{destination_id}/check_connection_for_update/",
    auth=auth.CanManagePipelines(),
)
def post_airbyte_check_destination_for_update(
    request, destination_id: str, payload: AirbyteDestinationUpdateCheckConnection
):
    """Test connection to destination in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    response = airbyte_service.check_destination_connection_for_update(
        destination_id, payload
    )
    return {
        "status": "succeeded" if response["jobInfo"]["succeeded"] else "failed",
        "logs": response["jobInfo"]["logs"]["logLines"],
    }


@airbyteapi.put("/destinations/{destination_id}/", auth=auth.CanManagePipelines())
def put_airbyte_destination(
    request, destination_id: str, payload: AirbyteDestinationUpdate
):
    """Update an airbyte destination in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    destination = airbyte_service.update_destination(
        destination_id, payload.name, payload.config, payload.destinationDefId
    )
    logger.info("updated destination having id " + destination["destinationId"])
    warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()

    if warehouse.name != payload.name:
        warehouse.name = payload.name
        warehouse.save()

    dbt_credentials = secretsmanager.retrieve_warehouse_credentials(warehouse)

    if warehouse.wtype == "postgres":
        aliases = {
            "dbname": "database",
        }
        for config_key in ["host", "port", "username", "password", "database"]:
            if (
                config_key in payload.config
                and isinstance(payload.config[config_key], str)
                and len(payload.config[config_key]) > 0
                and list(set(payload.config[config_key]))[0] != "*"
            ):
                dbt_credentials[aliases.get(config_key, config_key)] = payload.config[
                    config_key
                ]

    elif warehouse.wtype == "bigquery":
        dbt_credentials = json.loads(payload.config["credentials_json"])
    elif warehouse.wtype == "snowflake":
        if (
            "credentials" in payload.config
            and "password" in payload.config["credentials"]
            and isinstance(payload.config["credentials"]["password"], str)
            and len(payload.config["credentials"]["password"]) > 0
            and list(set(payload.config["credentials"]["password"])) != "*"
        ):
            dbt_credentials["credentials"]["password"] = payload.config["credentials"]["password"]

    else:
        raise HttpError(400, "unknown warehouse type " + warehouse.wtype)

    secretsmanager.update_warehouse_credentials(warehouse, dbt_credentials)

    for dbtblock in OrgPrefectBlock.objects.filter(org=orguser.org, block_type=DBTCORE):
        prefect_service.update_dbt_core_block_credentials(
            warehouse.wtype, dbtblock.block_name, dbt_credentials
        )

    return {"destinationId": destination["destinationId"]}


@airbyteapi.get("/destinations", auth=auth.CanManagePipelines())
def get_airbyte_destinations(request):
    """Fetch all airbyte destinations in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_destinations(orguser.org.airbyte_workspace_id)[
        "destinations"
    ]
    logger.debug(res)
    return res


@airbyteapi.get("/destinations/{destination_id}", auth=auth.CanManagePipelines())
def get_airbyte_destination(request, destination_id):
    """Fetch an airbyte destination in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_destination(
        orguser.org.airbyte_workspace_id, destination_id
    )
    logger.debug(res)
    return res


@airbyteapi.get(
    "/connections",
    auth=auth.CanManagePipelines(),
    response=List[PrefectAirbyteConnectionBlockSchema],
)
def get_airbyte_connections(request):
    """Fetch all airbyte connections in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    org_prefect_blocks = OrgPrefectBlock.objects.filter(
        org=orguser.org,
        block_type=AIRBYTECONNECTION,
    ).all()

    res = []

    for org_block in org_prefect_blocks:
        # fetch prefect block
        prefect_block = prefect_service.get_airbyte_connection_block_by_id(
            org_block.block_id
        )
        # fetch airbyte connection
        airbyte_conn = airbyte_service.get_connection(
            orguser.org.airbyte_workspace_id, prefect_block["data"]["connection_id"]
        )
        # a single connection will have a manual deployment and (usually) a pipeline
        # we want to show the last sync, from whichever
        last_runs = []
        for dfb in DataflowBlock.objects.filter(
            opb=org_block,
        ):
            run = prefect_service.get_last_flow_run_by_deployment_id(
                dfb.dataflow.deployment_id
            )
            if run:
                last_runs.append(run)

        last_runs.sort(
            key=lambda run: run["startTime"]
            if run["startTime"]
            else run["expectedStartTime"]
        )

        manual_dataflow = DataflowBlock.objects.filter(
            opb=org_block,
            dataflow__cron__isnull=True,
        ).first()

        # is the block currently locked?
        lock = BlockLock.objects.filter(opb=org_block).first()

        res.append(
            {
                "name": org_block.display_name,
                "blockId": org_block.block_id,
                "blockName": prefect_block["name"],
                "blockData": prefect_block["data"],
                "connectionId": airbyte_conn["connectionId"],
                "source": airbyte_conn["source"],
                "destination": airbyte_conn["destination"],
                "catalogId": airbyte_conn["catalogId"],
                "syncCatalog": airbyte_conn["syncCatalog"],
                "status": airbyte_conn["status"],
                "deploymentId": manual_dataflow.dataflow.deployment_id
                if manual_dataflow
                else None,
                "lastRun": last_runs[-1] if len(last_runs) > 0 else None,
                "lock": {
                    "lockedBy": lock.locked_by.user.email,
                    "lockedAt": lock.locked_at,
                }
                if lock
                else None,
            }
        )

    logger.info(res)

    # by default normalization is going as False here because we dont do anything with it
    return res


@airbyteapi.get(
    "/connections/{connection_block_id}",
    auth=auth.CanManagePipelines(),
    response=PrefectAirbyteConnectionBlockSchema,
)
def get_airbyte_connection(request, connection_block_id):
    """Fetch a connection in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    org_block = OrgPrefectBlock.objects.filter(
        org=orguser.org,
        block_id=connection_block_id,
    ).first()

    # fetch prefect block
    prefect_block = prefect_service.get_airbyte_connection_block_by_id(
        connection_block_id
    )
    # fetch airbyte connection
    airbyte_conn = airbyte_service.get_connection(
        orguser.org.airbyte_workspace_id, prefect_block["data"]["connection_id"]
    )
    dataflow = OrgDataFlow.objects.filter(
        org=orguser.org, connection_id=airbyte_conn["connectionId"]
    ).first()

    # is the block currently locked?
    lock = BlockLock.objects.filter(opb=org_block).first()

    # fetch the source and destination names
    # the web_backend/connections/get fetches the source & destination objects also so we dont need to query again

    source_name = airbyte_conn["source"]["sourceName"]

    destination_name = airbyte_conn["destination"]["destinationName"]

    res = {
        "name": org_block.display_name,
        "blockId": prefect_block["id"],
        "blockName": prefect_block["name"],
        "blockData": prefect_block["data"],
        "connectionId": airbyte_conn["connectionId"],
        "source": {"id": airbyte_conn["sourceId"], "name": source_name},
        "destination": {"id": airbyte_conn["destinationId"], "name": destination_name},
        "catalogId": airbyte_conn["catalogId"],
        "syncCatalog": airbyte_conn["syncCatalog"],
        "destinationSchema": airbyte_conn["namespaceFormat"]
        if airbyte_conn["namespaceDefinition"] == "customformat"
        else "",
        "status": airbyte_conn["status"],
        "deploymentId": dataflow.deployment_id if dataflow else None,
        "normalize": airbyte_service.is_operation_normalization(
            airbyte_conn["operationIds"][0]
        )
        if "operationIds" in airbyte_conn and len(airbyte_conn["operationIds"]) == 1
        else False,
        "lock": {
            "lockedBy": lock.locked_by.user.email,
            "lockedAt": lock.locked_at,
        }
        if lock
        else None,
    }

    logger.debug(res)
    return res


@airbyteapi.post(
    "/connections/",
    auth=auth.CanManagePipelines(),
    response=PrefectAirbyteConnectionBlockSchema,
)
def post_airbyte_connection(request, payload: AirbyteConnectionCreate):
    """Create an airbyte connection in the user organization workspace"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    if len(payload.streams) == 0:
        raise HttpError(400, "must specify stream names")

    warehouse = OrgWarehouse.objects.filter(org=org).first()
    if warehouse is None:
        raise HttpError(400, "need to set up a warehouse first")
    if warehouse.airbyte_destination_id is None:
        raise HttpError(400, "warehouse has no airbyte_destination_id")
    payload.destinationId = warehouse.airbyte_destination_id

    if warehouse.airbyte_norm_op_id is None:
        warehouse.airbyte_norm_op_id = airbyte_service.create_normalization_operation(
            org.airbyte_workspace_id
        )["operationId"]
        warehouse.save()

    airbyte_conn = airbyte_service.create_connection(
        org.airbyte_workspace_id, warehouse.airbyte_norm_op_id, payload
    )

    org_airbyte_server_block = OrgPrefectBlock.objects.filter(
        org=org,
        block_type=AIRBYTESERVER,
    ).first()
    if org_airbyte_server_block is None:
        raise Exception(f"{org.slug} has no {AIRBYTESERVER} block in OrgPrefectBlock")

    source_name = airbyte_service.get_source(
        org.airbyte_workspace_id, payload.sourceId
    )["sourceName"]
    destination_name = airbyte_service.get_destination(
        org.airbyte_workspace_id, warehouse.airbyte_destination_id
    )["destinationName"]
    connection_name = f"{source_name}-{destination_name}"
    base_block_name = f"{org.slug}-{slugify(connection_name)}"

    # fetch all prefect blocks are being fetched for a particular type
    prefect_airbyte_connection_block_names = []
    for orgprefectblock in OrgPrefectBlock.objects.filter(
        org=org, block_type=AIRBYTECONNECTION
    ):
        block = prefect_service.get_airbyte_connection_block_by_id(
            orgprefectblock.block_id
        )
        prefect_airbyte_connection_block_names.append(block["name"])

    display_name = payload.name
    block_name = base_block_name
    name_index = 0
    while block_name in prefect_airbyte_connection_block_names:
        name_index += 1
        block_name = base_block_name + f"-{name_index}"

    airbyte_connection_block_id = prefect_service.create_airbyte_connection_block(
        prefect_service.PrefectAirbyteConnectionSetup(
            serverBlockName=org_airbyte_server_block.block_name,
            connectionBlockName=block_name,
            connectionId=airbyte_conn["connectionId"],
        )
    )
    airbyte_connection_block = prefect_service.get_airbyte_connection_block_by_id(
        airbyte_connection_block_id
    )

    logger.info(airbyte_connection_block)

    # create a prefect AirbyteConnection block
    connection_block = OrgPrefectBlock(
        org=org,
        block_type=AIRBYTECONNECTION,
        block_id=airbyte_connection_block["id"],
        block_name=airbyte_connection_block["name"],
        display_name=display_name,
    )
    connection_block.save()

    # use the actual blockname, which may differ from what we constructed above
    block_name = airbyte_connection_block["name"]

    dataflow = prefect_service.create_dataflow(
        PrefectDataFlowCreateSchema2(
            deployment_name=f"manual-sync-{block_name}",
            flow_name=f"manual-sync-{block_name}",
            orgslug=org.slug,
            connection_blocks=[
                PrefectFlowAirbyteConnection(seq=0, blockName=block_name)
            ],
            dbt_blocks=[],
        )
    )

    existing_dataflow = OrgDataFlow.objects.filter(
        deployment_id=dataflow["deployment"]["id"]
    ).first()
    if existing_dataflow:
        existing_dataflow.delete()

    org_dataflow = OrgDataFlow.objects.create(
        org=orguser.org,
        name=f"manual-sync-{block_name}",
        deployment_name=dataflow["deployment"]["name"],
        deployment_id=dataflow["deployment"]["id"],
        connection_id=airbyte_conn["connectionId"],
    )
    write_dataflowblocks(org_dataflow)

    res = {
        "name": display_name,
        "blockId": airbyte_connection_block["id"],
        "blockName": airbyte_connection_block["name"],
        "blockData": airbyte_connection_block["data"],
        "connectionId": airbyte_conn["connectionId"],
        "source": {"id": airbyte_conn["sourceId"]},
        "destination": {
            "id": airbyte_conn["destinationId"],
        },
        "catalogId": airbyte_conn["sourceCatalogId"],
        "syncCatalog": airbyte_conn["syncCatalog"],
        "status": airbyte_conn["status"],
        "deploymentId": dataflow["deployment"]["id"],
        "normalize": payload.normalize,
    }
    logger.debug(res)
    return res


@airbyteapi.post(
    "/connections/{connection_block_id}/reset", auth=auth.CanManagePipelines()
)
def post_airbyte_connection_reset(request, connection_block_id):
    """Reset the data for connection at destination"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    # check if the block exists
    org_prefect_block = OrgPrefectBlock.objects.filter(
        org=orguser.org,
        block_id=connection_block_id,
    ).first()

    if org_prefect_block is None:
        raise HttpError(400, "connection block not found")

    # prefect block
    airbyte_connection_block = prefect_service.get_airbyte_connection_block_by_id(
        connection_block_id
    )

    if (
        "data" not in airbyte_connection_block
        or "connection_id" not in airbyte_connection_block["data"]
    ):
        raise HttpError(500, "connection is missing from the block")

    connection_id = airbyte_connection_block["data"]["connection_id"]

    airbyte_service.reset_connection(connection_id)

    return {"success": 1}


@airbyteapi.put(
    "/connections/{connection_block_id}/update", auth=auth.CanManagePipelines()
)
def put_airbyte_connection(
    request, connection_block_id, payload: AirbyteConnectionUpdate
):  # pylint: disable=unused-argument
    """Update an airbyte connection in the user organization workspace"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    if len(payload.streams) == 0:
        raise HttpError(400, "must specify stream names")

    # check if the block exists
    org_prefect_block = OrgPrefectBlock.objects.filter(
        org=orguser.org,
        block_id=connection_block_id,
    ).first()

    if org_prefect_block is None:
        raise HttpError(400, "connection block not found")

    warehouse = OrgWarehouse.objects.filter(org=org).first()
    if warehouse is None:
        raise HttpError(400, "need to set up a warehouse first")
    if warehouse.airbyte_destination_id is None:
        raise HttpError(400, "warehouse has no airbyte_destination_id")
    payload.destinationId = warehouse.airbyte_destination_id

    # prefect block
    airbyte_connection_block = prefect_service.get_airbyte_connection_block_by_id(
        connection_block_id
    )

    if (
        "data" not in airbyte_connection_block
        or "connection_id" not in airbyte_connection_block["data"]
    ):
        raise HttpError(500, "connection if missing from the block")

    connection_id = airbyte_connection_block["data"]["connection_id"]

    # fetch connection by id from airbyte
    connection = airbyte_service.get_connection(org.airbyte_workspace_id, connection_id)

    # update normalization of data
    if payload.normalize:
        if "operationIds" not in connection or len(connection["operationIds"]) == 0:
            warehouse.airbyte_norm_op_id = (
                airbyte_service.create_normalization_operation(
                    org.airbyte_workspace_id
                )["operationId"]
            )
            warehouse.save()
            connection["operationIds"] = [warehouse.airbyte_norm_op_id]
    else:
        connection["operationIds"] = []

    # update name
    if payload.name:
        # airbyte conn name
        connection["name"] = payload.name
        # update prefect block name
        org_prefect_block.display_name = payload.name
        org_prefect_block.save()

    # always reset the connection
    connection["skipReset"] = False

    # update the airbyte connection
    res = airbyte_service.update_connection(
        org.airbyte_workspace_id, payload, connection
    )

    return res


@airbyteapi.delete("/connections/{connection_block_id}", auth=auth.CanManagePipelines())
def delete_airbyte_connection(request, connection_block_id):
    """Update an airbyte connection in the user organization workspace"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org is None:
        raise HttpError(400, "create an organization first")
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    prefect_block = prefect_service.get_airbyte_connection_block_by_id(
        connection_block_id
    )

    # delete airbyte connection
    logger.info("deleting airbyte connection")
    airbyte_service.delete_connection(
        org.airbyte_workspace_id, prefect_block["data"]["connection_id"]
    )

    # delete prefect block
    logger.info("deleting prefect block")
    prefect_service.delete_airbyte_connection_block(connection_block_id)

    # delete the org prefect airbyteconnection block
    org_airbyte_connection_block = OrgPrefectBlock.objects.filter(
        org=org, block_id=connection_block_id
    ).first()
    if org_airbyte_connection_block:
        # delete the dataflows and their corresponding deployments in prefect
        for dataflow_block in DataflowBlock.objects.filter(
            opb=org_airbyte_connection_block
        ):
            # delete the deployment from prefect
            logger.info("deleting prefect deployment")
            prefect_service.delete_deployment_by_id(
                dataflow_block.dataflow.deployment_id
            )

            # delete the org dataflow for manual deployment
            logger.info("deleting org dataflow from db")
            dataflow_block.dataflow.delete()

            # delete the dataflowblock
            logger.info("deleting dataflowblock from db")
            dataflow_block.delete()

        logger.info("deleting org prefect block")
        org_airbyte_connection_block.delete()

    return {"success": 1}


@airbyteapi.post(
    "/connections/{connection_block_id}/sync/",
    auth=auth.CanManagePipelines(),
    deprecated=True,
)
def post_airbyte_sync_connection(request, connection_block_id):
    """Sync an airbyte connection in the uer organization workspace"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    org_prefect_connection_block = OrgPrefectBlock.objects.filter(
        org=org, block_id=connection_block_id
    ).first()

    timenow = datetime.now().strftime("%Y-%m-%d.%H:%M:%S")
    return prefect_service.run_airbyte_connection_sync(
        PrefectAirbyteSync(
            blockName=org_prefect_connection_block.block_name,
            flowRunName=f"manual-sync-{org.name}-{timenow}",
        )
    )


@airbyteapi.get("/jobs/{job_id}", auth=auth.CanManagePipelines())
def get_job_status(request, job_id):
    """get the job info from airbyte"""
    result = airbyte_service.get_job_info(job_id)
    logs = result["attempts"][-1]["logs"]["logLines"]
    return {
        "status": result["job"]["status"],
        "logs": logs,
    }
