"""Dalgo API for Airbyte"""
import os
import json
from typing import List
from ninja import NinjaAPI
from ninja.errors import HttpError

from ninja.errors import ValidationError
from ninja.responses import Response

from pydantic.error_wrappers import ValidationError as PydanticValidationError
from ddpui import auth
from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpairbyte.schema import (
    AirbyteConnectionCreate,
    AirbyteConnectionCreateResponse,
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
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
)

from ddpui.ddpprefect import AIRBYTESERVER, AIRBYTECONNECTION, DBTCLIPROFILE
from ddpui.ddpprefect import prefect_service
from ddpui.models.org import (
    OrgPrefectBlock,
    OrgWarehouse,
    OrgPrefectBlockv1,
    OrgDataFlowv1,
)
from ddpui.models.tasks import Task, DataflowOrgTask, OrgTask, TaskLock
from ddpui.models.org_user import OrgUser
from ddpui.ddpairbyte import airbytehelpers
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager
from ddpui.utils.constants import TASK_AIRBYTESYNC, AIRBYTE_SYNC_TIMEOUT
from ddpui.utils.helpers import generate_hash_id


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


@airbyteapi.get("/jobs/{job_id}", auth=auth.CanManagePipelines())
def get_job_status(request, job_id):
    """get the job info from airbyte"""
    result = airbyte_service.get_job_info(job_id)
    logs = result["attempts"][-1]["logs"]["logLines"]
    return {
        "status": result["job"]["status"],
        "logs": logs,
    }


# ==============================================================================
# new apis to go away from the block architecture


@airbyteapi.post(
    "/v1/workspace/", response=AirbyteWorkspace, auth=auth.CanManagePipelines()
)
def post_airbyte_workspace_v1(request, payload: AirbyteWorkspaceCreate):
    """Create an airbyte workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is not None:
        raise HttpError(400, "org already has a workspace")

    workspace = airbytehelpers.setup_airbyte_workspace_v1(payload.name, orguser.org)

    return workspace


@airbyteapi.post(
    "/v1/connections/",
    auth=auth.CanManagePipelines(),
    response=AirbyteConnectionCreateResponse,
)
def post_airbyte_connection_v1(request, payload: AirbyteConnectionCreate):
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

    org_airbyte_server_block = OrgPrefectBlockv1.objects.filter(
        org=org,
        block_type=AIRBYTESERVER,
    ).first()
    if org_airbyte_server_block is None:
        raise Exception(f"{org.slug} has no {AIRBYTESERVER} block in OrgPrefectBlock")

    task = Task.objects.filter(slug=TASK_AIRBYTESYNC).first()
    if task is None:
        raise HttpError(400, "task not supported")

    airbyte_conn = airbyte_service.create_connection(
        org.airbyte_workspace_id, warehouse.airbyte_norm_op_id, payload
    )

    org_task = OrgTask.objects.create(
        org=org, task=task, connection_id=airbyte_conn["connectionId"]
    )

    hash_code = generate_hash_id(8)
    logger.info(f"using the hash code {hash_code} for the deployment name")

    deployment_name = f"manual-{orguser.org.slug}-{task.slug}-{hash_code}"
    dataflow = prefect_service.create_dataflow_v1(
        PrefectDataFlowCreateSchema3(
            deployment_name=deployment_name,
            flow_name=deployment_name,
            orgslug=orguser.org.slug,
            deployment_params={
                "config": {
                    "tasks": [
                        {
                            "slug": task.slug,
                            "type": AIRBYTECONNECTION,
                            "seq": 1,
                            "airbyte_server_block": org_airbyte_server_block.block_name,
                            "connection_id": airbyte_conn["connectionId"],
                            "timeout": AIRBYTE_SYNC_TIMEOUT,
                        }
                    ]
                }
            },
        )
    )

    existing_dataflow = OrgDataFlowv1.objects.filter(
        deployment_id=dataflow["deployment"]["id"]
    ).first()
    if existing_dataflow:
        existing_dataflow.delete()

    org_dataflow = OrgDataFlowv1.objects.create(
        org=orguser.org,
        name=deployment_name,
        deployment_name=dataflow["deployment"]["name"],
        deployment_id=dataflow["deployment"]["id"],
        dataflow_type="manual",
    )
    DataflowOrgTask.objects.create(
        dataflow=org_dataflow,
        orgtask=org_task,
    )

    res = {
        "name": payload.name,
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


@airbyteapi.get(
    "/v1/connections",
    auth=auth.CanManagePipelines(),
    response=List[AirbyteConnectionCreateResponse],
)
def get_airbyte_connections_v1(request):
    """Fetch all airbyte connections in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    org_tasks = OrgTask.objects.filter(org=orguser.org, task__type="airbyte").all()

    res = []

    for org_task in org_tasks:
        # fetch the connection
        connection = airbyte_service.get_connection(
            orguser.org.airbyte_workspace_id, org_task.connection_id
        )

        # a single connection will have a manual deployment and (usually) a pipeline
        # we want to show the last sync, from whichever
        last_runs = []
        for df_orgtask in DataflowOrgTask.objects.filter(
            orgtask=org_task,
        ):
            run = prefect_service.get_last_flow_run_by_deployment_id(
                df_orgtask.dataflow.deployment_id
            )
            if run:
                last_runs.append(run)

        last_runs.sort(
            key=lambda run: run["startTime"]
            if run["startTime"]
            else run["expectedStartTime"]
        )

        sync_dataflow = DataflowOrgTask.objects.filter(
            orgtask=org_task, dataflow__dataflow_type="manual"
        ).first()

        # is the task currently locked?
        lock = TaskLock.objects.filter(orgtask=org_task).first()

        res.append(
            {
                "name": connection["name"],
                "connectionId": connection["connectionId"],
                "source": connection["source"],
                "destination": connection["destination"],
                "catalogId": connection["catalogId"],
                "syncCatalog": connection["syncCatalog"],
                "status": connection["status"],
                "deploymentId": sync_dataflow.dataflow.deployment_id
                if sync_dataflow
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
    "/v1/connections/{connection_id}",
    auth=auth.CanManagePipelines(),
    response=AirbyteConnectionCreateResponse,
)
def get_airbyte_connection_v1(request, connection_id):
    """Fetch a connection in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    org_task = OrgTask.objects.filter(
        org=orguser.org,
        connection_id=connection_id,
    ).first()

    if org_task is None or org_task.connection_id is None:
        raise HttpError(404, "connection not found")

    # fetch airbyte connection
    airbyte_conn = airbyte_service.get_connection(
        orguser.org.airbyte_workspace_id, org_task.connection_id
    )

    dataflow_orgtask = DataflowOrgTask.objects.filter(
        orgtask=org_task, dataflow__dataflow_type="manual"
    ).first()

    if dataflow_orgtask is None:
        raise HttpError(422, "deployment not found")

    # check if the task is locked or not
    lock = TaskLock.objects.filter(orgtask=org_task).first()

    # fetch the source and destination names
    # the web_backend/connections/get fetches the source & destination objects also so we dont need to query again
    source_name = airbyte_conn["source"]["sourceName"]

    destination_name = airbyte_conn["destination"]["destinationName"]

    res = {
        "name": airbyte_conn["name"],
        "connectionId": airbyte_conn["connectionId"],
        "source": {"id": airbyte_conn["sourceId"], "name": source_name},
        "destination": {"id": airbyte_conn["destinationId"], "name": destination_name},
        "catalogId": airbyte_conn["catalogId"],
        "syncCatalog": airbyte_conn["syncCatalog"],
        "destinationSchema": airbyte_conn["namespaceFormat"]
        if airbyte_conn["namespaceDefinition"] == "customformat"
        else "",
        "status": airbyte_conn["status"],
        "deploymentId": dataflow_orgtask.dataflow.deployment_id
        if dataflow_orgtask.dataflow
        else None,
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
    "/v1/connections/{connection_id}/reset", auth=auth.CanManagePipelines()
)
def post_airbyte_connection_reset_v1(request, connection_id):
    """Reset the data for connection at destination"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    org_task = OrgTask.objects.filter(
        org=orguser.org,
        connection_id=connection_id,
    ).first()

    if org_task is None:
        raise HttpError(404, "connection not found")

    airbyte_service.reset_connection(connection_id)

    return {"success": 1}


@airbyteapi.put(
    "/v1/connections/{connection_id}/update", auth=auth.CanManagePipelines()
)
def put_airbyte_connection_v1(
    request, connection_id, payload: AirbyteConnectionUpdate
):  # pylint: disable=unused-argument
    """Update an airbyte connection in the user organization workspace"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    org_task = OrgTask.objects.filter(
        org=orguser.org,
        connection_id=connection_id,
    ).first()

    if org_task is None:
        raise HttpError(404, "connection not found")

    warehouse = OrgWarehouse.objects.filter(org=org).first()
    if warehouse is None:
        raise HttpError(400, "need to set up a warehouse first")
    if warehouse.airbyte_destination_id is None:
        raise HttpError(400, "warehouse has no airbyte_destination_id")
    payload.destinationId = warehouse.airbyte_destination_id

    if len(payload.streams) == 0:
        raise HttpError(400, "must specify stream names")

    # fetch connection by id from airbyte
    connection = airbyte_service.get_connection(org.airbyte_workspace_id, connection_id)

    # update normalization of data
    if payload.normalize:
        if "operationIds" not in connection or len(connection["operationIds"]) == 0:
            if warehouse.airbyte_norm_op_id is None:
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
        connection["name"] = payload.name

    # always reset the connection
    connection["skipReset"] = False

    # update the airbyte connection
    res = airbyte_service.update_connection(
        org.airbyte_workspace_id, payload, connection
    )

    return res


@airbyteapi.delete("/v1/connections/{connection_id}", auth=auth.CanManagePipelines())
def delete_airbyte_connection_v1(request, connection_id):
    """Update an airbyte connection in the user organization workspace"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org is None:
        raise HttpError(400, "create an organization first")
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    org_task = OrgTask.objects.filter(
        org=orguser.org,
        connection_id=connection_id,
    ).first()

    if org_task is None:
        raise HttpError(404, "connection not found")

    dataflow_orgtask = DataflowOrgTask.objects.filter(orgtask=org_task).first()

    if dataflow_orgtask is None:
        raise HttpError(422, "deployment not found")

    # delete airbyte connection
    logger.info("deleting airbyte connection")
    airbyte_service.delete_connection(org.airbyte_workspace_id, connection_id)

    # delete prefect deployment
    logger.info("deleteing prefect deployment")
    prefect_service.delete_deployment_by_id(dataflow_orgtask.dataflow.deployment_id)

    # delete the org dataflow for manual deployment
    logger.info("deleting org dataflow from db")
    dataflow_orgtask.dataflow.delete()

    # delete orgtask <-> dataflow mapping
    logger.info("deleteing datafloworgtask mapping")
    dataflow_orgtask.delete()

    # delete orgtask
    logger.info("deleteing orgtask")
    org_task.delete()

    return {"success": 1}


@airbyteapi.get("/v1/connections/{connection_id}/jobs", auth=auth.CanManagePipelines())
def get_latest_job_for_connection(request, connection_id):
    """get the job info from airbyte for a connection"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    result = airbyte_service.get_jobs_for_connection(connection_id)
    if len(result["jobs"]) == 0:
        return {
            "status": "not found",
        }
    latest_job = result["jobs"][0]
    job_info = airbyte_service.parse_job_info(latest_job)
    logs = airbyte_service.get_logs_for_job(job_info["job_id"])
    job_info["logs"] = logs["logs"]["logLines"]
    return job_info


@airbyteapi.put("/v1/destinations/{destination_id}/", auth=auth.CanManagePipelines())
def put_airbyte_destination_v1(
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
            dbt_credentials["credentials"]["password"] = payload.config["credentials"][
                "password"
            ]

    else:
        raise HttpError(400, "unknown warehouse type " + warehouse.wtype)

    secretsmanager.update_warehouse_credentials(warehouse, dbt_credentials)

    cli_profile_block = OrgPrefectBlockv1.objects.filter(
        org=orguser.org, block_type=DBTCLIPROFILE
    ).first()

    if cli_profile_block:
        logger.info(f"Updating the cli profile block : {cli_profile_block.block_name}")
        prefect_service.update_dbt_cli_profile_block(
            block_name=cli_profile_block.block_name,
            wtype=warehouse.wtype,
            credentials=dbt_credentials,
            bqlocation=payload.config["dataset_location"]
            if "dataset_location" in payload.config
            else None,
        )
        logger.info(
            f"Successfully updated the cli profile block : {cli_profile_block.block_name}"
        )

    return {"destinationId": destination["destinationId"]}
