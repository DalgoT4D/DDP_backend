"""Dalgo API for Airbyte"""

import os
from typing import List, Optional
from pydantic import BaseModel
from ninja.errors import HttpError
from ninja import Router
from flags.state import flag_enabled

from ddpui import auth
from ddpui import settings
from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpairbyte.schema import (
    AirbyteConnectionCreate,
    AirbyteConnectionCreateResponse,
    AirbyteGetConnectionsResponse,
    AirbyteConnectionSchemaUpdate,
    AirbyteConnectionSchemaUpdateSchedule,
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
from ddpui.auth import has_permission

from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgType
from ddpui.models.llm import LogsSummarizationType, LlmSession, LlmSessionStatus
from ddpui.ddpairbyte import airbytehelpers
from ddpui.utils.custom_logger import CustomLogger
from ddpui.celeryworkers.tasks import (
    get_connection_catalog_task,
    add_custom_connectors_to_workspace,
    summarize_logs,
)
from ddpui.models.tasks import (
    TaskProgressHashPrefix,
    TaskProgressStatus,
)
from ddpui.utils.singletaskprogress import SingleTaskProgress

airbyte_router = Router()
logger = CustomLogger("airbyte")

class AirbyteSourceDefinition(BaseModel):
    sourceId: str
    name: str

class AirbyteSourceDefinitionResponse(BaseModel):
    sourceDefinitions: list[AirbyteSourceDefinition]

@airbyte_router.get("/source_definitions", auth=auth.CustomAuthMiddleware(), response=AirbyteSourceDefinitionResponse)
@has_permission(["can_view_sources"])
def get_airbyte_source_definitions(request) -> AirbyteSourceDefinitionResponse:
    """Fetch airbyte source definitions in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source_definitions(orguser.org.airbyte_workspace_id)

    # filter source definitions for demo account
    allowed_sources = os.getenv("DEMO_AIRBYTE_SOURCE_TYPES")
    if orguser.org.base_plan() == OrgType.DEMO and allowed_sources:
        res["sourceDefinitions"] = [
            source_def
            for source_def in res["sourceDefinitions"]
            if source_def["name"] in allowed_sources.split("|")
        ]
    logger.debug(res)
    return AirbyteSourceDefinitionResponse(sourceDefinitions=res["sourceDefinitions"])


@airbyte_router.get(
    "/source_definitions/{sourcedef_id}/specifications",
    auth=auth.CustomAuthMiddleware(),
)
@has_permission(["can_view_sources"])
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


@airbyte_router.post("/sources/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_create_source"])
def post_airbyte_source(request, payload: AirbyteSourceCreate):
    """Create airbyte source in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    if orguser.org.base_plan() == OrgType.DEMO:
        logger.info("Demo account user")
        source_def = airbyte_service.get_source_definition(
            orguser.org.airbyte_workspace_id, payload.sourceDefId
        )
        # replace the payload config with the correct whitelisted source config
        whitelisted_config, error = airbytehelpers.get_demo_whitelisted_source_config(
            source_def["name"]
        )
        if error:
            raise HttpError(400, error)

        payload.config = whitelisted_config
        logger.info("whitelisted the source config")

    source = airbyte_service.create_source(
        orguser.org.airbyte_workspace_id,
        payload.name,
        payload.sourceDefId,
        payload.config,
    )
    logger.info("created source having id " + source["sourceId"])
    return {"sourceId": source["sourceId"]}


@airbyte_router.put("/sources/{source_id}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_edit_source"])
def put_airbyte_source(request, source_id: str, payload: AirbyteSourceUpdate):
    """Update airbyte source in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    if orguser.org.base_plan() == OrgType.DEMO:
        logger.info("Demo account user")
        source = airbyte_service.get_source(orguser.org.airbyte_workspace_id, source_id)

        # replace the payload config with the correct whitelisted source config
        whitelisted_config, error = airbytehelpers.get_demo_whitelisted_source_config(
            source["sourceName"]
        )
        if error:
            raise HttpError(400, error)

        payload.config = whitelisted_config
        logger.info("whitelisted the source config for update")

    source = airbyte_service.update_source(
        source_id, payload.name, payload.config, payload.sourceDefId
    )
    logger.info("updated source having id " + source["sourceId"])
    return {"sourceId": source["sourceId"]}


@airbyte_router.post("/sources/check_connection/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_create_source"])
def post_airbyte_check_source(request, payload: AirbyteSourceCreate):
    """Test the source connection in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    if orguser.org.base_plan() == OrgType.DEMO:
        logger.info("Demo account user")
        source_def = airbyte_service.get_source_definition(
            orguser.org.airbyte_workspace_id, payload.sourceDefId
        )
        # replace the payload config with the correct whitelisted source config
        whitelisted_config, error = airbytehelpers.get_demo_whitelisted_source_config(
            source_def["name"]
        )
        if error:
            raise HttpError(400, error)

        payload.config = whitelisted_config
        logger.info("whitelisted the source config")

    response = airbyte_service.check_source_connection(orguser.org.airbyte_workspace_id, payload)
    return {
        "status": "succeeded" if response["jobInfo"]["succeeded"] else "failed",
        "logs": response["jobInfo"]["logs"]["logLines"],
    }


@airbyte_router.post(
    "/sources/{source_id}/check_connection_for_update/",
    auth=auth.CustomAuthMiddleware(),
)
@has_permission(["can_edit_source"])
def post_airbyte_check_source_for_update(
    request, source_id: str, payload: AirbyteSourceUpdateCheckConnection
):
    """Test the source connection in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    if orguser.org.base_plan() == OrgType.DEMO:
        logger.info("Demo account user")
        source = airbyte_service.get_source(orguser.org.airbyte_workspace_id, source_id)

        # replace the payload config with the correct whitelisted source config
        whitelisted_config, error = airbytehelpers.get_demo_whitelisted_source_config(
            source["sourceName"]
        )
        if error:
            raise HttpError(400, error)

        payload.config = whitelisted_config
        logger.info("whitelisted the source config for update")

    response = airbyte_service.check_source_connection_for_update(source_id, payload)
    return {
        "status": "succeeded" if response["jobInfo"]["succeeded"] else "failed",
        "logs": response["jobInfo"]["logs"]["logLines"],
    }


@airbyte_router.get("/sources", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_sources"])
def get_airbyte_sources(request):
    """Fetch all airbyte sources in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_sources(orguser.org.airbyte_workspace_id)["sources"]
    logger.debug(res)
    return res


@airbyte_router.get("/sources/{source_id}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_source"])
def get_airbyte_source(request, source_id):
    """Fetch a single airbyte source in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source(orguser.org.airbyte_workspace_id, source_id)
    logger.debug(res)
    return res


@airbyte_router.get("/sources/{source_id}/schema_catalog", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_source"])
def get_airbyte_source_schema_catalog(request, source_id):
    """Fetch schema catalog for a source in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source_schema_catalog(orguser.org.airbyte_workspace_id, source_id)
    logger.debug(res)
    return res


@airbyte_router.get("/destination_definitions", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouses"])
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
        res = [destdef for destdef in res if destdef["name"] in allowed_destinations.split(",")]
    logger.debug(res)
    return res


@airbyte_router.get(
    "/destination_definitions/{destinationdef_id}/specifications",
    auth=auth.CustomAuthMiddleware(),
)
@has_permission(["can_view_warehouse"])
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


@airbyte_router.post("/destinations/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_create_warehouse"])
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


@airbyte_router.post("/destinations/check_connection/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_create_warehouse"])
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


@airbyte_router.post(
    "/destinations/{destination_id}/check_connection_for_update/",
    auth=auth.CustomAuthMiddleware(),
)
@has_permission(["can_edit_warehouse"])
def post_airbyte_check_destination_for_update(
    request, destination_id: str, payload: AirbyteDestinationUpdateCheckConnection
):
    """Test connection to destination in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    response = airbyte_service.check_destination_connection_for_update(destination_id, payload)
    return {
        "status": "succeeded" if response["jobInfo"]["succeeded"] else "failed",
        "logs": response["jobInfo"]["logs"]["logLines"],
    }


@airbyte_router.get("/destinations", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouses"])
def get_airbyte_destinations(request):
    """Fetch all airbyte destinations in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_destinations(orguser.org.airbyte_workspace_id)["destinations"]
    logger.debug(res)
    return res


@airbyte_router.get("/destinations/{destination_id}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse"])
def get_airbyte_destination(request, destination_id):
    """Fetch an airbyte destination in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_destination(orguser.org.airbyte_workspace_id, destination_id)
    logger.debug(res)
    return res


@airbyte_router.get("/jobs/{job_id}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_connection"])
def get_job_status(request, job_id):
    """get the job info from airbyte"""
    result = airbyte_service.get_job_info(job_id)
    logs = result["attempts"][-1]["logs"]["logLines"]
    return {
        "status": result["job"]["status"],
        "logs": logs,
    }


@airbyte_router.get("/jobs/{job_id}/status", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_connection"])
def get_job_status_without_logs(request, job_id):
    """get the job info from airbyte"""
    result = airbyte_service.get_job_info_without_logs(job_id)
    print(result)
    return {
        "status": result["job"]["status"],
    }


# ==============================================================================
# new apis to go away from the block architecture


@airbyte_router.post("/v1/workspace/", response=AirbyteWorkspace, auth=auth.CustomAuthMiddleware())
@has_permission(["can_create_org"])
def post_airbyte_workspace_v1(request, payload: AirbyteWorkspaceCreate):
    """Create an airbyte workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is not None:
        raise HttpError(400, "org already has a workspace")

    workspace = airbytehelpers.setup_airbyte_workspace_v1(payload.name, orguser.org)
    # add custom sources to this workspace
    add_custom_connectors_to_workspace.delay(
        workspace.workspaceId, list(settings.AIRBYTE_CUSTOM_SOURCES.values())
    )

    return workspace


@airbyte_router.post(
    "/v1/connections/",
    auth=auth.CustomAuthMiddleware(),
    response=AirbyteConnectionCreateResponse,
)
@has_permission(["can_create_connection"])
def post_airbyte_connection_v1(request, payload: AirbyteConnectionCreate):
    """Create an airbyte connection in the user organization workspace"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    if len(payload.streams) == 0:
        raise HttpError(400, "must specify stream names")

    res, error = airbytehelpers.create_connection(org, payload)
    if error:
        raise HttpError(400, error)

    logger.debug(res)
    return res


@airbyte_router.get(
    "/v1/connections",
    auth=auth.CustomAuthMiddleware(),
    response=List[AirbyteGetConnectionsResponse],
)
@has_permission(["can_view_connections"])
def get_airbyte_connections_v1(request):
    """Fetch all airbyte connections in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res, error = airbytehelpers.get_connections(orguser.org)
    if error:
        raise HttpError(400, error)
    logger.debug(res)

    return res


@airbyte_router.get(
    "/v1/connections/{connection_id}",
    auth=auth.CustomAuthMiddleware(),
    response=AirbyteConnectionCreateResponse,
)
@has_permission(["can_view_connection"])
def get_airbyte_connection_v1(request, connection_id):
    """Fetch a connection in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res, error = airbytehelpers.get_one_connection(orguser.org, connection_id)
    if error:
        raise HttpError(400, error)
    logger.debug(res)
    return res


@airbyte_router.post("/v1/connections/{connection_id}/reset", auth=auth.CustomAuthMiddleware())
@has_permission(["can_reset_connection"])
def post_airbyte_connection_reset_v1(request, connection_id):
    """Reset the data for connection at destination"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    if not flag_enabled("AIRBYTE_RESET_JOB", request_org_slug=org.slug):
        raise HttpError(
            400, "Reset job is disabled. Please contact the dalgo support team at support@dalgo.in"
        )

    _, error = airbytehelpers.reset_connection(org, connection_id)
    if error:
        raise HttpError(400, error)
    return {"success": 1}


@airbyte_router.put("/v1/connections/{connection_id}/update", auth=auth.CustomAuthMiddleware())
@has_permission(["can_edit_connection"])
def put_airbyte_connection_v1(
    request, connection_id, payload: AirbyteConnectionUpdate
):  # pylint: disable=unused-argument
    """Update an airbyte connection in the user organization workspace"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res, error = airbytehelpers.update_connection(org, connection_id, payload)
    if error:
        raise HttpError(400, error)

    return res


@airbyte_router.delete("/v1/connections/{connection_id}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_delete_connection"])
def delete_airbyte_connection_v1(request, connection_id):
    """Update an airbyte connection in the user organization workspace"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org is None:
        raise HttpError(400, "create an organization first")
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    _, error = airbytehelpers.delete_connection(org, connection_id)
    if error:
        raise HttpError(400, error)

    return {"success": 1}


@airbyte_router.get("/v1/connections/{connection_id}/jobs", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_connection"])
def get_latest_job_for_connection(request, connection_id):
    """get the job info from airbyte for a connection"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    job_info, error = airbytehelpers.get_job_info_for_connection(org, connection_id)
    if error:
        raise HttpError(400, error)
    return job_info


@airbyte_router.get(
    "/v1/connections/{connection_id}/sync/history", auth=auth.CustomAuthMiddleware()
)
@has_permission(["can_view_connection"])
def get_sync_history_for_connection(request, connection_id, limit: int = 10, offset: int = 0):
    """get the job info from airbyte for a connection"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    job_info, error = airbytehelpers.get_sync_job_history_for_connection(
        org, connection_id, limit=limit, offset=offset
    )
    if error:
        raise HttpError(400, error)
    return job_info


@airbyte_router.put("/v1/destinations/{destination_id}/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_edit_warehouse"])
def put_airbyte_destination_v1(request, destination_id: str, payload: AirbyteDestinationUpdate):
    """Update an airbyte destination in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    destination, error = airbytehelpers.update_destination(orguser.org, destination_id, payload)
    if error:
        raise HttpError(400, error)

    return {"destinationId": destination["destinationId"]}


@airbyte_router.delete("/sources/{source_id}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_delete_source"])
def delete_airbyte_source_v1(request, source_id):
    """Delete a single airbyte source in the user organization workspace"""
    logger.info("deleting source started")

    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    _, error = airbytehelpers.delete_source(orguser.org, source_id)
    if error:
        raise HttpError(400, error)

    return {"success": 1}


@airbyte_router.get(
    "/v1/connections/{connection_id}/catalog",
    auth=auth.CustomAuthMiddleware(),
)
@has_permission(["can_view_connection"])
def get_connection_catalog_v1(request, connection_id):
    """Fetch a connection in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    task_key = f"{TaskProgressHashPrefix.SCHEMA_CHANGE}-{orguser.org.slug}-{connection_id}"
    current_task_progress = SingleTaskProgress.fetch(task_key)
    if current_task_progress is not None:
        if "progress" in current_task_progress and len(current_task_progress["progress"]) > 0:
            if current_task_progress["progress"][-1]["status"] == [
                TaskProgressStatus.RUNNING,
            ]:
                return {"task_id": task_key, "message": "already running"}

    taskprogress = SingleTaskProgress(task_key, int(os.getenv("SCHEMA_REFRESH_TTL", "180")))

    taskprogress.add({"message": "queued", "status": "queued", "result": None})
    # ignore the returned celery task id
    get_connection_catalog_task.delay(task_key, orguser.org.id, connection_id)

    return {"task_id": task_key}


@airbyte_router.put(
    "/v1/connections/{connection_id}/schema_update", auth=auth.CustomAuthMiddleware()
)
@has_permission(["can_edit_connection"])
def update_connection_schema(request, connection_id, payload: AirbyteConnectionSchemaUpdate):
    """update schema change in a connection"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res, error = airbytehelpers.update_connection_schema(org, connection_id, payload)
    if error:
        raise HttpError(400, error)
    return res


@airbyte_router.post(
    "/v1/connections/{connection_id}/schema_update/schedule",
    auth=auth.CustomAuthMiddleware(),
)
@has_permission(["can_edit_connection"])
def schedule_update_connection_schema(
    request, connection_id, payload: AirbyteConnectionSchemaUpdateSchedule
):
    """
    schedule a schema change flow for a connection. this would include
    1. updating the connection with the correct catalog
    2. resetting affecting streams
    3. syncing the connection again
    """
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    airbytehelpers.schedule_update_connection_schema(orguser, connection_id, payload)

    return {"success": 1}


@airbyte_router.get(
    "/v1/connection/schema_change",
    auth=auth.CustomAuthMiddleware(),
)
@has_permission(["can_view_connection"])
def get_schema_changes_for_connection(request):
    """Get schema changes for an org"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "Create an Airbyte workspace first")

    res, error = airbytehelpers.get_schema_changes(org)
    if error:
        raise HttpError(400, error)
    return res


@airbyte_router.get("/v1/connections/{connection_id}/logsummary", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_pipeline"])
def get_flow_runs_logsummary_v1(
    request, connection_id: str, job_id: int, attempt_number: int, regenerate: int = 0
):  # pylint: disable=unused-argument
    """
    Use llms to summarize logs
    """
    try:
        orguser: OrgUser = request.orguser

        llm_session = (
            LlmSession.objects.filter(orguser=orguser, airbyte_job_id=job_id)
            .order_by("-created_at")
            .first()
        )
        if llm_session and llm_session.session_status == LlmSessionStatus.RUNNING:
            return {"task_id": llm_session.request_uuid}

        task = summarize_logs.apply_async(
            kwargs={
                "orguser_id": orguser.id,
                "type": LogsSummarizationType.AIRBYTE_SYNC,
                "job_id": job_id,
                "connection_id": connection_id,
                "attempt_number": attempt_number,
                "regenerate": regenerate == 1,
            },
        )
        return {"task_id": task.id}
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to retrieve logs") from error


@airbyte_router.get("v1/logs", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_connection"])
def get_job_logs(
    request,
    job_id: int,
    attempt_number: int,
):
    """get the log info from airbyte for a job attempt"""
    try:
        log_lines = airbyte_service.get_logs_for_job(job_id, attempt_number)
    except Exception as error:
        logger.exception(error)
        log_lines = ["An error occured while fetching logs!"]

    return log_lines
