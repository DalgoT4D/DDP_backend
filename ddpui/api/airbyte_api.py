"""Dalgo API for Airbyte"""

import os
from typing import List
from ninja.errors import HttpError
from ninja import Router

from ddpui import settings
from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpairbyte.schema import (
    AirbyteConnectionCreate,
    AirbyteConnectionCreateResponse,
    AirbyteGetConnectionsResponse,
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
from ddpui.ddpairbyte import airbytehelpers, deleteconnection
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


@airbyte_router.get("/source_definitions")
@has_permission(["can_view_sources"])
def get_airbyte_source_definitions(request):
    """
    Fetch all available Airbyte source definitions for the user's organization workspace.

    For demo accounts, filters the available source definitions based on the
    DEMO_AIRBYTE_SOURCE_TYPES environment variable.

    Args:
        request: HTTP request object containing orguser authentication data

    Returns:
        list: List of source definition dictionaries containing connector metadata

    Raises:
        HttpError: 400 if no Airbyte workspace exists for the organization
    """
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
    return res["sourceDefinitions"]


@airbyte_router.get("/source_definitions/{sourcedef_id}/specifications")
@has_permission(["can_view_sources"])
def get_airbyte_source_definition_specifications(request, sourcedef_id):
    """
    Fetch configuration specifications for a specific Airbyte source definition.

    Returns the JSON schema specification that defines what configuration
    parameters are required and optional for setting up this source connector.

    Args:
        request: HTTP request object containing orguser authentication data
        sourcedef_id (str): Unique identifier of the source definition

    Returns:
        dict: Connection specification schema for the source definition

    Raises:
        HttpError: 400 if no Airbyte workspace exists for the organization
    """
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source_definition_specification(
        orguser.org.airbyte_workspace_id, sourcedef_id
    )
    logger.debug(res)
    return res["connectionSpecification"]


@airbyte_router.post("/sources/")
@has_permission(["can_create_source"])
def post_airbyte_source(request, payload: AirbyteSourceCreate):
    """
    Create a new Airbyte source connector in the organization's workspace.

    For demo accounts, automatically replaces the provided configuration with
    whitelisted demo configuration to prevent access to unauthorized data sources.

    Args:
        request: HTTP request object containing orguser authentication data
        payload (AirbyteSourceCreate): Source configuration including name,
                                     source definition ID, and connection config

    Returns:
        dict: Dictionary containing the created source's ID

    Raises:
        HttpError: 400 if no Airbyte workspace exists, or if demo account
                      configuration validation fails
    """
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


@airbyte_router.put("/sources/{source_id}")
@has_permission(["can_edit_source"])
def put_airbyte_source(request, source_id: str, payload: AirbyteSourceUpdate):
    """
    Update an existing Airbyte source connector configuration.

    For demo accounts, automatically replaces the provided configuration with
    whitelisted demo configuration to maintain security restrictions.

    Args:
        request: HTTP request object containing orguser authentication data
        source_id (str): Unique identifier of the source to update
        payload (AirbyteSourceUpdate): Updated source configuration including
                                     name, source definition ID, and config

    Returns:
        dict: Dictionary containing the updated source's ID

    Raises:
        HttpError: 400 if organization or Airbyte workspace doesn't exist,
                      or if demo account configuration validation fails
    """
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


@airbyte_router.post("/sources/check_connection/")
@has_permission(["can_create_source"])
def post_airbyte_check_source(request, payload: AirbyteSourceCreate):
    """
    Test connectivity to a source before creating it.

    Validates that the provided source configuration can successfully establish
    a connection. For demo accounts, uses whitelisted configuration instead
    of the provided config.

    Args:
        request: HTTP request object containing orguser authentication data
        payload (AirbyteSourceCreate): Source configuration to test

    Returns:
        dict: Connection test result with status ('succeeded'/'failed') and logs

    Raises:
        HttpError: 400 if no Airbyte workspace exists or demo config validation fails
    """
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


@airbyte_router.post("/sources/{source_id}/check_connection_for_update/")
@has_permission(["can_edit_source"])
def post_airbyte_check_source_for_update(
    request, source_id: str, payload: AirbyteSourceUpdateCheckConnection
):
    """
    Test connectivity for an existing source with updated configuration.

    Validates that updated source configuration can successfully establish
    a connection before applying the changes. For demo accounts, uses
    whitelisted configuration.

    Args:
        request: HTTP request object containing orguser authentication data
        source_id (str): Unique identifier of the existing source
        payload (AirbyteSourceUpdateCheckConnection): Updated configuration to test

    Returns:
        dict: Connection test result with status ('succeeded'/'failed') and logs

    Raises:
        HttpError: 400 if no Airbyte workspace exists or demo config validation fails
    """
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


@airbyte_router.get("/sources")
@has_permission(["can_view_sources"])
def get_airbyte_sources(request):
    """
    Fetch all Airbyte sources configured in the organization's workspace.

    Returns a list of all source connectors that have been created and
    configured within the organization's Airbyte workspace.

    Args:
        request: HTTP request object containing orguser authentication data

    Returns:
        list: List of source dictionaries containing source metadata and configuration

    Raises:
        HttpError: 400 if no Airbyte workspace exists for the organization
    """
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_sources(orguser.org.airbyte_workspace_id)["sources"]
    logger.debug(res)
    return res


@airbyte_router.get("/sources/{source_id}")
@has_permission(["can_view_source"])
def get_airbyte_source(request, source_id):
    """
    Fetch details of a specific Airbyte source by its ID.

    Returns complete configuration and metadata for a single source connector
    including its current status, configuration parameters, and connection details.

    Args:
        request: HTTP request object containing orguser authentication data
        source_id (str): Unique identifier of the source to retrieve

    Returns:
        dict: Source details including configuration, status, and metadata

    Raises:
        HttpError: 400 if no Airbyte workspace exists for the organization
    """
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source(orguser.org.airbyte_workspace_id, source_id)
    logger.debug(res)
    return res


@airbyte_router.get("/sources/{source_id}/schema_catalog")
@has_permission(["can_view_source"])
def get_airbyte_source_schema_catalog(request, source_id):
    """
    Fetch the discovered schema catalog for a specific source.

    Returns the complete data schema that Airbyte has discovered from the source,
    including all available streams, fields, and data types. This is used to
    configure which data to sync and how to structure it.

    Args:
        request: HTTP request object containing orguser authentication data
        source_id (str): Unique identifier of the source

    Returns:
        dict: Schema catalog containing streams, fields, and data type information

    Raises:
        HttpError: 400 if no Airbyte workspace exists for the organization
    """
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source_schema_catalog(orguser.org.airbyte_workspace_id, source_id)
    logger.debug(res)
    return res


@airbyte_router.get("/destination_definitions")
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


@airbyte_router.get("/destination_definitions/{destinationdef_id}/specifications")
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


@airbyte_router.post("/destinations/")
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


@airbyte_router.post("/destinations/check_connection/")
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


@airbyte_router.post("/destinations/{destination_id}/check_connection_for_update/")
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


@airbyte_router.get("/destinations")
@has_permission(["can_view_warehouses"])
def get_airbyte_destinations(request):
    """Fetch all airbyte destinations in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_destinations(orguser.org.airbyte_workspace_id)["destinations"]
    logger.debug(res)
    return res


@airbyte_router.get("/destinations/{destination_id}")
@has_permission(["can_view_warehouse"])
def get_airbyte_destination(request, destination_id):
    """Fetch an airbyte destination in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_destination(orguser.org.airbyte_workspace_id, destination_id)
    logger.debug(res)
    return res


@airbyte_router.get("/jobs/{job_id}")
@has_permission(["can_view_connection"])
def get_job_status(request, job_id):
    """
    Retrieve the status and logs for a specific Airbyte job.

    Returns comprehensive job information including current status and complete
    log output from the latest attempt. Used for monitoring sync progress and
    debugging connection issues.

    Args:
        request: HTTP request object containing orguser authentication data
        job_id (str): Unique identifier of the job to query

    Returns:
        dict: Job status and log lines from the most recent attempt

    Raises:
        HttpError: 400 if user lacks permission to view connections
    """
    result = airbyte_service.get_job_info(job_id)
    logs = result["attempts"][-1]["logs"]["logLines"]
    return {
        "status": result["job"]["status"],
        "logs": logs,
    }


@airbyte_router.get("/jobs/{job_id}/status")
@has_permission(["can_view_connection"])
def get_job_status_without_logs(request, job_id):
    """
    Retrieve only the status of a specific Airbyte job without logs.

    Lightweight endpoint to check job status without downloading potentially
    large log files. Useful for polling job completion status.

    Args:
        request: HTTP request object containing orguser authentication data
        job_id (str): Unique identifier of the job to query

    Returns:
        dict: Job status only, without log data

    Raises:
        HttpError: 400 if user lacks permission to view connections
    """
    result = airbyte_service.get_job_info_without_logs(job_id)
    print(result)
    return {
        "status": result["job"]["status"],
    }


# ==============================================================================
# new apis to go away from the block architecture


@airbyte_router.post("/v1/workspace/", response=AirbyteWorkspace)
@has_permission(["can_create_org"])
def post_airbyte_workspace_v1(request, payload: AirbyteWorkspaceCreate):
    """
    Create a new Airbyte workspace for the organization.

    Initializes a dedicated Airbyte workspace for the organization and adds
    custom connectors configured in system settings. Each organization can
    have only one workspace.

    Args:
        request: HTTP request object containing orguser authentication data
        payload (AirbyteWorkspaceCreate): Workspace configuration including name

    Returns:
        AirbyteWorkspace: Created workspace details including workspace ID

    Raises:
        HttpError: 400 if organization already has an existing workspace
    """
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
    response=AirbyteConnectionCreateResponse,
)
@has_permission(["can_create_connection"])
def post_airbyte_connection_v1(request, payload: AirbyteConnectionCreate):
    """
    Create a new Airbyte connection between a source and destination.

    Establishes a data pipeline connection that defines which streams to sync,
    how frequently to sync them, and how to handle the data transformation.
    At least one stream must be specified.

    Args:
        request: HTTP request object containing orguser authentication data
        payload (AirbyteConnectionCreate): Connection configuration including
                                         source, destination, streams, and sync settings

    Returns:
        AirbyteConnectionCreateResponse: Created connection details including connection ID

    Raises:
        HttpError: 400 if no Airbyte workspace exists, no streams specified,
                      or connection creation fails
    """
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
    response=List[AirbyteGetConnectionsResponse],
)
@has_permission(["can_view_connections"])
def get_airbyte_connections_v1(request):
    """
    Fetch all Airbyte connections in the organization's workspace.

    Returns a comprehensive list of all data pipeline connections configured
    within the organization, including their current status, configuration,
    and sync schedules.

    Args:
        request: HTTP request object containing orguser authentication data

    Returns:
        List[AirbyteGetConnectionsResponse]: List of all connections with their details

    Raises:
        HttpError: 400 if no Airbyte workspace exists or connection retrieval fails
    """
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


@airbyte_router.put("/v1/connections/{connection_id}/update")
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


@airbyte_router.delete("/v1/connections/{connection_id}")
@has_permission(["can_delete_connection"])
def delete_airbyte_connection_v1(request, connection_id):
    """Update an airbyte connection in the user organization workspace"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org is None:
        raise HttpError(400, "create an organization first")
    if org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    _, error = deleteconnection.delete_org_connection(org, connection_id)
    if error:
        raise HttpError(400, error)

    return {"success": 1}


@airbyte_router.get("/v1/connections/{connection_id}/jobs")
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


@airbyte_router.get("/v1/connections/{connection_id}/sync/history")
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


@airbyte_router.put("/v1/destinations/{destination_id}/")
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


@airbyte_router.delete("/sources/{source_id}")
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


@airbyte_router.get("/v1/connections/{connection_id}/catalog")
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


@airbyte_router.post("/v1/connections/{connection_id}/schema_update/schedule")
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


@airbyte_router.get("/v1/connection/schema_change")
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


@airbyte_router.get("/v1/connections/{connection_id}/logsummary")
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


@airbyte_router.get("/v1/logs")
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


@airbyte_router.post("/v1/connections/{connection_id}/cancel/{job_type}/")
@has_permission(["can_edit_connection"])
def post_cancel_connection_job(
    request,
    connection_id: str,
    job_type: str,
):
    """cancels a job for a connection"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    return airbyte_service.cancel_connection_job(
        orguser.org.airbyte_workspace_id, connection_id, job_type
    )
