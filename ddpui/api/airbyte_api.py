"""Dalgo API for Airbyte"""
import os
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


from ddpui.models.org_user import OrgUser
from ddpui.ddpairbyte import airbytehelpers
from ddpui.utils.custom_logger import CustomLogger
from ddpui.assets.whitelist import DEMO_WHITELIST_SOURCES


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

    # filter source definitions for demo account
    allowed_sources = os.getenv("DEMO_AIRBYTE_SOURCE_TYPES")
    if orguser.org.is_demo is True and allowed_sources:
        res["sourceDefinitions"] = [
            source_def
            for source_def in res["sourceDefinitions"]
            if source_def["name"] in allowed_sources.split("|")
        ]
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

    if orguser.org.is_demo:
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


@airbyteapi.put("/sources/{source_id}", auth=auth.CanManagePipelines())
def put_airbyte_source(request, source_id: str, payload: AirbyteSourceUpdate):
    """Update airbyte source in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    if orguser.org.is_demo:
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


@airbyteapi.post("/sources/check_connection/", auth=auth.CanManagePipelines())
def post_airbyte_check_source(request, payload: AirbyteSourceCreate):
    """Test the source connection in the user organization workspace"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    if orguser.org.is_demo:
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

    if orguser.org.is_demo:
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

    res, error = airbytehelpers.create_connection(org, payload)
    if error:
        raise HttpError(400, error)

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

    res, error = airbytehelpers.get_connections(orguser.org)
    if error:
        raise HttpError(400, error)
    logger.debug(res)

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

    res, error = airbytehelpers.get_one_connection(orguser.org, connection_id)
    if error:
        raise HttpError(400, error)
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

    _, error = airbytehelpers.reset_connection(org, connection_id)
    if error:
        raise HttpError(400, error)
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

    res, error = airbytehelpers.update_connection(org, connection_id, payload)
    if error:
        raise HttpError(400, error)

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

    _, error = airbytehelpers.delete_connection(org, connection_id)
    if error:
        raise HttpError(400, error)

    return {"success": 1}


@airbyteapi.get("/v1/connections/{connection_id}/jobs", auth=auth.CanManagePipelines())
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

    destination, error = airbytehelpers.update_destination(
        orguser.org, destination_id, payload
    )
    if error:
        raise HttpError(400, error)

    return {"destinationId": destination["destinationId"]}


@airbyteapi.delete("/sources/{source_id}", auth=auth.CanManagePipelines())
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
