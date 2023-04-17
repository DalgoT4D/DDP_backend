from ninja import NinjaAPI
from ninja.errors import HttpError, ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError

from django.utils.text import slugify
from ddpui import auth
from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpairbyte.schema import (
    AirbyteConnectionCreate,
    AirbyteConnectionUpdate,
    AirbyteDestinationCreate,
    AirbyteDestinationUpdate,
    AirbyteSourceCreate,
    AirbyteSourceUpdate,
    AirbyteWorkspace,
    AirbyteWorkspaceCreate,
)
from ddpui.ddpprefect import prefect_service
from ddpui.ddpprefect.org_prefect_block import OrgPrefectBlock
from ddpui.utils.ddp_logger import logger


airbyteapi = NinjaAPI(urls_namespace="airbyte")


@airbyteapi.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """Handle any ninja validation errors raised in the apis"""
    return Response({"error": exc.errors}, status=422)


@airbyteapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """Handle any pydantic errors raised in the apis"""
    return Response({"error": exc.errors()}, status=422)


@airbyteapi.exception_handler(HttpError)
def ninja_http_error_handler(
    request, exc: HttpError
):  # pylint: disable=unused-argument
    """Handle any http errors raised in the apis"""
    return Response({"error": " ".join(exc.args)}, status=exc.status_code)


@airbyteapi.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception
):  # pylint: disable=unused-argument
    """Handle any other exception raised in the apis"""
    return Response({"error": " ".join(exc.args)}, status=500)


@airbyteapi.post("/workspace/detach/", auth=auth.CanManagePipelines())
def post_airbyte_detach_workspace(request):
    """Detach airbyte workspace from organization"""
    orguser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "org already has no workspace")

    orguser.org.airbyte_workspace_id = None
    orguser.org.save()

    org_airbyte_server_block = OrgPrefectBlock.objects.filter(
        org=orguser.org, blocktype=prefect_service.AIRBYTESERVER
    ).first()

    if org_airbyte_server_block:
        # delete the prefect AirbyteServer block
        prefect_service.delete_airbyte_server_block(org_airbyte_server_block.blockid)

        # delete all prefect airbyteconnection blocks fo this org
        for org_airbyte_connection_block in OrgPrefectBlock.objects.filter(
            org=orguser.org, blocktype=prefect_service.AIRBYTECONNECTION
        ):
            prefect_service.delete_airbyte_connection_block(
                org_airbyte_connection_block.blockid
            )
            org_airbyte_connection_block.delete()

        org_airbyte_server_block.delete()

    return {"success": 1}


@airbyteapi.post(
    "/workspace/", response=AirbyteWorkspace, auth=auth.CanManagePipelines()
)
def post_airbyte_workspace(request, payload: AirbyteWorkspaceCreate):
    """Create an airbyte workspace"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is not None:
        raise HttpError(400, "org already has a workspace")

    workspace = airbyte_service.create_workspace(payload.name)

    orguser.org.airbyte_workspace_id = workspace["workspaceId"]
    orguser.org.save()

    # Airbyte server block details
    block_name = f"{orguser.org.slug}-{slugify(prefect_service.AIRBYTESERVER)}"
    display_name = payload.name

    airbyte_server_block = prefect_service.get_block(
        prefect_service.AIRBYTESERVER, block_name
    )
    if airbyte_server_block is None:
        airbyte_server_block = prefect_service.create_airbyte_server_block(block_name)
        logger.info(airbyte_server_block)

    # todo: update the server block if already present. Create a function in prefect service

    if not OrgPrefectBlock.objects.filter(
        org=orguser.org, blocktype=prefect_service.AIRBYTESERVER
    ).exists():
        org_airbyte_server_block = OrgPrefectBlock(
            org=orguser.org,
            blocktype=prefect_service.AIRBYTESERVER,
            blockid=airbyte_server_block["id"],
            blockname=airbyte_server_block["name"],
            displayname=display_name,
        )
        org_airbyte_server_block.save()

    return AirbyteWorkspace(
        name=workspace["name"],
        workspaceId=workspace["workspaceId"],
        initialSetupComplete=workspace["initialSetupComplete"],
    )


@airbyteapi.get("/source_definitions", auth=auth.CanManagePipelines())
def get_airbyte_source_definitions(request):
    """Fetch airbyte source definitions in the user organization workspace"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source_definitions(orguser.org.airbyte_workspace_id)
    logger.debug(res)
    return res


@airbyteapi.get(
    "/source_definitions/{sourcedef_id}/specifications",
    auth=auth.CanManagePipelines(),
)
def get_airbyte_source_definition_specifications(request, sourcedef_id):
    """Fetch definition specifications for a particular source definition in the user organization workspace"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source_definition_specification(
        orguser.org.airbyte_workspace_id, sourcedef_id
    )
    logger.debug(res)
    return res


@airbyteapi.post("/sources/", auth=auth.CanManagePipelines())
def post_airbyte_source(request, payload: AirbyteSourceCreate):
    """Create airbyte source in the user organization workspace"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    source = airbyte_service.create_source(
        orguser.org.airbyte_workspace_id,
        payload.name,
        payload.sourcedef_id,
        payload.config,
    )
    logger.info("created source having id " + source["sourceId"])
    return {"source_id": source["sourceId"]}


@airbyteapi.put("/sources/{source_id}", auth=auth.CanManagePipelines())
def put_airbyte_source(request, source_id: str, payload: AirbyteSourceUpdate):
    """Update airbyte source in the user organization workspace"""
    orguser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    source = airbyte_service.update_source(
        source_id,
        payload.name,
        payload.config,
    )
    logger.info("updated source having id " + source["sourceId"])
    return {"source_id": source["sourceId"]}


@airbyteapi.post("/sources/{source_id}/check/", auth=auth.CanManagePipelines())
def post_airbyte_check_source(request, source_id):
    """Test the source connection in the user organization workspace"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.check_source_connection(
        orguser.org.airbyte_workspace_id, source_id
    )
    logger.debug(res)
    return res


@airbyteapi.get("/sources", auth=auth.CanManagePipelines())
def get_airbyte_sources(request):
    """Fetch all airbyte sources in the user organization workspace"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_sources(orguser.org.airbyte_workspace_id)
    logger.debug(res)
    return res


@airbyteapi.get("/sources/{source_id}", auth=auth.CanManagePipelines())
def get_airbyte_source(request, source_id):
    """Fetch a single airbyte source in the user organization workspace"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source(orguser.org.airbyte_workspace_id, source_id)
    logger.debug(res)
    return res


@airbyteapi.get("/sources/{source_id}/schema_catalog", auth=auth.CanManagePipelines())
def get_airbyte_source_schema_catalog(request, source_id):
    """Fetch schema catalog for a source in the user organization workspace"""
    orguser = request.orguser
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
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_destination_definitions(orguser.org.airbyte_workspace_id)
    logger.debug(res)
    return res


@airbyteapi.get(
    "/destination_definitions/{destinationdef_id}/specifications",
    auth=auth.CanManagePipelines(),
)
def get_airbyte_destination_definition_specifications(request, destinationdef_id):
    """Fetch specifications for a destination definition in the user organization workspace"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_destination_definition_specification(
        orguser.org.airbyte_workspace_id, destinationdef_id
    )
    logger.debug(res)
    return res


@airbyteapi.post("/destinations/", auth=auth.CanManagePipelines())
def post_airbyte_destination(request, payload: AirbyteDestinationCreate):
    """Create an airbyte destination in the user organization workspace"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    destination = airbyte_service.create_destination(
        orguser.org.airbyte_workspace_id,
        payload.name,
        payload.destinationdef_id,
        payload.config,
    )
    logger.info("created destination having id " + destination["destinationId"])
    return {"destination_id": destination["destinationId"]}


@airbyteapi.put("/destinations/{destination_id}/", auth=auth.CanManagePipelines())
def put_airbyte_destination(
    request, destination_id: str, payload: AirbyteDestinationUpdate
):
    """Update an airbyte destination in the user organization workspace"""
    orguser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    destination = airbyte_service.update_destination(
        destination_id,
        payload.name,
        payload.config,
    )
    logger.info("updated destination having id " + destination["destinationId"])
    return {"destination_id": destination["destinationId"]}


@airbyteapi.post(
    "/destinations/{destination_id}/check/", auth=auth.CanManagePipelines()
)
def post_airbyte_check_destination(request, destination_id):
    """Test connection to destination in the user organization workspace"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.check_destination_connection(
        orguser.org.airbyte_workspace_id, destination_id
    )
    logger.debug(res)
    return res


@airbyteapi.get("/destinations", auth=auth.CanManagePipelines())
def get_airbyte_destinations(request):
    """Fetch all airbyte destinations in the user organization workspace"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_destinations(orguser.org.airbyte_workspace_id)
    logger.debug(res)
    return res


@airbyteapi.get("/destinations/{destination_id}", auth=auth.CanManagePipelines())
def get_airbyte_destination(request, destination_id):
    """Fetch an airbyte destination in the user organization workspace"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_destination(
        orguser.org.airbyte_workspace_id, destination_id
    )
    logger.debug(res)
    return res


@airbyteapi.get("/connections", auth=auth.CanManagePipelines())
def get_airbyte_connections(request):
    """Fetch all airbyte connections in the user organization workspace"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_connections(orguser.org.airbyte_workspace_id)
    logger.debug(res)
    return res


@airbyteapi.get("/connections/{connection_id}", auth=auth.CanManagePipelines())
def get_airbyte_connection(request, connection_id):
    """Fetch a connection in the user organization workspace"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_connection(
        orguser.org.airbyte_workspace_id, connection_id
    )
    logger.debug(res)
    return res


@airbyteapi.post("/connections/", auth=auth.CanManagePipelines())
def post_airbyte_connection(request, payload: AirbyteConnectionCreate):
    """Create an airbyte connection in the user organization workspace"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    if len(payload.streamnames) == 0:
        raise HttpError(400, "must specify stream names")

    res = airbyte_service.create_connection(orguser.org.airbyte_workspace_id, payload)

    org_airbyte_server_block = OrgPrefectBlock.objects.filter(
        org=orguser.org,
        blocktype=prefect_service.AIRBYTESERVER,
    ).first()
    if org_airbyte_server_block is None:
        raise Exception(
            f"{orguser.org.slug} has no {prefect_service.AIRBYTESERVER} block in OrgPrefectBlock"
        )

    connection_name = f"{airbyte_service.get_source(orguser.org.airbyte_workspace_id, payload.source_id)['sourceName']}-{airbyte_service.get_destination(orguser.org.airbyte_workspace_id, payload.destination_id)['destinationName']}"
    base_block_name = f"{orguser.org.slug}-{slugify(connection_name)}"
    # fetch all prefect blocks are being fetched for a particular type
    prefect_airbyte_connection_blocks = prefect_service.get_blocks(
        prefect_service.AIRBYTECONNECTION, f"{orguser.org.slug}"
    )
    prefect_airbyte_connection_block_names = [
        blk["name"] for blk in prefect_airbyte_connection_blocks
    ]
    display_name = payload.name
    block_name = base_block_name
    name_index = 0
    while block_name in prefect_airbyte_connection_block_names:
        name_index += 1
        block_name = base_block_name + f"-{name_index}"

    airbyte_connection_block = prefect_service.create_airbyte_connection_block(
        prefect_service.PrefectAirbyteConnectionSetup(
            serverblockname=org_airbyte_server_block.blockname,
            connectionblockname=block_name,
            connection_id=res["connectionId"],
        )
    )

    logger.info(airbyte_connection_block)

    # create a prefect AirbyteConnection block
    connection_block = OrgPrefectBlock(
        org=orguser.org,
        blocktype=prefect_service.AIRBYTECONNECTION,
        blockid=airbyte_connection_block["id"],
        blockname=airbyte_connection_block["name"],
        displayname=display_name,
    )
    connection_block.save()

    logger.debug(res)
    return res


@airbyteapi.put("/connections/{connection_id}", auth=auth.CanManagePipelines())
def put_airbyte_connection(request, connection_id, payload: AirbyteConnectionUpdate):
    """Update an airbyte connection in the user organization workspace"""
    orguser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    if len(payload.streamnames) == 0:
        raise HttpError(400, "must specify stream names")

    res = airbyte_service.update_connection(
        orguser.org.airbyte_workspace_id, connection_id, payload
    )
    logger.debug(res)
    return res


@airbyteapi.delete("/connections/{connection_id}", auth=auth.CanManagePipelines())
def delete_airbyte_connection(request, connection_id):
    """Update an airbyte connection in the user organization workspace"""
    orguser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.delete_connection(
        orguser.org.airbyte_workspace_id, connection_id
    )
    # delete the prefect airbyteconnection block
    # todo: There is no way to know the blockid of this connection. How will we delete ? Might have to change the schema a bit
    org_airbyte_connection_block = OrgPrefectBlock.objects.filter(
        org=orguser.org, blocktype=prefect_service.AIRBYTECONNECTION
    )
    org_airbyte_connection_block.delete()
    logger.debug(res)
    return res


@airbyteapi.post("/connections/{connection_id}/sync/", auth=auth.CanManagePipelines())
def post_airbyte_sync_connection(request, connection_id):
    """Sync an airbyte connection in the uer organization workspace"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    return airbyte_service.sync_connection(
        orguser.org.airbyte_workspace_id, connection_id
    )
