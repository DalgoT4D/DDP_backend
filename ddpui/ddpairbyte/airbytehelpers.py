from django.utils.text import slugify
from django.conf import settings
from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpairbyte.schema import AirbyteWorkspace
from ddpui.ddpprefect import prefect_service
from ddpui.ddpprefect import AIRBYTESERVER
from ddpui.models.org import OrgPrefectBlock, OrgPrefectBlockv1, OrgDataFlow, Org
from ddpui.models.orgjobs import BlockLock
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("airbyte")


def add_custom_airbyte_connector(
    workspace_id: str,
    connector_name: str,
    connector_docker_repository: str,
    connector_docker_image_tag: str,
    connector_documentation_url,
) -> None:
    """creates a custom source definition in airbyte"""
    airbyte_service.create_custom_source_definition(
        workspace_id=workspace_id,
        name=connector_name,
        docker_repository=connector_docker_repository,
        docker_image_tag=connector_docker_image_tag,
        documentation_url=connector_documentation_url,
    )
    logger.info(
        f"added custom source {connector_name} [{connector_docker_repository}:{connector_docker_image_tag}]"
    )


def upgrade_custom_sources(workspace_id: str) -> None:
    """
    compares the versions of the custom sources listed above with those in the airbyte workspace
    and upgrades if necessary
    """
    source_definitions = airbyte_service.get_source_definitions(workspace_id)

    for custom_source, custom_source_info in settings.AIRBYTE_CUSTOM_SOURCES.items():
        found_custom_source = False

        for source_def in source_definitions["sourceDefinitions"]:
            if (
                custom_source == source_def["dockerRepository"]
                and custom_source_info["name"] == source_def["name"]
            ):
                found_custom_source = True
                if (
                    source_def["dockerImageTag"]
                    < custom_source_info["docker_image_tag"]
                ):
                    logger.info(
                        f"ready to upgrade {custom_source} from "
                        f"{custom_source_info['docker_image_tag']} to "
                        f"{source_def['dockerImageTag']}"
                    )
                    # instead we should update the existing sourceDef to use the new docker image
                    #  /v1/source_definitions/update
                    add_custom_airbyte_connector(
                        workspace_id,
                        custom_source_info["name"],
                        custom_source_info["docker_repository"],
                        custom_source_info["docker_image_tag"],
                        custom_source_info["documentation_url"],
                    )
                else:
                    logger.info(
                        f"{custom_source} version {custom_source_info['docker_image_tag']} has not changed"
                    )

        if not found_custom_source:
            logger.info(
                f'did not find {custom_source}, adding version {custom_source_info["docker_image_tag"]} now'
            )
            add_custom_airbyte_connector(
                workspace_id,
                custom_source_info["name"],
                custom_source_info["docker_repository"],
                custom_source_info["docker_image_tag"],
                custom_source_info["documentation_url"],
            )


def setup_airbyte_workspace(wsname, org) -> AirbyteWorkspace:
    """creates an airbyte workspace and attaches it to the org
    also creates an airbyte server block in prefect if there isn't one already
    we don't need to update any existing server block because it does not hold
    the workspace id... only the connection details of the airbyte server
    """
    workspace = airbyte_service.create_workspace(wsname)

    org.airbyte_workspace_id = workspace["workspaceId"]
    org.save()

    try:
        for custom_source_info in settings.AIRBYTE_CUSTOM_SOURCES.values():
            add_custom_airbyte_connector(
                workspace["workspaceId"],
                custom_source_info["name"],
                custom_source_info["docker_repository"],
                custom_source_info["docker_image_tag"],
                custom_source_info["documentation_url"],
            )
    except Exception as error:
        logger.error("Error creating custom source definitions: %s", str(error))
        raise error

    # Airbyte server block details. prefect doesn't know the workspace id
    block_name = f"{org.slug}-{slugify(AIRBYTESERVER)}"
    display_name = wsname

    airbyte_server_block_cleaned_name = block_name
    try:
        airbyte_server_block_id = prefect_service.get_airbyte_server_block_id(
            block_name
        )
    except Exception as exc:
        raise Exception("could not connect to prefect-proxy") from exc

    if airbyte_server_block_id is None:
        (
            airbyte_server_block_id,
            airbyte_server_block_cleaned_name,
        ) = prefect_service.create_airbyte_server_block(block_name)
        logger.info(f"Created Airbyte server block with ID {airbyte_server_block_id}")

    if not OrgPrefectBlock.objects.filter(org=org, block_type=AIRBYTESERVER).exists():
        org_airbyte_server_block = OrgPrefectBlock(
            org=org,
            block_type=AIRBYTESERVER,
            block_id=airbyte_server_block_id,
            block_name=airbyte_server_block_cleaned_name,
            display_name=display_name,
        )
        try:
            org_airbyte_server_block.save()
        except Exception as error:
            prefect_service.delete_airbyte_server_block(airbyte_server_block_id)
            raise Exception(
                "could not create orgprefectblock for airbyte-server"
            ) from error

    return AirbyteWorkspace(
        name=workspace["name"],
        workspaceId=workspace["workspaceId"],
        initialSetupComplete=workspace["initialSetupComplete"],
    )


def setup_airbyte_workspace_v1(wsname, org) -> AirbyteWorkspace:
    """creates an airbyte workspace and attaches it to the org
    also creates an airbyte server block in prefect if there isn't one already
    we don't need to update any existing server block because it does not hold
    the workspace id... only the connection details of the airbyte server
    """
    workspace = airbyte_service.create_workspace(wsname)

    org.airbyte_workspace_id = workspace["workspaceId"]
    org.save()

    try:
        for custom_source_info in settings.AIRBYTE_CUSTOM_SOURCES.values():
            add_custom_airbyte_connector(
                workspace["workspaceId"],
                custom_source_info["name"],
                custom_source_info["docker_repository"],
                custom_source_info["docker_image_tag"],
                custom_source_info["documentation_url"],
            )
    except Exception as error:
        logger.error("Error creating custom source definitions: %s", str(error))
        raise error

    # Airbyte server block details. prefect doesn't know the workspace id
    block_name = f"{org.slug}-{slugify(AIRBYTESERVER)}"

    airbyte_server_block_cleaned_name = block_name
    try:
        airbyte_server_block_id = prefect_service.get_airbyte_server_block_id(
            block_name
        )
    except Exception as exc:
        raise Exception("could not connect to prefect-proxy") from exc

    if airbyte_server_block_id is None:
        (
            airbyte_server_block_id,
            airbyte_server_block_cleaned_name,
        ) = prefect_service.create_airbyte_server_block(block_name)
        logger.info(f"Created Airbyte server block with ID {airbyte_server_block_id}")

    if not OrgPrefectBlockv1.objects.filter(org=org, block_type=AIRBYTESERVER).exists():
        org_airbyte_server_block = OrgPrefectBlockv1(
            org=org,
            block_type=AIRBYTESERVER,
            block_id=airbyte_server_block_id,
            block_name=airbyte_server_block_cleaned_name,
        )
        try:
            org_airbyte_server_block.save()
        except Exception as error:
            prefect_service.delete_airbyte_server_block(airbyte_server_block_id)
            raise Exception(
                "could not create orgprefectblock for airbyte-server"
            ) from error

    return AirbyteWorkspace(
        name=workspace["name"],
        workspaceId=workspace["workspaceId"],
        initialSetupComplete=workspace["initialSetupComplete"],
    )


def do_get_airbyte_connection(org: Org, workspace_id: str, connection_block_id: str):
    """Fetches an airbyte connection, prefect block info, latest run status"""
    org_block = OrgPrefectBlock.objects.filter(
        org=org,
        block_id=connection_block_id,
    ).first()

    # fetch prefect block
    prefect_block = prefect_service.get_airbyte_connection_block_by_id(
        connection_block_id
    )
    # fetch airbyte connection
    airbyte_conn = airbyte_service.get_connection(
        workspace_id, prefect_block["data"]["connection_id"]
    )
    dataflow = OrgDataFlow.objects.filter(
        org=org, connection_id=airbyte_conn["connectionId"]
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
        "schemaChange": airbyte_conn["schemaChange"],
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
    return res
