from django.utils.text import slugify
from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpairbyte.schema import AirbyteWorkspace
from ddpui.ddpprefect import prefect_service
from ddpui.ddpprefect import AIRBYTESERVER
from ddpui.models.org import OrgPrefectBlock
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("airbyte")


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
        # create a custom source kobotoolbox
        kobo_name = "Kobotoolbox"
        kobo_docker_repository = "airbyte/source-kobotoolbox"
        kobo_docker_image_tag = "0.1.0"
        kobo_documentation_url = ""
        airbyte_service.create_custom_source_definition(
            workspace_id=workspace["workspaceId"],
            name=kobo_name,
            docker_repository=kobo_docker_repository,
            docker_image_tag=kobo_docker_image_tag,
            documentation_url=kobo_documentation_url,
        )

        # create a custom source commcare
        commcare_name = "custom_commcare"
        commcare_docker_repository = "airbyte/source-commcare"
        commcare_docker_image_tag = "0.1.1"
        commcare_documentation_url = ""
        airbyte_service.create_custom_source_definition(
            workspace_id=workspace["workspaceId"],
            name=commcare_name,
            docker_repository=commcare_docker_repository,
            docker_image_tag=commcare_docker_image_tag,
            documentation_url=commcare_documentation_url,
        )
    except Exception as error:
        logger.error("Error creating custom source definitions: %s", str(error))
        raise error

    # Airbyte server block details. prefect doesn't know the workspace id
    block_name = f"{org.slug}-{slugify(AIRBYTESERVER)}"
    display_name = wsname

    try:
        airbyte_server_block_id = prefect_service.get_airbyte_server_block_id(
            block_name
        )
    except Exception as exc:
        raise Exception("could not connect to prefect-proxy") from exc

    if airbyte_server_block_id is None:
        airbyte_server_block_id = prefect_service.create_airbyte_server_block(
            block_name
        )
        logger.info(f"Created Airbyte server block with ID {airbyte_server_block_id}")

    if not OrgPrefectBlock.objects.filter(org=org, block_type=AIRBYTESERVER).exists():
        org_airbyte_server_block = OrgPrefectBlock(
            org=org,
            block_type=AIRBYTESERVER,
            block_id=airbyte_server_block_id,
            block_name=block_name,
            display_name=display_name,
        )
        org_airbyte_server_block.save()

    return AirbyteWorkspace(
        name=workspace["name"],
        workspaceId=workspace["workspaceId"],
        initialSetupComplete=workspace["initialSetupComplete"],
    )
