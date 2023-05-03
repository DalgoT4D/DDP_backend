from django.utils.text import slugify
from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpairbyte.schema import AirbyteWorkspace
from ddpui.ddpprefect import prefect_service
from ddpui.ddpprefect import AIRBYTESERVER
from ddpui.models.org import OrgPrefectBlock
from ddpui.utils.ddp_logger import logger

def setup_airbyte_workspace(wsname, org) -> AirbyteWorkspace:
    """creates an airbyte workspace and attaches it to the org
    also creates an airbyte server block in prefect if there isn't one already
    we don't need to update any existing server block because it does not hold
    the workspace id... only the connection details of the airbyte server
    """

    workspace = airbyte_service.create_workspace(wsname)

    org.airbyte_workspace_id = workspace["workspaceId"]
    org.save()

    # Airbyte server block details. prefect doesn't know the workspace id
    block_name = f"{org.slug}-{slugify(AIRBYTESERVER)}"
    display_name = wsname

    airbyte_server_block_id = prefect_service.get_airbyte_server_block_id(
        block_name
    )
    if airbyte_server_block_id is None:
        airbyte_server_block_id = prefect_service.create_airbyte_server_block(
            block_name
        )
        logger.info(airbyte_server_block_id)

    if not OrgPrefectBlock.objects.filter(
        org=org, block_type=AIRBYTESERVER
    ).exists():
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
