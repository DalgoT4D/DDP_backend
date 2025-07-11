from ddpui.models.org import (
    Org,
    OrgSchemaChange,
)
from ddpui.models.tasks import OrgTask
from ddpui.utils.custom_logger import CustomLogger
from ddpui.core.orgtaskfunctions import delete_orgtask
from ddpui.ddpairbyte import airbyte_service

logger = CustomLogger("airbyte")


def delete_connection(org: Org, connection_id: str):
    """deletes an airbyte connection"""

    # remove all orgtasks (sync, clear, ...)
    for org_task in OrgTask.objects.filter(
        org=org,
        connection_id=connection_id,
    ):
        delete_orgtask(org_task)

    # delete airbyte connection
    logger.info("deleting airbyte connection %s", connection_id)
    airbyte_service.delete_connection(org.airbyte_workspace_id, connection_id)

    # delete pending schema changes
    ndeleted, _ = OrgSchemaChange.objects.filter(connection_id=connection_id).delete()
    logger.info(f"Deleted {ndeleted} schema changes for connection {connection_id}")

    return None, None
