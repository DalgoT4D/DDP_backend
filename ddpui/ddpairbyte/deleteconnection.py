from ddpui.models.org import (
    Org,
    OrgSchemaChange,
)
from ddpui.models.tasks import OrgTask, DataflowOrgTask
from ddpui.utils.custom_logger import CustomLogger
from ddpui.core.orgtaskfunctions import delete_orgtask
from ddpui.ddpairbyte import airbyte_service
from ninja.errors import HttpError

logger = CustomLogger("airbyte")


def delete_org_connection(org: Org, connection_id: str):
    """deletes an airbyte connection"""
    # Find org tasks for this connection
    org_tasks = OrgTask.objects.filter(
        org=org,
        connection_id=connection_id,
    )

    # Check if connection is being used in orchestrate pipelines before deletion
    pipeline_usage = DataflowOrgTask.objects.filter(
        orgtask__in=org_tasks, dataflow__dataflow_type="orchestrate"
    )

    if pipeline_usage.exists():
        # Get pipeline names for better error message
        pipeline_names = list(pipeline_usage.values_list("dataflow__name", flat=True).distinct())
        raise HttpError(
            403,
            f"Cannot delete connection. It's being used in pipeline(s): {', '.join(pipeline_names)}. "
            f"Please remove the connection from the pipeline(s) first.",
        )

    # remove all orgtasks (sync, clear, ...)
    for org_task in org_tasks:
        delete_orgtask(org_task)

    # delete airbyte connection
    logger.info("deleting airbyte connection %s", connection_id)
    airbyte_service.delete_connection(org.airbyte_workspace_id, connection_id)

    # delete pending schema changes
    ndeleted, _ = OrgSchemaChange.objects.filter(connection_id=connection_id).delete()
    logger.info(f"Deleted {ndeleted} schema changes for connection {connection_id}")

    return None, None
