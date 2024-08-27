from ddpui.core.pipelinefunctions import setup_airbyte_sync_task_config
from ddpui.models.org import Org, OrgPrefectBlockv1
from ddpui.ddpprefect import AIRBYTESERVER, prefect_service

from ddpui.models.tasks import DataflowOrgTask, OrgTask
from ddpui.utils.constants import TASK_AIRBYTERESET, TASK_AIRBYTESYNC
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


def trigger_reset_and_sync_workflow(org: Org, connection_id: str):
    """
    Trigger a prefect flow run for a connection, first for reset and then for sync.
    """
    sync_org_task = OrgTask.objects.filter(
        org=org, connection_id=connection_id, task__slug=TASK_AIRBYTESYNC
    ).first()

    if sync_org_task is None:
        logger.error("Sync OrgTask not found")
        return None, "sync OrgTask not found"

    # Find the relevant DataflowOrgTask for sync
    sync_dataflow_orgtask = DataflowOrgTask.objects.filter(
        orgtask=sync_org_task, dataflow__dataflow_type="manual"
    ).first()

    if sync_dataflow_orgtask is None:
        logger.error("Sync dataflow not found")
        return None, "sync dataflow not found"

    # Find the relevant OrgTask for reset
    reset_org_task = OrgTask.objects.filter(
        org=org, connection_id=connection_id, task__slug=TASK_AIRBYTERESET
    ).first()

    if reset_org_task is None:
        logger.error("Reset OrgTask not found")
        return None, "reset OrgTask not found"

    sync_deployment_id = sync_dataflow_orgtask.dataflow.deployment_id

    if not sync_deployment_id:
        logger.error("Deployment ID not found")
        return None, "deployment ID not found"

    org_server_block = OrgPrefectBlockv1.objects.filter(
        org=org, block_type=AIRBYTESERVER
    ).first()

    if not org_server_block:
        logger.error("Airbyte server block not found")
        return None, "airbyte server block not found"

    # full reset + sync; run via the manual sync deployment
    params = {
        "config": {
            "tasks": [
                setup_airbyte_sync_task_config(
                    reset_org_task, org_server_block, seq=1
                ).to_json(),
                setup_airbyte_sync_task_config(
                    sync_org_task, org_server_block, seq=2
                ).to_json(),
            ],
            "org_slug": org.slug,
        }
    }

    try:
        prefect_service.create_deployment_flow_run(sync_deployment_id, params)
        logger.info("Successfully triggered Prefect flow run for reset")
    except Exception as error:
        logger.error("Failed to trigger Prefect flow run for reset: %s", error)
        return None, "failed to trigger Prefect flow run for reset"

    return None, None
