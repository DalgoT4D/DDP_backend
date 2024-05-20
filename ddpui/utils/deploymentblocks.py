from ddpui.core.pipelinefunctions import setup_airbyte_sync_task_config
from ddpui.models.org import Org, OrgPrefectBlock, OrgDataFlow, OrgPrefectBlockv1
from ddpui.models.orgjobs import DataflowBlock
from ddpui.ddpprefect import AIRBYTESERVER, prefect_service

from ddpui.models.tasks import DataflowOrgTask, OrgTask
from ddpui.utils.constants import TASK_AIRBYTERESET, TASK_AIRBYTESYNC
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


def write_dataflowblocks(dataflow: OrgDataFlow) -> None:
    """fetches deployment parameters from prefect to write deployment blocks"""
    deployment = prefect_service.get_deployment(dataflow.deployment_id)
    logger.info(deployment)

    blocks = deployment["parameters"].get("airbyte_blocks", [])
    blocks += deployment["parameters"].get("dbt_blocks", [])

    for block in blocks:
        logger.info("searching for %s", block["blockName"])

        if DataflowBlock.objects.filter(
            dataflow=dataflow, opb__org=dataflow.org, opb__block_name=block["blockName"]
        ).exists():
            logger.info("found DataflowBlock for %s", block["blockName"])
            continue

        q_opb = OrgPrefectBlock.objects.filter(
            org=dataflow.org, block_name=block["blockName"]
        )
        if q_opb.count() > 1:
            logger.error("block name is not unique: %s", block["blockName"])
            continue

        opb = q_opb.first()
        if opb:
            DataflowBlock.objects.create(dataflow=dataflow, opb=opb)
            logger.info("wrote DataflowBlock for %s", block["blockName"])


def trigger_prefect_flow_run(org: Org, connection_id: str):
    """
    Trigger a prefect flow run for a connection, first for reset and then for sync.
    """
    sync_org_task = OrgTask.objects.filter(
        org=org,
        connection_id=connection_id,
        task__slug=TASK_AIRBYTESYNC
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
        org=org,
        connection_id=connection_id,
        task__slug=TASK_AIRBYTERESET
    ).first()

    if reset_org_task is None:
        logger.error("Reset OrgTask not found")
        return None, "reset OrgTask not found"

    reset_dataflow = sync_dataflow_orgtask.dataflow.reset_conn_dataflow
    reset_deployment_id = reset_dataflow.deployment_id
    sync_deployment_id = sync_dataflow_orgtask.dataflow.deployment_id

    if not reset_deployment_id or not sync_deployment_id:
        logger.error("Deployment ID not found")
        return None, "deployment ID not found"

    org_server_block = OrgPrefectBlockv1.objects.filter(
        org=org, block_type=AIRBYTESERVER
    ).first()

    if not org_server_block:
        logger.error("Airbyte server block not found")
        return None, "airbyte server block not found"

    reset_sync_params = {
        "config": {
            "tasks": [
                setup_airbyte_sync_task_config(
                    reset_org_task,
                    org_server_block,
                    seq=1
                ).to_json(),
                setup_airbyte_sync_task_config(
                    sync_org_task,
                    org_server_block,
                    seq=2
                ).to_json()
            ],
            "org_slug": org.slug,
        }
    }

    try:
        prefect_service.create_deployment_flow_run(reset_deployment_id, reset_sync_params)
        logger.info("Successfully triggered Prefect flow run for reset")
    except Exception as error:
        logger.error("Failed to trigger Prefect flow run for reset: %s", error)
        return None, "failed to trigger Prefect flow run for reset"

    return None, None
