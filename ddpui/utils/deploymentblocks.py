from ddpui.models.org import OrgPrefectBlock, OrgDataFlow
from ddpui.models.orgjobs import DataflowBlock
from ddpui.ddpprefect import prefect_service

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
