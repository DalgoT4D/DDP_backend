from django.core.management.base import BaseCommand
from ddpui.models.org import (
    Org,
    OrgPrefectBlock,
    OrgPrefectBlockv1,
    OrgDataFlow,
    OrgDataFlowv1,
)
from ddpui.ddpprefect.schema import PrefectDataFlowUpdateSchema3
from ddpui.ddpprefect.prefect_service import get_deployment, update_dataflow_v1
from ddpui.models.tasks import OrgTask, Task, DataflowOrgTask
from ddpui.utils.constants import TASK_AIRBYTESYNC, AIRBYTE_SYNC_TIMEOUT
from ddpui.ddpprefect import AIRBYTESERVER, AIRBYTECONNECTION

import logging

logger = logging.getLogger("migration")


class Command(BaseCommand):
    help = "Process Commands in tasks-architecture folder"

    def add_arguments(self, parser):
        pass

    @staticmethod
    def migrate_airbyte_server_blocks(org: Org):
        """Create/update new server block"""
        old_block = OrgPrefectBlock.objects.filter(
            org=org, block_type=AIRBYTESERVER
        ).first()
        if not old_block:
            logger.info(f"Server block not found for the org '{org.slug}'")
            return

        new_block = OrgPrefectBlockv1.objects.filter(
            org=org, block_type=AIRBYTESERVER
        ).first()

        if not new_block:  # create
            logger.info(
                f"Creating new server block with id '{old_block.block_id}' in orgprefectblockv1"
            )
            OrgPrefectBlockv1.objects.create(
                org=org,
                block_id=old_block.block_id,
                block_name=old_block.block_name,
                block_type=AIRBYTESERVER,
            )
        else:  # update
            logger.info(
                f"Updating the newly created server block with id '{old_block.block_id}' in orgprefectblockv1"
            )
            new_block.block_id = old_block.block_id
            new_block.block_name = old_block.block_name
            new_block.save()

        # assert server block creation
        cnt = OrgPrefectBlockv1.objects.filter(
            org=org, block_type=AIRBYTESERVER
        ).count()
        logger.info(
            f"{'SUCCESS' if cnt == 1 else 'FAILURE'}: found {cnt} server block for org {org.slug} in orgprefectblockv1"
        )

        return new_block

    @staticmethod
    def migrate_manual_sync_conn_deployments(org: Org):
        """
        Create/update airbyte connection's manual deployments
        """

        # check if the server block exists or not
        server_block = OrgPrefectBlockv1.objects.filter(
            org=org, block_type=AIRBYTESERVER
        ).first()
        if not server_block:
            logger.info(
                "Skipping - migrating manual sync deployments because server block not found"
            )
            return

        logger.info("Found airbyte server block")

        airbyte_sync_task = Task.objects.filter(slug=TASK_AIRBYTESYNC).first()
        if not airbyte_sync_task:
            logger.info("run the tasks migration to populate the master table")
            return

        for old_dataflow in OrgDataFlow.objects.filter(
            org=org, dataflow_type="manual", deployment_name__startswith="manual-sync"
        ).all():
            new_dataflow = OrgDataFlowv1.objects.filter(
                org=org,
                dataflow_type="manual",
                deployment_id=old_dataflow.deployment_id,
            ).first()

            org_task = OrgTask.objects.filter(
                org=org,
                task=airbyte_sync_task,
                connection_id=old_dataflow.connection_id,
            ).first()

            if not org_task:
                org_task = OrgTask.objects.create(
                    org=org,
                    task=airbyte_sync_task,
                    connection_id=old_dataflow.connection_id,
                )

            # assert creation of orgtask
            cnt = OrgTask.objects.filter(
                org=org,
                task=airbyte_sync_task,
                connection_id=old_dataflow.connection_id,
            ).count()
            logger.info(
                f"{'SUCCESS' if cnt == 1 else 'FAILURE'}: found {cnt} orgtask in {org.slug}"
            )

            if not new_dataflow:  # create
                logger.info(
                    f"Creating new dataflow with id '{old_dataflow.deployment_id}' in orgdataflowv1"
                )
                new_dataflow = OrgDataFlowv1.objects.create(
                    org=org,
                    name=old_dataflow.name,
                    deployment_name=old_dataflow.deployment_name,
                    deployment_id=old_dataflow.deployment_id,
                    cron=old_dataflow.cron,
                    dataflow_type="manual",
                )

                DataflowOrgTask.objects.create(dataflow=new_dataflow, orgtask=org_task)

            # assert orgdataflowv1 creation
            cnt = OrgDataFlowv1.objects.filter(
                org=org,
                dataflow_type="manual",
                deployment_id=old_dataflow.deployment_id,
            ).count()
            logger.info(
                f"{'SUCCESS' if cnt == 1 else 'FAILURE'}: found {cnt} dataflowv1 in {org.slug}"
            )
            cnt = DataflowOrgTask.objects.filter(
                dataflow=new_dataflow, orgtask=org_task
            ).count()
            logger.info(
                f"{'SUCCESS' if cnt == 1 else 'FAILURE'}: found {cnt} datafloworgtask in {org.slug}"
            )

            # update deployment params
            deployment = None
            try:
                deployment = get_deployment(new_dataflow.deployment_id)
            except Exception as error:
                logger.info(
                    f"Something went wrong in fetching the deployment with id '{new_dataflow.deployment_id}'"
                )
                logger.exception(error)
                logger.info("skipping to next loop")
                continue

            params = deployment["parameters"]
            task_config = {
                "slug": airbyte_sync_task.slug,
                "type": AIRBYTECONNECTION,
                "seq": 1,
                "airbyte_server_block": server_block.block_name,
                "connection_id": org_task.connection_id,
                "timeout": AIRBYTE_SYNC_TIMEOUT,
            }
            params["config"] = {"tasks": [task_config]}
            logger.info(f"PARAMS {new_dataflow.deployment_id}")
            try:
                payload = PrefectDataFlowUpdateSchema3(
                    name=new_dataflow.name,  # wont be updated
                    connections=[],  # wont be updated
                    dbtTransform="ignore",  # wont be updated
                    cron=new_dataflow.cron if new_dataflow.cron else "",
                    deployment_params=params,
                )
                update_dataflow_v1(new_dataflow.deployment_id, payload)
                logger.info(
                    f"updated deployment params for the deployment with id {new_dataflow.deployment_id}"
                )
            except Exception as error:
                logger.info(
                    f"Something went wrong in updating the deployment params with id '{new_dataflow.deployment_id}'"
                )
                logger.exception(error)
                logger.info("skipping to next loop")
                continue

            # assert deployment params updation
            try:
                deployment = get_deployment(new_dataflow.deployment_id)
                if "config" not in deployment["parameters"]:
                    raise Exception(
                        "Didnt find the 'config' key in the deployment parameters"
                    )
                logger.info(
                    f"SUCCESS: found correct deployment params for id with {new_dataflow.deployment_id} "
                )
            except Exception as error:
                logger.info(
                    f"Assertion failed for deployment with id '{new_dataflow.deployment_id}'"
                )
                logger.exception(error)
                logger.info("skipping to next loop")
                continue

    def handle(self, *args, **options):
        for org in Org.objects.all():
            logger.info(f"Found org with slug '{org.slug}'")
            Command.migrate_airbyte_server_blocks(org)
            Command.migrate_manual_sync_conn_deployments(org)
