# this service is used to perform cleanup operations for objects related to an org
import uuid
import os
import shutil
from ninja.errors import HttpError

from ddpui.models.org import Org, OrgWarehouse, OrgPrefectBlockv1
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import DataflowOrgTask, OrgDataFlowv1, OrgTask

from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpprefect import prefect_service

from ddpui.ddpprefect import AIRBYTESERVER, DBTCLIPROFILE, SECRET
from ddpui.core.orgdbt_manager import DbtProjectManager

from ddpui.utils.constants import TASK_AIRBYTESYNC, TASK_AIRBYTERESET
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager
from ddpui.utils.file_storage.storage_factory import StorageFactory

logger = CustomLogger("ddpui")


class OrgCleanupServiceError(Exception):
    """Custom exception for OrgCleanupService errors"""

    pass


class OrgCleanupService:
    def __init__(self, org: Org, dry_run: bool = True):
        self.org = org
        self.dry_run = dry_run

    def delete_orchestrate_pipelines(self):
        """
        1. delete all (pipelines) deployments <> OrgDataFlowv1 of dataflow_type=orchestrate
        2. delete the OrgDataFlowv1 objects as well as the deployments in Prefect
        """

        for dataflow in OrgDataFlowv1.objects.filter(org=self.org, dataflow_type="orchestrate"):
            logger.info(
                f"Will delete orchestrate dataflow deployment: {dataflow.deployment_name} from prefect & DB"
            )
            if not self.dry_run:
                try:
                    prefect_service.delete_deployment_by_id(dataflow.deployment_id)
                except HttpError:
                    pass
                dataflow.delete()

                logger.info(
                    f"Deleted orchestrate dataflow deployment: {dataflow.deployment_name} from prefect & DB"
                )

    def delete_transformation_layer(self):
        """
        1. delete all transformation OrgTask(s) of type - git, dbt, dbtcloud. \
            We should make sure no tasks are being used in any orchestrate pipelines
        2. delete all deployments in prefect related to transformation OrgTask(s) eg. dbt run
        3. delete cli profile blocks, git secret blocks in prefect, db and in secrets manager
        4. delete the dbt workspace on disk
        5. delete OrgDbt object and the link to org
        """
        delete_transform_orgtask_ids = []
        for org_task in OrgTask.objects.filter(
            org=self.org, task__type__in=["dbt", "git", "dbtcloud"]
        ).all():
            if (
                DataflowOrgTask.objects.filter(
                    orgtask=org_task, dataflow__dataflow_type="orchestrate"
                ).count()
                > 0
            ):
                raise OrgCleanupServiceError(f"{str(org_task)} is being used in a deployment")
            delete_transform_orgtask_ids.append(org_task.id)

        # delete deployments in prefect related to transform tasks
        for dataflow_orgtask in DataflowOrgTask.objects.filter(
            orgtask__id__in=delete_transform_orgtask_ids
        ).all():
            dataflow = dataflow_orgtask.dataflow
            logger.info(
                f"Will delete dataflow deployment: {dataflow.deployment_name} from prefect & DB"
            )
            if not self.dry_run:
                try:
                    prefect_service.delete_deployment_by_id(dataflow.deployment_id)
                except HttpError:
                    pass
                dataflow.delete()
                logger.info(
                    f"Deleted dataflow deployment: {dataflow.deployment_name} from prefect & DB"
                )

        logger.info(f"will delete {len(delete_transform_orgtask_ids)} transform orgtasks")
        if not self.dry_run:
            OrgTask.objects.filter(id__in=delete_transform_orgtask_ids).delete()
            logger.info(f"deleted {len(delete_transform_orgtask_ids)} transform orgtasks")

        # remove cli profile blocks
        for dbt_cli_block in OrgPrefectBlockv1.objects.filter(
            org=self.org, block_type=DBTCLIPROFILE
        ).all():
            logger.info(
                f"will delete dbt cli profile block {dbt_cli_block.block_name} from prefect & DB"
            )
            if not self.dry_run:
                try:
                    prefect_service.delete_dbt_cli_profile_block(dbt_cli_block.block_id)
                    logger.info(
                        f"deleted dbt cli profile block {dbt_cli_block.block_name} from prefect"
                    )
                except Exception:  # pylint:disable=broad-exception-caught
                    pass
                dbt_cli_block.delete()
                logger.info(f"deleted dbt cli profile block {dbt_cli_block.block_name} from DB")

        # clear up github PAT from everywhere if exists
        try:
            orgdbt = self.org.dbt
        except Exception:
            orgdbt = None
            logger.info("No existing dbt workspace found")
            return

        for secret_block in OrgPrefectBlockv1.objects.filter(org=self.org, block_type=SECRET).all():
            logger.info(f"will delete secret block {secret_block.block_name} from prefect & DB")
            logger.info("will also delete github PAT if exists in secrets manager")
            if not self.dry_run:
                try:
                    prefect_service.delete_secret_block(secret_block.block_id)
                    logger.info(f"deleted secret block {secret_block.block_name} from prefect")
                except Exception:  # pylint:disable=broad-exception-caught
                    pass

                if orgdbt and orgdbt.gitrepo_access_token_secret:
                    secretsmanager.delete_github_pat(orgdbt.gitrepo_access_token_secret)
                    logger.info("deleted github PAT from secrets manager")

                secret_block.delete()
                logger.info(f"deleted secret block {secret_block.block_name} from DB")

        # delete the dbt workspace on disk and remove orgdbt references
        logger.info(
            "will delete dbt workspace on disk and remove orgdbt references tied to the org"
        )
        if not self.dry_run:
            if orgdbt:
                storage = StorageFactory.get_storage_adapter()
                dbt_project_dir = DbtProjectManager.get_dbt_project_dir(orgdbt)

                if storage.exists(str(dbt_project_dir)):
                    storage.delete_file(str(dbt_project_dir))

                logger.info("deleted dbt project directory from disk")

                orgdbt.delete()

                if self.org.dbt:
                    self.org.dbt = None
                    self.org.save()

    def delete_warehouse(self):
        """
        1. delete all connections
            - delete all deployments in prefect related to airbyte tasks
            - delete all connections in airbyte for the workspace
        2. delete the destinations in airbyte for all OrgWarehouse objects
        3. delete all warehouse credentials in secrets manager
        4. delete all OrgWarehouse object related to the org

        Note that this will also remove the connection syncs from the pipelines
        """
        for dataflow in OrgDataFlowv1.objects.filter(org=self.org, dataflow_type="manual"):
            all_tasks_are_airbyte_type = all(
                dataflow_orgtask.orgtask.task.type == "airbyte"
                for dataflow_orgtask in DataflowOrgTask.objects.filter(dataflow=dataflow)
            )
            if not all_tasks_are_airbyte_type:
                continue

            logger.info(
                f"Will delete airbyte dataflow deployment: {dataflow.deployment_name} from prefect & DB"
            )
            if not self.dry_run:
                try:
                    prefect_service.delete_deployment_by_id(dataflow.deployment_id)
                    logger.info(
                        f"Deleted airbyte dataflow deployment: {dataflow.deployment_name} from prefect"
                    )
                except HttpError:
                    pass
                dataflow.delete()
                logger.info(
                    f"Deleted airbyte dataflow deployment: {dataflow.deployment_name} from DB"
                )

        for org_task in OrgTask.objects.filter(org=self.org, task__type__in=["airbyte"]).all():
            logger.info(f"will delete connection {org_task.connection_id} in Airbyte and DB")
            if org_task.connection_id:
                try:
                    if not self.dry_run:
                        airbyte_service.delete_connection(
                            self.org.airbyte_workspace_id, org_task.connection_id
                        )
                        logger.info(f"deleted connection in Airbyte - {org_task.connection_id}")
                except Exception:
                    pass

            if not self.dry_run:
                org_task.delete()
                logger.info(
                    f"deleted orgtask - {str(org_task)} for connection {org_task.connection_id} from DB"
                )

        for warehouse in OrgWarehouse.objects.filter(org=self.org):
            logger.info(
                f"will delete destination {warehouse.airbyte_destination_id} from airbyte and db"
            )
            logger.info("will also delete warehouse credentials from secrets manager")
            if not self.dry_run:
                secretsmanager.delete_warehouse_credentials(warehouse)
                logger.info("deleted warehouse credentials from secrets manager")

                try:
                    airbyte_service.delete_destination(
                        self.org.airbyte_workspace_id, warehouse.airbyte_destination_id
                    )
                    logger.info(
                        f"deleted destination {warehouse.airbyte_destination_id} from airbyte"
                    )
                except Exception as err:
                    logger.error("error deleting destination in airbyte: %s", str(err))
                    pass

                warehouse.delete()
                logger.info(f"deleted warehouse {str(warehouse)} from db")

    def delete_airbyte_workspace(self):
        """
        deletes airbyte workspace along with all sources, destinations and connections
        """

        if not self.org.airbyte_workspace_id:
            logger.info(
                "no airbyte workspace id found for org, skipping deletion of airbyte workspace"
            )
            return

        for source in airbyte_service.get_sources(self.org.airbyte_workspace_id)["sources"]:
            logger.info("will delete source in Airbyte " + source["sourceId"])
            if not self.dry_run:
                try:
                    airbyte_service.delete_source(self.org.airbyte_workspace_id, source["sourceId"])
                    logger.info(f"deleted source in Airbyte - {source['sourceId']}")
                except Exception as err:
                    logger.error("error deleting source in airbyte: %s", str(err))
                    pass

        for destination in airbyte_service.get_destinations(self.org.airbyte_workspace_id)[
            "destinations"
        ]:
            logger.info("will delete destination in Airbyte " + destination["destinationId"])
            if not self.dry_run:
                try:
                    airbyte_service.delete_destination(
                        self.org.airbyte_workspace_id, destination["destinationId"]
                    )
                    logger.info(f"deleted destination in Airbyte - {destination['destinationId']}")
                except Exception as err:
                    logger.error("error deleting destination in airbyte: %s", str(err))
                    pass

        logger.info("will delete airbyte workspace %s", self.org.airbyte_workspace_id)
        if not self.dry_run:
            try:
                airbyte_service.delete_workspace(self.org.airbyte_workspace_id)
                logger.info(f"deleted airbyte workspace - {self.org.airbyte_workspace_id}")
            except Exception as err:
                logger.error("error deleting airbyte workspace: %s", str(err))
                pass

    def delete_orgusers(self):
        """
        deletes all org users
        """
        for orguser in OrgUser.objects.filter(org=self.org):
            logger.info("will delete orguser %s", orguser.user.email)
            if not self.dry_run:
                orguser.delete()

    def delete_edr_pipelines(self):
        """
        delete edr pipeline (in prefect & db) for the org setup for elementary reports
        """
        delete_orgtask_ids = [
            ot.id for ot in OrgTask.objects.filter(org=self.org, task__type__in=["edr"])
        ]

        delete_dataflow_ids = set()
        for dataflow_orgtask in DataflowOrgTask.objects.filter(orgtask__id__in=delete_orgtask_ids):
            delete_dataflow_ids.add(dataflow_orgtask.dataflow.id)

        for dataflow in OrgDataFlowv1.objects.filter(org=self.org, id__in=delete_dataflow_ids):
            logger.info("will delete dataflow in db and prefect - %s", dataflow.id)
            if not self.dry_run:
                try:
                    prefect_service.delete_deployment_by_id(dataflow.deployment_id)
                    logger.info(f"deleted deployment {dataflow.deployment_name} from prefect")
                except Exception as err:
                    logger.error(err)
                dataflow.delete()
                logger.info(f"deleted dataflow {dataflow.deployment_name} from db")

        logger.info(f"will delete {len(delete_orgtask_ids)} orgtasks")
        if not self.dry_run:
            OrgTask.objects.filter(id__in=delete_orgtask_ids).delete()
            logger.info(f"deleted {len(delete_orgtask_ids)} orgtasks")

    def delete_org(self):
        # delete all orchestrate pipelines
        self.delete_orchestrate_pipelines()

        # delete the transformation layer
        self.delete_transformation_layer()

        # delete the warehouse
        self.delete_warehouse()

        # delete airbyte workspace
        self.delete_airbyte_workspace()

        # delete org users
        self.delete_orgusers()

        # delete edr pipeline and tasks
        self.delete_edr_pipelines()

        # delete airbyte server block
        for block in OrgPrefectBlockv1.objects.filter(org=self.org, block_type=AIRBYTESERVER).all():
            logger.info(f"will delete airbyte server block {block.block_name} in prefect and db")
            if not self.dry_run:
                try:
                    prefect_service.prefect_delete_a_block(block.block_id)
                    logger.info(f"deleted airbyte server block {block.block_name} from prefect")
                except Exception as err:
                    logger.error("error deleting airbyte server block in prefect: %s", str(err))
                    pass
                block.delete()
                logger.info(f"deleted airbyte server block {block.block_name} from db")

        # delete org directory created on disk for transformation
        storage = StorageFactory.get_storage_adapter()
        org_dir = DbtProjectManager.get_org_dir(self.org)
        logger.info(f"will delete org directory from disk {org_dir}")
        if not self.dry_run:
            if storage.exists(str(org_dir)):
                storage.delete_file(str(org_dir))
                logger.info(f"deleted org directory from disk {org_dir}")

        # delete org object itself
        logger.info(f"will delete org {self.org.name} from DB")
        if not self.dry_run:
            self.org.delete()
            logger.info(f"deleted org {self.org.name} from DB")
