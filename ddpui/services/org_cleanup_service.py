# this service is used to perform cleanup operations for objects related to an org
import uuid
import os
import shutil
from ninja.errors import HttpError

from ddpui.models.org import Org, OrgWarehouse, OrgPrefectBlockv1
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import DataflowOrgTask, OrgDataFlowv1, OrgTask
from ddpui.models.userpreferences import UserPreferences
from ddpui.models.org_plans import OrgPlans
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.llm import LlmSession

from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpprefect import prefect_service

from ddpui.ddpprefect import AIRBYTESERVER, DBTCLIPROFILE, SECRET
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.core.git_manager import GitManager, GitManagerError

from ddpui.utils.constants import TASK_AIRBYTESYNC, TASK_AIRBYTERESET
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager
from ddpui.utils.s3_utils import bulk_delete_files, list_objects

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
        0. clean up everything Elementary-related (S3 reports, EDR OrgTask,
           EDR Prefect deployment) — elementary only makes sense with dbt,
           so tear it down first before the dbt state disappears.
        1. delete all transformation OrgTask(s) of type - git, dbt, dbtcloud. \
            We should make sure no tasks are being used in any orchestrate pipelines
        2. delete all deployments in prefect related to transformation OrgTask(s) eg. dbt run
        3. delete cli profile blocks, git secret blocks in prefect, db and in secrets manager
        4. delete the dbt workspace on disk
        5. delete OrgDbt object and the link to org
        """
        self.delete_elementary_setup()

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
        # iterate over distinct dataflows (a chained manual dataflow has
        # multiple DataflowOrgTask rows pointing at the same dataflow — the
        # cascade delete during the loop would otherwise invalidate later rows)
        dataflow_ids = set(
            DataflowOrgTask.objects.filter(
                orgtask__id__in=delete_transform_orgtask_ids
            ).values_list("dataflow_id", flat=True)
        )
        for dataflow in OrgDataFlowv1.objects.filter(id__in=dataflow_ids):
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

        # delete managed GitHub repository if it exists (before deleting PAT)
        if (
            orgdbt
            and orgdbt.is_repo_managed_by_system
            and orgdbt.gitrepo_url
            and orgdbt.gitrepo_access_token_secret
        ):
            logger.info(f"will delete managed GitHub repository: {orgdbt.gitrepo_url}")
            if not self.dry_run:
                try:
                    # Get PAT from secrets manager
                    pat = secretsmanager.retrieve_github_pat(orgdbt.gitrepo_access_token_secret)
                    if pat:
                        # Delete the repository using the static method
                        GitManager.delete_managed_repository(orgdbt.gitrepo_url, pat)
                        logger.info(f"deleted managed GitHub repository: {orgdbt.gitrepo_url}")
                    else:
                        logger.warning(
                            "could not retrieve PAT from secrets manager, skipping repository deletion"
                        )

                except GitManagerError as e:
                    logger.warning(f"failed to delete managed GitHub repository: {e.message}")
                    # Continue with cleanup even if GitHub deletion fails
                except Exception as e:
                    logger.warning(f"failed to delete managed GitHub repository: {str(e)}")
                    # Continue with cleanup even if GitHub deletion fails

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
                dbt_project_dir = DbtProjectManager.get_dbt_project_dir(orgdbt)

                if os.path.exists(dbt_project_dir):
                    shutil.rmtree(dbt_project_dir)

                logger.info("deleted dbt project directory from disk")

                orgdbt.delete()

                if self.org.dbt:
                    self.org.dbt = None
                    self.org.save()

    def delete_warehouse(self) -> dict:
        """
        1. delete all connections
            - delete all deployments in prefect related to airbyte tasks
            - delete all connections in airbyte for the workspace
        2. delete the destinations in airbyte for all OrgWarehouse objects
        3. delete all warehouse credentials in secrets manager
        4. delete all OrgWarehouse object related to the org

        Note that this will also remove the connection syncs from the pipelines

        Returns the warehouse's name/destination id, so callers (e.g. the
        API layer's audit log) don't need a separate fetch of their own.
        """
        warehouse_info = {"name": "", "airbyte_destination_id": ""}
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
            warehouse_info = {
                "name": warehouse.name,
                "airbyte_destination_id": warehouse.airbyte_destination_id,
            }

            logger.info(
                f"will delete destination {warehouse.airbyte_destination_id} from airbyte and db"
            )
            logger.info("will also delete warehouse credentials from secrets manager")
            if not self.dry_run:
                secretsmanager.delete_warehouse_credentials(warehouse)
                logger.info("deleted warehouse credentials from secrets manager")

                # Delete the dbt-profile Secret block (runner-flow artifact) if
                # this org has dbt set up. FK lives on OrgDbt; deleting the
                # Prefect block + OrgPrefectBlockv1 row is our responsibility
                # here since the warehouse is being torn down.
                dbt_profile_secret_block = (
                    self.org.dbt.dbt_profile_secret_block if self.org.dbt else None
                )
                if dbt_profile_secret_block:
                    try:
                        prefect_service.delete_secret_block(dbt_profile_secret_block.block_id)
                        logger.info(
                            f"deleted dbt-profile secret block {dbt_profile_secret_block.block_name} in prefect"
                        )
                    except Exception as err:  # pylint: disable=broad-exception-caught
                        logger.error(
                            "error deleting dbt-profile secret block %s in prefect: %s",
                            dbt_profile_secret_block.block_name,
                            str(err),
                        )
                    dbt_profile_secret_block.delete()
                    logger.info("deleted OrgPrefectBlockv1 row for dbt-profile secret block")

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

        return warehouse_info

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
        deletes all org users; first removes UserPreferences rows that FK to
        each OrgUser (they don't CASCADE) so the OrgUser delete doesn't
        violate the FK constraint.
        """
        for orguser in OrgUser.objects.filter(org=self.org):
            logger.info("will delete orguser %s", orguser.user.email)
            if not self.dry_run:
                n_prefs = UserPreferences.objects.filter(orguser=orguser).count()
                if n_prefs:
                    logger.info(
                        "deleting %s UserPreferences row(s) attached to orguser %s",
                        n_prefs,
                        orguser.user.email,
                    )
                    UserPreferences.objects.filter(orguser=orguser).delete()
                orguser.delete()

    def delete_elementary_setup(self):
        """Clean up everything Elementary-related for this org:
          - Prefect deployment(s) for the EDR send-report task
          - EDR OrgTask row(s)
          - Historical HTML reports in S3 (prefix `reports/<slug>.`)

        elementary_profiles/profiles.yml on disk is inside the dbt project
        directory, which gets nuked by delete_transformation_layer's
        shutil.rmtree — no separate cleanup needed for that.
        """
        # 1. EDR OrgTasks + their Prefect deployments
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

        logger.info(f"will delete {len(delete_orgtask_ids)} EDR orgtasks")
        if not self.dry_run:
            OrgTask.objects.filter(id__in=delete_orgtask_ids).delete()
            logger.info(f"deleted {len(delete_orgtask_ids)} EDR orgtasks")

        # 2. Elementary reports in S3 (prefix scoped to this org's slug)
        bucket = os.getenv("ELEMENTARY_S3_BUCKET")
        if not bucket:
            logger.info("ELEMENTARY_S3_BUCKET not configured — skipping S3 report cleanup")
            return

        prefix = f"reports/{self.org.slug}."
        # Paginate — an org could have hundreds of daily reports accumulated
        keys: list[str] = []
        start_after: str | None = None
        while True:
            try:
                page = list_objects(bucket, prefix=prefix, start_after=start_after, max_keys=1000)
            except Exception as err:  # pylint: disable=broad-exception-caught
                logger.error(f"failed to list S3 reports for {self.org.slug}: {err}")
                return
            if not page:
                break
            keys.extend(obj["Key"] for obj in page)
            if len(page) < 1000:
                break
            start_after = page[-1]["Key"]

        logger.info(f"will delete {len(keys)} S3 report(s) for {self.org.slug}")
        if not self.dry_run and keys:
            try:
                bulk_delete_files(bucket, keys)
                logger.info(f"deleted {len(keys)} S3 report(s) for {self.org.slug}")
            except Exception as err:  # pylint: disable=broad-exception-caught
                logger.error(f"failed to bulk delete S3 reports for {self.org.slug}: {err}")

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

        # Elementary cleanup (S3 reports + EDR pipeline + OrgTask) already
        # ran as part of delete_transformation_layer above — no separate call
        # needed here.

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
        org_dir = DbtProjectManager.get_org_dir(self.org)
        logger.info(f"will delete org directory from disk {org_dir}")
        if not self.dry_run:
            if os.path.exists(org_dir):
                shutil.rmtree(org_dir)
                logger.info(f"deleted org directory from disk {org_dir}")

        # delete rows whose FK to Org does not cascade at the DB level even
        # though the Django model declares on_delete=CASCADE (mismatch between
        # model definition and actual Postgres constraint from an older migration)
        for model_cls, label in (
            (OrgPlans, "OrgPlans"),
            (OrgPreferences, "OrgPreferences"),
            (LlmSession, "LlmSession"),
        ):
            n = model_cls.objects.filter(org=self.org).count()
            if n:
                logger.info(f"will delete {n} {label} row(s) for org")
                if not self.dry_run:
                    model_cls.objects.filter(org=self.org).delete()
                    logger.info(f"deleted {n} {label} row(s) for org")

        # delete org object itself
        logger.info(f"will delete org {self.org.name} from DB")
        if not self.dry_run:
            self.org.delete()
            logger.info(f"deleted org {self.org.name} from DB")
