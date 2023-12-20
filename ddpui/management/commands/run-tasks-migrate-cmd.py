import os
import yaml
from pathlib import Path
from django.core.management.base import BaseCommand
from ddpui.models.org import (
    Org,
    OrgPrefectBlock,
    OrgPrefectBlockv1,
    OrgDataFlow,
    OrgDataFlowv1,
    OrgWarehouse,
)
from ddpui.models.orgjobs import DataflowBlock
from ddpui.utils import secretsmanager
from ddpui.ddpprefect.schema import PrefectDataFlowUpdateSchema3
from ddpui.ddpprefect.prefect_service import get_deployment, update_dataflow_v1
from ddpui.models.tasks import OrgTask, Task, DataflowOrgTask
from ddpui.utils.constants import (
    TASK_AIRBYTESYNC,
    AIRBYTE_SYNC_TIMEOUT,
    TASK_DBTRUN,
    TASK_GITPULL,
    TRANSFORM_TASKS_SEQ,
)
from ddpui.ddpprefect import (
    AIRBYTESERVER,
    AIRBYTECONNECTION,
    DBTCORE,
    DBTCLIPROFILE,
    SECRET,
    SHELLOPERATION,
)
from ddpui.ddpprefect.schema import (
    PrefectSecretBlockCreate,
    PrefectDbtTaskSetup,
    PrefectShellTaskSetup,
)
from ddpui.ddpprefect import prefect_service
from ddpui.ddpairbyte import airbyte_service

import logging

logger = logging.getLogger("migration")


class Command(BaseCommand):
    """migrate from old blocks to new tasks"""

    help = "Process Commands in tasks-architecture folder"

    def __init__(self):
        self.failures = []
        self.successes = []

    def add_arguments(self, parser):
        pass

    def migrate_airbyte_server_blocks(self, org: Org):
        """Create/update new server block"""
        old_block = OrgPrefectBlock.objects.filter(
            org=org, block_type=AIRBYTESERVER
        ).first()
        if not old_block:
            self.failures.append(f"Server block not found for the org '{org.slug}'")
            return

        new_block = OrgPrefectBlockv1.objects.filter(
            org=org, block_type=AIRBYTESERVER
        ).first()

        if not new_block:  # create
            logger.debug(
                f"Creating new server block with id '{old_block.block_id}' in orgprefectblockv1"
            )
            OrgPrefectBlockv1.objects.create(
                org=org,
                block_id=old_block.block_id,
                block_name=old_block.block_name,
                block_type=AIRBYTESERVER,
            )
        else:  # update
            logger.debug(
                f"Updating the newly created server block with id '{old_block.block_id}' in orgprefectblockv1"
            )
            new_block.block_id = old_block.block_id
            new_block.block_name = old_block.block_name
            new_block.save()

        # assert server block creation
        cnt = OrgPrefectBlockv1.objects.filter(
            org=org, block_type=AIRBYTESERVER
        ).count()
        if cnt == 0:
            self.failures.append(
                f"found 0 server blocks for org {org.slug} in orgprefectblockv1"
            )
        else:
            self.successes.append(
                f"found {cnt} server block(s) for org {org.slug} in orgprefectblockv1"
            )

        return new_block

    def migrate_manual_sync_conn_deployments(self, org: Org):
        """
        Create/update airbyte connection's manual deployments
        """

        # check if the server block exists or not
        server_block = OrgPrefectBlockv1.objects.filter(
            org=org, block_type=AIRBYTESERVER
        ).first()
        if not server_block:
            # self.failures.append(f"Server block not found for {org.slug}")
            return

        logger.debug("Found airbyte server block")

        airbyte_sync_task = Task.objects.filter(slug=TASK_AIRBYTESYNC).first()
        if not airbyte_sync_task:
            self.failures.append("run the tasks migration to populate the master table")
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
            if cnt == 0:
                self.failures.append(f"found 0 orgtasks in {org.slug}")
                return
            else:
                self.successes.append(f"found {cnt} orgtasks in {org.slug}")

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
            if cnt == 0:
                self.failures.append(
                    f"found 0 dataflowv1 in {org.slug} with deployment_id {old_dataflow.deployment_id}"
                )
            else:
                self.successes.append(
                    f"found {cnt} dataflowv1 in {org.slug} with deployment_id {old_dataflow.deployment_id}"
                )
            cnt = DataflowOrgTask.objects.filter(
                dataflow=new_dataflow, orgtask=org_task
            ).count()
            if cnt == 0:
                self.failures.append(
                    f"found 0 datafloworgtask in {org.slug} with deployment_id {old_dataflow.deployment_id}"
                )
            else:
                self.successes.append(
                    f"found {cnt} datafloworgtask in {org.slug} with deployment_id {old_dataflow.deployment_id}"
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
                    self.failures.append(
                        f"Missing 'config' key in the deployment parameters for {org.slug} {new_dataflow.deployment_id}"
                    )
                else:
                    self.successes.append(
                        f"Found correct deployment params for for {org.slug} {new_dataflow.deployment_id}"
                    )
            except Exception as error:
                self.failures.append(
                    f"Failed to fetch deployment with id '{new_dataflow.deployment_id}' for {org.slug}"
                )
                logger.exception(error)
                logger.info("skipping to next loop")
                continue

    def create_cli_profile_and_secret_blocks(self, org: Org):
        """
        - Create/Fetch the dbt cli profile block
        - Create/Fetch the secret block for git pull url if private repo & git token provided
        """
        # create the dbt cli profile block
        cli_profile_block = OrgPrefectBlockv1.objects.filter(
            org=org, block_type=DBTCLIPROFILE
        ).first()
        if cli_profile_block is None:
            # look through the dbt core operation blocks & find the cli_profile block reference
            # break as soon as find the dbt cli profile
            for dbt_core_block in OrgPrefectBlock.objects.filter(
                org=org, block_type=DBTCORE
            ).all():
                try:
                    # get_airbyte_connection_block_by_id function fetches any block by id not just airbyte connection block
                    res = prefect_service.get_airbyte_connection_block_by_id(
                        dbt_core_block.block_id
                    )
                    if (
                        "dbt_cli_profile" not in res["block_document_references"]
                        or "block_document"
                        not in res["block_document_references"]["dbt_cli_profile"]
                    ):
                        self.failures.append(
                            f"Couldnt find dbt cli profile reference in dbt core block: {dbt_core_block.block_name}. Going to next"
                        )
                        continue

                    cli_profile_blk_id = res["block_document_references"][
                        "dbt_cli_profile"
                    ]["block_document"]["id"]
                    cli_profile_blk_name = res["block_document_references"][
                        "dbt_cli_profile"
                    ]["block_document"]["name"]
                    self.successes.append(
                        f"Found the dbt cli profile block with name: {cli_profile_blk_name}, from dbt core block: {dbt_core_block.block_name}"
                    )

                    cli_profile_block = OrgPrefectBlockv1.objects.create(
                        org=org,
                        block_type=DBTCLIPROFILE,
                        block_name=cli_profile_blk_name,
                        block_id=cli_profile_blk_id,
                    )
                    break

                except Exception as error:
                    self.failures.append(
                        f"Couldnt fetch the dbt core operation block : {dbt_core_block.block_name}. Trying the next one."
                    )
                    logger.exception(error)

        # assert cli profile block creation
        self.successes.append(
            f"Using the dbt cli profile block {cli_profile_block.block_name} for {org.slug}"
        )
        cnt = OrgPrefectBlockv1.objects.filter(
            org=org, block_type=DBTCLIPROFILE
        ).count()
        if cnt == 1:
            self.successes.append(
                f"ASSERT: Found {cnt} dbt cli profile block for org {org.slug}"
            )
        else:
            self.failures.append(
                f"ASSERT: Found {cnt} dbt cli profile block for org {org.slug}"
            )

        # create the secret block
        secret_git_url_block = OrgPrefectBlockv1.objects.filter(
            org=org, block_type=SECRET
        ).first()
        if secret_git_url_block is None:
            shell_op_block = OrgPrefectBlock.objects.filter(
                org=org, block_type=SHELLOPERATION
            ).first()

            if not shell_op_block:
                self.successes.append(
                    f"SKIPPING: Shell operation block not found for the org {org.slug}"
                )
                return

            try:
                # get_airbyte_connection_block_by_id function fetches any block by id not just airbyte connection block
                res = prefect_service.get_airbyte_connection_block_by_id(
                    shell_op_block.block_id
                )

                # secret block name lies in the env of shell operation block
                if "data" not in res or "env" not in res["data"]:
                    self.failures.append(
                        f"Couldnt find secret block in env of shell op block: {shell_op_block.block_name}"
                    )
                    return

                if "secret-git-pull-url-block" not in res["data"]["env"]:
                    self.successes.append(
                        f"Secret block creation not needed, dbt repo for org {org.slug} seems to be public"
                    )
                    return

                secret_blkname = res["data"]["env"]["secret-git-pull-url-block"]
                res = prefect_service.get_secret_block_by_name(secret_blkname)
                secret_git_url_block = OrgPrefectBlockv1.objects.create(
                    org=org,
                    block_type=SECRET,
                    block_name=res["block_name"],
                    block_id=res["block_id"],
                )

            except Exception as error:
                self.failures.append(
                    f"Couldnt create the secret block to store git token url for shell op operation block : {shell_op_block.block_name}"
                )
                logger.exception(error)

        # assert secret block creation
        cnt = OrgPrefectBlockv1.objects.filter(org=org, block_type=SECRET).count()
        self.successes.append(f"ASSERT: Found {cnt} secret block for org {org.slug}")

    def migrate_transformation_blocks(self, org: Org):
        """
        - Migrate Dbt Core Operation & Shell Operation blocks to org tasks
        """

        # migrate dbt blocks ->  dbt tasks
        # migrate git-pull block -> git pull task
        dbt_blocks = OrgPrefectBlock.objects.filter(
            org=org, block_type__in=[DBTCORE, SHELLOPERATION]
        ).all()
        for old_block in dbt_blocks:
            task = None
            if old_block.block_name.endswith("git-pull"):  # its a git pull block
                task = Task.objects.filter(slug=TASK_GITPULL).first()
            else:  # its one of the dbt core block
                old_cmd = old_block.block_name.split(f"{old_block.dbt_target_schema}-")[
                    -1
                ]
                task = Task.objects.filter(slug__endswith=old_cmd).first()

            if not task:
                self.failures.append(f"Couldnt find the task {old_cmd} for {org.slug}")
                self.failures.append(
                    f"SKIPPING: migration of {old_block.block_name} for {org.slug}"
                )
                continue

            self.successes.append(
                f"Found corresponding task {task.slug} for {org.slug}"
            )

            org_task = OrgTask.objects.filter(org=org, task=task).first()
            if not org_task:
                self.successes.append(
                    f"Creating orgtask for task {task.slug} for {org.slug}"
                )
                org_task = OrgTask.objects.create(task=task, org=org)
                self.successes.append(
                    f"Created orgtask for task {task.slug} for {org.slug}"
                )

            cnt = OrgTask.objects.filter(org=org, task=task).count()
            if cnt == 1:
                self.successes.append(
                    f"ASSERT: Found {cnt} orgtask for {task.slug} for {org.slug}"
                )
            else:
                self.failures.append(
                    f"ASSERT: Found {cnt} orgtask for {task.slug} for {org.slug}"
                )

    def migrate_manual_dbt_run_deployments(self, org: Org):
        """
        Migrate the manual deployment to execute dbt run command
        """

        # check if cli block is created
        cli_profile_block = OrgPrefectBlockv1.objects.filter(
            org=org, block_type=DBTCLIPROFILE
        ).first()
        if not cli_profile_block:
            self.failures.append(
                "SKIPPING: Couldnt find the dbt cli profile block for {org.slug}"
            )
            return
        self.successes.append(
            f"Found dbt cli profile block : {cli_profile_block.block_name} for {org.slug}"
        )

        # dbt related params
        dbt_env_dir = Path(org.dbt.dbt_venv)
        if not dbt_env_dir.exists():
            self.failures.append("SKIPPING: couldnt find the dbt venv")
            return
        dbt_binary = str(dbt_env_dir / "venv/bin/dbt")
        dbtrepodir = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug / "dbtrepo"
        project_dir = str(dbtrepodir)
        target = org.dbt.default_schema

        org_task = OrgTask.objects.filter(org=org, task__slug=TASK_DBTRUN).first()

        if not org_task:
            self.failures.append("SKIPPING: couldnt find the orgtask for dbt-run")
            return

        for old_dataflow in OrgDataFlow.objects.filter(
            org=org, deployment_name__startswith="manual-run", dataflow_type="manual"
        ).all():
            new_dataflow = OrgDataFlowv1.objects.filter(
                org=org, deployment_id=old_dataflow.deployment_id
            ).first()

            if not new_dataflow:
                logger.info(
                    f"Creating a new dataflow for manual dbt run for org {org.slug}"
                )
                new_dataflow = OrgDataFlowv1.objects.create(
                    org=org,
                    name=old_dataflow.name,
                    deployment_name=old_dataflow.deployment_name,
                    deployment_id=old_dataflow.deployment_id,
                    cron=old_dataflow.cron,
                    dataflow_type="manual",
                )
                self.successes.append(
                    "Created dataflow in orgdataflowv1 for {org.slug}"
                )

            # assert new dataflow creation
            cnt = OrgDataFlowv1.objects.filter(
                org=org, deployment_id=old_dataflow.deployment_id
            ).count()
            if cnt == 1:
                self.successes.append(
                    f"Found {cnt} row(s) for orgdataflowv1 for deployment id {old_dataflow.deployment_id} for {org.slug}"
                )
            else:
                self.failures.append(
                    f"Found {cnt} row(s) for orgdataflowv1 for deployment id {old_dataflow.deployment_id} for {org.slug}"
                )

            # map dataflow to org task
            df_orgtask = DataflowOrgTask.objects.filter(
                dataflow=new_dataflow, orgtask=org_task
            ).first()
            if not df_orgtask:
                logger.info("Creating dataflow orgtask mapping for {org.slug}")
                df_orgtask = DataflowOrgTask.objects.create(
                    dataflow=new_dataflow, orgtask=org_task
                )
                logger.info("Created dataflow org task mapping for {org.slug}")
                self.successes.append("Created dataflow orgtask mapping for {org.slug}")

            cnt = DataflowOrgTask.objects.filter(
                dataflow=new_dataflow, orgtask=org_task
            ).count()
            self.successes.append(
                f"Found {1} row(s) for dataflow<->orgtask mapping for deployment id {old_dataflow.deployment_id} for {org.slug}"
            )

            # update deployment params
            deployment = None
            try:
                deployment = get_deployment(new_dataflow.deployment_id)
            except Exception as error:
                logger.info(
                    f"Something went wrong in fetching the deployment with id '{new_dataflow.deployment_id}' for {org.slug}"
                )
                logger.exception(error)
                logger.debug("skipping to next loop")
                continue

            params = deployment["parameters"]
            task_config = {
                "slug": org_task.task.slug,
                "type": DBTCORE,
                "seq": 1,
                "commands": [f"{dbt_binary} {org_task.task.command} --target {target}"],
                "env": {},
                "working_dir": project_dir,
                "profiles_dir": f"{project_dir}/profiles/",
                "project_dir": project_dir,
                "cli_profile_block": cli_profile_block.block_name,
                "cli_args": [],
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
                    f"updated deployment params for the deployment with id {new_dataflow.deployment_id} for {org.slug}"
                )
            except Exception as error:
                logger.info(
                    f"Something went wrong in updating the deployment params with id '{new_dataflow.deployment_id}' for {org.slug}"
                )
                logger.exception(error)
                logger.debug("skipping to next loop")
                continue

            # assert deployment params updation
            try:
                deployment = get_deployment(new_dataflow.deployment_id)
                if "config" not in deployment["parameters"]:
                    self.failures.append(
                        f"Missing 'config' key in the deployment parameters for {org.slug} {new_dataflow.deployment_id}"
                    )
                else:
                    self.successes.append(
                        f"Found correct deployment params for for {org.slug} {new_dataflow.deployment_id}"
                    )
            except Exception as error:
                self.failures.append(
                    f"Failed to fetch deployment with id '{new_dataflow.deployment_id}' for {org.slug}"
                )
                logger.exception(error)
                logger.debug("skipping to next loop")
                continue

    def migrate_org_pipelines(self, org: Org):
        """
        Migrate 'orchestrate' type dataflows
        """
        # check if the server block exists or not
        server_block = OrgPrefectBlockv1.objects.filter(
            org=org, block_type=AIRBYTESERVER
        ).first()
        if not server_block:
            self.failures.append(f"Server block not found for {org.slug}")
            return
        self.successes.append(
            f"Found airbyte server block: {server_block.block_name} for {org.slug}"
        )

        # check if cli block is created
        cli_profile_block = OrgPrefectBlockv1.objects.filter(
            org=org, block_type=DBTCLIPROFILE
        ).first()
        if not cli_profile_block:
            self.failures.append(
                f"SKIPPING: Couldnt find the dbt cli profile block for org {org.slug}"
            )
            return
        self.successes.append(
            f"Found dbt cli profile block : {cli_profile_block.block_name} for {org.slug}"
        )

        for old_dataflow in OrgDataFlow.objects.filter(
            org=org, dataflow_type="orchestrate"
        ).all():
            new_dataflow = OrgDataFlowv1.objects.filter(
                org=org, deployment_id=old_dataflow.deployment_id
            ).first()
            if not new_dataflow:
                logger.info("Creating a new pipeline in orgdataflowv1 for {org.slug}")
                new_dataflow = OrgDataFlowv1.objects.create(
                    org=org,
                    name=old_dataflow.name,
                    deployment_name=old_dataflow.deployment_name,
                    deployment_id=old_dataflow.deployment_id,
                    cron=old_dataflow.cron,
                    dataflow_type="orchestrate",
                )
                self.successes.append(
                    "Created pipeline in orgdataflowv1 for {org.slug}"
                )

            # assert new dataflow creation
            cnt = OrgDataFlowv1.objects.filter(
                org=org, deployment_id=old_dataflow.deployment_id
            ).count()
            if cnt == 1:
                self.successes.append(
                    f"Found {cnt} row(s) for orgdataflowv1 for deployment id {old_dataflow.deployment_id} for {org.slug}"
                )
            else:
                self.failures.append(
                    f"Found {cnt} row(s) for orgdataflowv1 for deployment id {old_dataflow.deployment_id} for {org.slug}"
                )

            # map orgtasks for this new deloyment in datafloworgtask table
            for dataflow_blk in DataflowBlock.objects.filter(
                dataflow=old_dataflow
            ).all():
                task = None
                org_task = None
                connection_id = None
                if dataflow_blk.opb.block_type == AIRBYTECONNECTION:
                    task = Task.objects.filter(slug=TASK_AIRBYTESYNC).first()
                    if not task:
                        self.failures.append(
                            "SKIPPING: airbyte connection task not found for {org.slug}"
                        )
                        continue

                    # fetch connection_id of the airbyte connection block from a manual dataflow
                    manual_df_blk = DataflowBlock.objects.filter(
                        opb=dataflow_blk.opb, dataflow__dataflow_type="manual"
                    ).first()

                    if not manual_df_blk:
                        self.failures.append(
                            f"SKIPPING: Couldnt find the dataflowblock mapping for a manual dataflow with the same airbyte connection block - {dataflow_blk.opb.block_name}"
                        )
                        continue

                    if not manual_df_blk.dataflow.connection_id:
                        self.failures.append(
                            f"SKIPPING: Couldnt find the connection_id in related manual dataflow : {manual_df_blk.dataflow.name}"
                        )
                        continue

                    self.successes.append(
                        f"Found airbyte conn block in prefect {dataflow_blk.opb.block_name}. Will use connection_id from here to create orgtask mapping of pipeline for {org.slug}"
                    )
                    connection_id = manual_df_blk.dataflow.connection_id

                elif dataflow_blk.opb.block_type == SHELLOPERATION:
                    task = Task.objects.filter(slug=TASK_GITPULL).first()
                    if not task:
                        self.failures.append(
                            f"SKIPPING: shell operation task not found for {org.slug}"
                        )
                        continue

                elif dataflow_blk.opb.block_type == DBTCORE:
                    old_cmd = dataflow_blk.opb.block_name.split(
                        f"{dataflow_blk.opb.dbt_target_schema}-"
                    )[-1]
                    task = Task.objects.filter(slug__endswith=old_cmd).first()
                    if not task:
                        self.failures.append(
                            f"SKIPPING: dbt core task {old_cmd} not found for {org.slug}"
                        )
                        continue

                if task is None:
                    logger.info("Unrecognized block_type")
                    self.failures.append(
                        f"Unrecognized block_type {dataflow_blk.opb.block_type} for {org.slug}"
                    )
                    continue

                org_task = OrgTask.objects.filter(
                    org=org,
                    task=task,
                    connection_id=connection_id,
                ).first()
                if not org_task:
                    logger.info(
                        f"Org task not found, creating a new one for {org.slug}"
                    )
                    org_task = OrgTask.objects.create(
                        org=org,
                        task=task,
                        connection_id=connection_id,
                    )
                    self.successes.append(
                        f"Created orgtask {org_task.task.slug} for new pipeline {new_dataflow.name} for {org.slug}"
                    )

                # assert orgtask
                cnt = OrgTask.objects.filter(
                    org=org,
                    task=task,
                    connection_id=connection_id,
                ).count()
                if cnt == 1:
                    self.successes.append(
                        f"ASSERT: Found {cnt} orgtask record for new pipeline {new_dataflow.name} and its opb {dataflow_blk.opb.block_name} for {org.slug}"
                    )
                else:
                    self.failures.append(
                        f"ASSERT: Found {cnt} orgtask record for new pipeline {new_dataflow.name} and its opb {dataflow_blk.opb.block_name} for {org.slug}"
                    )

                # create orgtask mapping to dataflow
                dataflow_orgtask = DataflowOrgTask.objects.filter(
                    dataflow=new_dataflow, orgtask=org_task
                ).first()
                if not dataflow_orgtask:
                    logger.info("Creating datafloworgtask")
                    dataflow_orgtask = DataflowOrgTask.objects.create(
                        dataflow=new_dataflow, orgtask=org_task
                    )
                    self.successes.append(f"Created datafloworgtask for {org.slug}")

                # assert datafloworgtask
                cnt = DataflowOrgTask.objects.filter(
                    dataflow=new_dataflow, orgtask=org_task
                ).count()
                if cnt == 1:
                    self.successes.append(
                        f"ASSERT: Found {cnt} datafloworgtask record for new pipeline {new_dataflow.name} and its opb {dataflow_blk.opb.block_name} for {org.slug}"
                    )
                else:
                    self.failures.append(
                        f"ASSERT: Found {cnt} datafloworgtask record for new pipeline {new_dataflow.name} and its opb {dataflow_blk.opb.block_name} for {org.slug}"
                    )

            self.successes.append(
                f"Fetching deployment params for dataflow {new_dataflow.deployment_name} & updating the params"
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
            seq = 0
            tasks = []
            # convert airbyte block(s) to task config param & push
            if "airbyte_blocks" in params and len(params["airbyte_blocks"]) > 0:
                self.successes.append(
                    f"Pipeline : {new_dataflow.deployment_name} has airbyte syncs. Processing them now"
                )
                for airbyte_block in sorted(
                    params["airbyte_blocks"], key=lambda x: x["seq"]
                ):
                    airbyte_block_name = airbyte_block["blockName"]

                    org_prefect_blk = OrgPrefectBlock.objects.filter(
                        org=org, block_name=airbyte_block_name
                    ).first()
                    if not org_prefect_blk:
                        self.failures.append(
                            f"SKIPPING: couldnt convert airbyte block named {airbyte_block_name}, to task config params. orgprefectblock not found"
                        )
                        continue

                    # fetch connection id for this block
                    dataflow_block = DataflowBlock.objects.filter(
                        opb=org_prefect_blk, dataflow__dataflow_type="manual"
                    ).first()
                    if not dataflow_block:
                        self.failures.append(
                            f"SKIPPING: couldnt fetch the connection_id of airbyte block named {airbyte_block_name}. dataflowblock mapping not found that has connection_id"
                        )
                        continue

                    connection_id = dataflow_block.dataflow.connection_id
                    if not connection_id:
                        self.failures.append(
                            f"SKIPPING: connection_id of airbyte block named {airbyte_block_name} is null in dataflowblock mapping"
                        )
                        continue

                    org_task = OrgTask.objects.filter(
                        org=org,
                        task__slug=TASK_AIRBYTESYNC,
                        connection_id=connection_id,
                    ).first()

                    if not org_task:
                        self.failures.append(
                            f"SKIPPING: orgtask not found for airbyte block named {airbyte_block_name} with connection_id {connection_id}"
                        )
                        continue

                    seq += 1
                    task_config = {
                        "seq": seq,
                        "slug": org_task.task.slug,
                        "type": AIRBYTECONNECTION,
                        "airbyte_server_block": server_block.block_name,
                        "connection_id": connection_id,
                        "timeout": AIRBYTE_SYNC_TIMEOUT,
                    }
                    tasks.append(task_config)

            # convert airbyte block(s) to task config param & push
            if "dbt_blocks" in params and len(params["dbt_blocks"]) > 0:
                self.successes.append(
                    f"Pipeline : {new_dataflow.deployment_name} has dbt transform on. Processing it now"
                )

                # dbt params
                dbt_env_dir = Path(org.dbt.dbt_venv)
                if not dbt_env_dir.exists():
                    self.failures.append("dbt env not found")
                    return

                dbt_binary = str(dbt_env_dir / "venv/bin/dbt")
                dbtrepodir = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug / "dbtrepo"
                project_dir = str(dbtrepodir)
                target = org.dbt.default_schema

                for dbt_block in sorted(params["dbt_blocks"], key=lambda x: x["seq"]):
                    dbt_block_name = dbt_block["blockName"]
                    dbt_block_type = dbt_block["blockType"]

                    org_prefect_blk = OrgPrefectBlock.objects.filter(
                        org=org, block_name=dbt_block_name
                    ).first()
                    if not org_prefect_blk:
                        self.failures.append(
                            f"SKIPPING: couldnt convert dbt block named {dbt_block_name}, to task config params. orgprefectblock not found"
                        )
                        continue

                    task = None
                    if org_prefect_blk.block_name.endswith(
                        "git-pull"
                    ):  # its a git pull block
                        task = Task.objects.filter(slug=TASK_GITPULL).first()
                    else:  # its one of the dbt core block
                        old_cmd = org_prefect_blk.block_name.split(
                            f"{org_prefect_blk.dbt_target_schema}-"
                        )[-1]
                        task = Task.objects.filter(slug__endswith=old_cmd).first()

                    if not task:
                        self.failures.append(
                            f"Couldnt find the task {old_cmd} for org {org.slug}, for dbt block named {dbt_block_name}"
                        )
                        self.failures.append(
                            f"SKIPPING: couldnt convert dbt block named {dbt_block_name} to task config, master task not found"
                        )
                        continue

                    org_task = OrgTask.objects.filter(org=org, task=task).first()
                    if org_task.task.slug == TASK_DBTRUN:
                        dbt_core_task_setup = PrefectDbtTaskSetup(
                            seq=TRANSFORM_TASKS_SEQ[org_task.task.slug] + seq,
                            slug=org_task.task.slug,
                            commands=[
                                f"{dbt_binary} {org_task.task.command} --target {target}"
                            ],
                            type=DBTCORE,
                            env={},
                            working_dir=project_dir,
                            profiles_dir=f"{project_dir}/profiles/",
                            project_dir=project_dir,
                            cli_profile_block=cli_profile_block.block_name,
                            cli_args=[],
                        )

                        task_config = dict(dbt_core_task_setup)
                        tasks.append(task_config)

                    elif org_task.task.slug == TASK_GITPULL:
                        shell_env = {"secret-git-pull-url-block": ""}

                        gitpull_secret_block = OrgPrefectBlockv1.objects.filter(
                            org=org, block_type=SECRET, block_name__contains="git-pull"
                        ).first()

                        if gitpull_secret_block is not None:
                            shell_env[
                                "secret-git-pull-url-block"
                            ] = gitpull_secret_block.block_name

                        shell_task_setup = PrefectShellTaskSetup(
                            commands=["git pull"],
                            working_dir=project_dir,
                            env=shell_env,
                            slug=org_task.task.slug,
                            type=SHELLOPERATION,
                            seq=TRANSFORM_TASKS_SEQ[org_task.task.slug] + seq,
                        )

                        task_config = dict(shell_task_setup)
                        tasks.append(task_config)

            params["config"] = {"tasks": tasks}
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
                    f"updated deployment params for the deployment with id {new_dataflow.deployment_id} for {org.slug}"
                )
            except Exception as error:
                logger.info(
                    f"Something went wrong in updating the deployment params with id '{new_dataflow.deployment_id}' for {org.slug}"
                )
                logger.exception(error)
                logger.debug("skipping to next loop")
                continue

    def handle(self, *args, **options):
        for org in Org.objects.all():
            self.migrate_airbyte_server_blocks(org)
            self.migrate_manual_sync_conn_deployments(org)
            self.create_cli_profile_and_secret_blocks(org)
            self.migrate_transformation_blocks(org)
            self.migrate_manual_dbt_run_deployments(org)
            self.migrate_org_pipelines(org)

        # show summary
        print("=" * 80)
        print("SUCCESSES")
        print("=" * 80)
        for success in self.successes:
            print("SUCCESS " + success)
        print("=" * 80)
        print("FAILURES")
        print("=" * 80)
        for failure in self.failures:
            print("FAILURE " + failure)
