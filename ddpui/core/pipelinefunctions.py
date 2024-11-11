"""
functions to work with pipelies/dataflows
do not raise http errors here
"""

from pathlib import Path
from typing import Union
from functools import cmp_to_key
from django.db import transaction
from ninja.errors import HttpError

from ddpui.models.tasks import OrgTask, DataflowOrgTask, TaskLock, TaskLockStatus
from ddpui.models.org import Org, OrgPrefectBlockv1, OrgDataFlowv1
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger
from ddpui.ddpprefect.schema import (
    PrefectDbtTaskSetup,
    PrefectShellTaskSetup,
    PrefectAirbyteSyncTaskSetup,
    PrefectAirbyteRefreshSchemaTaskSetup,
    PrefectDataFlowUpdateSchema3,
)
from ddpui.ddpprefect import (
    AIRBYTECONNECTION,
    DBTCORE,
    SECRET,
    SHELLOPERATION,
    prefect_service,
    FLOW_RUN_PENDING_STATE_TYPE,
    FLOW_RUN_RUNNING_STATE_TYPE,
    FLOW_RUN_SCHEDULED_STATE_TYPE,
)
from ddpui.utils.constants import (
    AIRBYTE_SYNC_TIMEOUT,
    TASK_GITPULL,
    TASK_AIRBYTESYNC,
    TASK_GENERATE_EDR,
    TASK_AIRBYTERESET,
    UPDATE_SCHEMA,
    TRANSFORM_TASKS_SEQ,
)
from ddpui.ddpdbt.schema import DbtProjectParams

logger = CustomLogger("ddpui")

####################### big config dictionaries ##################################


def setup_airbyte_sync_task_config(
    org_task: OrgTask, server_block: OrgPrefectBlockv1, seq: int = 1
):
    """constructs the prefect payload for an airbyte sync or reset"""
    return PrefectAirbyteSyncTaskSetup(
        seq=seq,
        slug=org_task.task.slug,
        type=AIRBYTECONNECTION,
        airbyte_server_block=server_block.block_name,
        connection_id=org_task.connection_id,
        timeout=AIRBYTE_SYNC_TIMEOUT,
        orgtask_uuid=str(org_task.uuid),
    )


def setup_airbyte_update_schema_task_config(
    org_task: OrgTask,
    server_block: OrgPrefectBlockv1,
    catalog_diff: dict,
    seq: int = 1,
):
    """constructs the prefect payload for an airbyte refresh schema task config"""
    return PrefectAirbyteRefreshSchemaTaskSetup(
        seq=seq,
        slug=UPDATE_SCHEMA,
        type=AIRBYTECONNECTION,
        airbyte_server_block=server_block.block_name,
        connection_id=org_task.connection_id,
        timeout=AIRBYTE_SYNC_TIMEOUT,
        orgtask_uuid=str(org_task.uuid),
        catalog_diff=catalog_diff,
    )


def setup_dbt_core_task_config(
    org_task: OrgTask,
    cli_profile_block: OrgPrefectBlockv1,
    dbt_project_params: DbtProjectParams,
    seq: int = 1,
):
    """constructs the prefect payload for a dbt job"""
    return PrefectDbtTaskSetup(
        seq=seq,
        slug=org_task.task.slug,
        commands=[f"{dbt_project_params.dbt_binary} {org_task.get_task_parameters()}"],
        type=DBTCORE,
        env={},
        working_dir=dbt_project_params.project_dir,
        profiles_dir=f"{dbt_project_params.project_dir}/profiles/",
        project_dir=dbt_project_params.project_dir,
        cli_profile_block=cli_profile_block.block_name,
        cli_args=[],
        orgtask_uuid=str(org_task.uuid),
    )


def setup_git_pull_shell_task_config(
    org_task: OrgTask,
    project_dir: str,
    gitpull_secret_block: OrgPrefectBlockv1,
    seq: int = 1,
):
    """constructs the prefect payload for a git pull"""
    shell_env = {"secret-git-pull-url-block": ""}

    if gitpull_secret_block is not None:
        shell_env["secret-git-pull-url-block"] = gitpull_secret_block.block_name

    return PrefectShellTaskSetup(
        commands=[f"git {org_task.get_task_parameters()}"],
        working_dir=project_dir,
        env=shell_env,
        slug=org_task.task.slug,
        type=SHELLOPERATION,
        seq=seq,
        orgtask_uuid=str(org_task.uuid),
    )


def setup_edr_send_report_task_config(
    org_task: OrgTask, project_dir: str, venv_binary: str, seq: int = 1
):
    """constructs the prefect payload for edr"""
    # shell_env = {"PATH": str(dbt_env_dir / "venv/bin"), "shell": "/bin/bash"}
    shell_env = {"PATH": venv_binary, "shell": "/bin/bash"}  # venv_binary: ..../venv/bin
    return PrefectShellTaskSetup(
        commands=[
            org_task.task.type + " " + org_task.get_task_parameters(),
        ],
        working_dir=project_dir,
        env=shell_env,
        slug=org_task.task.slug,
        type=SHELLOPERATION,
        seq=seq,
        orgtask_uuid=str(org_task.uuid),
    )


def pipeline_with_orgtasks(
    org: Org,
    org_tasks: list[OrgTask],
    server_block: OrgPrefectBlockv1 = None,
    cli_block: OrgPrefectBlockv1 = None,
    dbt_project_params: DbtProjectParams = None,
    start_seq: int = 0,
):
    """
    Returns a list of task configs for a pipeline;
    This assumes the list of orgtasks is in the correct sequence
    """
    task_configs = []

    for org_task in org_tasks:
        task_config = None
        if org_task.task.slug == TASK_AIRBYTERESET:
            task_config = setup_airbyte_sync_task_config(org_task, server_block).to_json()
        elif org_task.task.slug == TASK_AIRBYTESYNC:
            task_config = setup_airbyte_sync_task_config(org_task, server_block).to_json()
        elif org_task.task.slug == TASK_GITPULL:
            gitpull_secret_block = OrgPrefectBlockv1.objects.filter(
                org=org, block_type=SECRET, block_name__contains="git-pull"
            ).first()

            if not gitpull_secret_block:
                logger.info(
                    f"secret block for {org_task.task.slug} not found in org prefect blocks;"
                )
            task_config = setup_git_pull_shell_task_config(
                org_task, dbt_project_params.project_dir, gitpull_secret_block
            ).to_json()
        elif org_task.task.slug == TASK_GENERATE_EDR:
            task_config = setup_edr_send_report_task_config(
                org_task,
                dbt_project_params.project_dir,
                dbt_project_params.venv_binary,
            ).to_json()
        else:
            task_config = setup_dbt_core_task_config(
                org_task, cli_block, dbt_project_params
            ).to_json()

        if task_config:
            task_configs.append(task_config)
            task_config["seq"] = start_seq
            start_seq += 1

    return task_configs, None


def fetch_pipeline_lock(dataflow: OrgDataFlowv1):
    """
    fetch the lock status of an dataflow/deployment
    """
    org_task_ids = DataflowOrgTask.objects.filter(dataflow=dataflow).values_list(
        "orgtask_id", flat=True
    )
    lock = TaskLock.objects.filter(orgtask_id__in=org_task_ids).first()
    if lock:
        lock_status = TaskLockStatus.QUEUED
        if lock.flow_run_id:
            flow_run = prefect_service.get_flow_run(lock.flow_run_id)
            if flow_run and flow_run["state_type"] in ["SCHEDULED", "PENDING"]:
                lock_status = TaskLockStatus.QUEUED
            elif flow_run and flow_run["state_type"] == "RUNNING":
                lock_status = TaskLockStatus.RUNNING
            else:
                lock_status = TaskLockStatus.COMPLETED

        return {
            "lockedBy": lock.locked_by.user.email,
            "lockedAt": lock.locked_at,
            "flowRunId": lock.flow_run_id,
            "status": (lock_status if lock.locking_dataflow == dataflow else TaskLockStatus.LOCKED),
        }

    return None


def fetch_pipeline_lock_v1(dataflow: OrgDataFlowv1, lock: Union[TaskLock, None]):
    """
    fetch the lock status of an dataflow/deployment
    """
    if lock:
        lock_status = TaskLockStatus.QUEUED
        if lock.flow_run_id:
            flow_run = prefect_service.get_flow_run(lock.flow_run_id)  # can taken from db now
            if flow_run and flow_run["state_type"] in [
                FLOW_RUN_SCHEDULED_STATE_TYPE,
                FLOW_RUN_PENDING_STATE_TYPE,
            ]:
                lock_status = TaskLockStatus.QUEUED
            elif flow_run and flow_run["state_type"] == FLOW_RUN_RUNNING_STATE_TYPE:
                lock_status = TaskLockStatus.RUNNING
            else:
                lock_status = TaskLockStatus.COMPLETED

        return {
            "lockedBy": lock.locked_by.user.email,
            "lockedAt": lock.locked_at,
            "flowRunId": lock.flow_run_id,
            "status": (lock_status if lock.locking_dataflow == dataflow else TaskLockStatus.LOCKED),
        }

    return None


def lock_tasks_for_dataflow(orguser: OrgUser, dataflow: OrgDataFlowv1, org_tasks: list[OrgTask]):
    """locks the orgtasks; if its any of the orgtasks are already locked, it will raise an error"""

    orgtask_ids = [org_task.id for org_task in org_tasks]
    lock = TaskLock.objects.filter(orgtask_id__in=orgtask_ids).first()
    if lock:
        logger.info(f"{lock.locked_by.user.email} is running this pipeline right now")
        raise HttpError(400, f"{lock.locked_by.user.email} is running this pipeline right now")

    locks = []
    try:
        with transaction.atomic():
            for org_task in org_tasks:
                task_lock = TaskLock.objects.create(
                    orgtask=org_task,
                    locked_by=orguser,
                    locking_dataflow=dataflow,
                )
                locks.append(task_lock)
    except Exception as error:
        raise HttpError(400, "Someone else is trying to run this pipeline... try again") from error
    return locks


def fix_transform_tasks_seq_dataflow(deployment_id: str):
    """corrects the order of the transform tasks in a prefect deployment config"""

    def task_config_comparator(task1, task2):
        if TRANSFORM_TASKS_SEQ[task1["slug"]] > TRANSFORM_TASKS_SEQ[task2["slug"]]:
            return 1
        elif TRANSFORM_TASKS_SEQ[task1["slug"]] < TRANSFORM_TASKS_SEQ[task2["slug"]]:
            return -1
        else:
            return 0

    def dataflow_orgtask_comparator(dfot1: DataflowOrgTask, dfot2: DataflowOrgTask):
        if (
            TRANSFORM_TASKS_SEQ[dfot1.orgtask.task.slug]
            > TRANSFORM_TASKS_SEQ[dfot2.orgtask.task.slug]
        ):
            return 1
        elif (
            TRANSFORM_TASKS_SEQ[dfot1.orgtask.task.slug]
            < TRANSFORM_TASKS_SEQ[dfot2.orgtask.task.slug]
        ):
            return -1
        else:
            return 0

    try:
        deployment = prefect_service.get_deployment(deployment_id)
    except Exception as e:
        logger.error(f"Error getting deployment for {deployment_id}: {str(e)}")
        return

    params = deployment.get("parameters", {})
    config = params.get("config", {})
    tasks = config.get("tasks", [])
    transform_tasks = [task for task in tasks if task["slug"] in TRANSFORM_TASKS_SEQ]
    other_tasks = [task for task in tasks if task["slug"] not in TRANSFORM_TASKS_SEQ]

    updated_transform_tasks = sorted(transform_tasks, key=cmp_to_key(task_config_comparator))
    for i, task in enumerate(updated_transform_tasks):
        task["seq"] = i + len(other_tasks)

    tasks = other_tasks + updated_transform_tasks
    try:
        prefect_service.update_dataflow_v1(
            deployment_id,
            PrefectDataFlowUpdateSchema3(deployment_params={"config": config}),
        )
        logger.info(f"Update the seq for deployment {deployment_id}")
    except Exception as err:
        logger.error(
            f"Error while updating deployment params for deployment_id {deployment_id}: {err}"
        )

    # update the seq in datafloworgtask mapping
    dataflow_orgtasks = DataflowOrgTask.objects.filter(
        dataflow__deployment_id=deployment_id,
    ).all()

    transform_dfots = [
        dfot for dfot in dataflow_orgtasks if dfot.orgtask.task.slug in TRANSFORM_TASKS_SEQ
    ]
    other_dfots = [
        dfot for dfot in dataflow_orgtasks if dfot.orgtask.task.slug not in TRANSFORM_TASKS_SEQ
    ]

    sorted_transform_dataflow_orgtasks = sorted(
        transform_dfots, key=cmp_to_key(dataflow_orgtask_comparator)
    )

    for i, dfot in enumerate(sorted_transform_dataflow_orgtasks):
        dfot.seq = i + len(other_dfots)
        dfot.save()

    logger.info("Updated the seq for datafloworgtasks for the transform tasks")
