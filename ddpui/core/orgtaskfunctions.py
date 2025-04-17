"""
functions to work with transform related tasks or orgtasks in general
do not raise http errors here
"""

import uuid
from typing import Union
from pathlib import Path
import yaml
from ddpui.models.tasks import OrgTask, Task, DataflowOrgTask, TaskLock, TaskLockStatus
from ddpui.models.org import (
    Org,
    OrgPrefectBlockv1,
    OrgDataFlowv1,
    TransformType,
    OrgDbt,
)
from ddpui.utils.custom_logger import CustomLogger
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
)
from ddpui.ddpprefect import (
    MANUL_DBT_WORK_QUEUE,
    FLOW_RUN_COMPLETED_STATE_TYPE,
    FLOW_RUN_PENDING_STATE_TYPE,
    FLOW_RUN_RUNNING_STATE_TYPE,
    FLOW_RUN_SCHEDULED_STATE_TYPE,
)
from ddpui.ddpdbt.schema import DbtCloudJobParams, DbtProjectParams
from ddpui.ddpprefect import prefect_service
from ddpui.core.pipelinefunctions import setup_dbt_core_task_config, setup_dbt_cloud_task_config
from ddpui.utils.constants import TASK_DBTRUN, TASK_GENERATE_EDR
from ddpui.utils.helpers import generate_hash_id
from ddpui.models.flow_runs import PrefectFlowRun

logger = CustomLogger("ddpui")


def create_default_transform_tasks(
    org: Org, cli_profile_block: OrgPrefectBlockv1, dbt_project_params: DbtProjectParams
):
    """Create all the transform (git, dbt) tasks"""
    if org.dbt is None:
        raise ValueError("dbt is not configured for this org")

    # if transform_type is "ui" then we don't set up git-pull
    task_types = ["dbt", "git"] if org.dbt.transform_type == TransformType.GIT else ["dbt"]
    for task in Task.objects.filter(type__in=task_types, is_system=True).all():
        org_task = OrgTask.objects.create(org=org, task=task, uuid=uuid.uuid4())

        if task.slug == TASK_DBTRUN:
            # create deployment
            create_prefect_deployment_for_dbtcore_task(
                org_task, cli_profile_block, dbt_project_params
            )

    return None, None


def fetch_elementary_profile_target(orgdbt: OrgDbt) -> str:
    """fetch the target from the elementary profiles yaml file"""
    elementary_target = "default"

    # parse the yaml file
    project_dir = Path(orgdbt.project_dir) / "dbtrepo"
    elementary_profiles_yml = project_dir / "elementary_profiles" / "profiles.yml"

    if not elementary_profiles_yml.exists():
        logger.info(
            f"couldn't find the profiles.yml file for the elementary setup for orgdbt {orgdbt.id}. setting target to default"
        )
    else:
        with open(elementary_profiles_yml, "r") as file:
            config = yaml.safe_load(file)
        elementary_config = config.get("elementary", {})
        outputs = elementary_config.get("outputs", {})
        targets = list(outputs.keys())
        if len(targets) > 0:
            logger.info(
                f"elementary profiles {str(targets)} found for orgdbt {orgdbt.id}. setting to the first one - {targets[0]}"
            )
            elementary_target = targets[0]

    return elementary_target


def get_edr_send_report_task(org: Org, **kwargs) -> OrgTask | None:
    """creates an OrgTask for edr send-report"""
    task = Task.objects.filter(slug=TASK_GENERATE_EDR).first()
    if task is None:
        raise ValueError("TASK_GENERATE_EDR not found")

    if kwargs.get("overwrite") or kwargs.get("create"):
        options = {
            "profiles-dir": "elementary_profiles",
            "bucket-file-path": f"reports/{org.slug}.TODAYS_DATE.html",
            "profile-target": fetch_elementary_profile_target(org.dbt),
        }

    org_task = OrgTask.objects.filter(task__slug=TASK_GENERATE_EDR, org=org).first()
    if org_task:
        if kwargs.get("overwrite"):
            org_task.parameters["options"] = options
            org_task.save()
        return org_task

    if kwargs.get("create"):
        org_task = OrgTask.objects.create(
            org=org,
            task=task,
            uuid=uuid.uuid4(),
            parameters={"options": options},
        )
    return org_task


def create_prefect_deployment_for_dbtcore_task(
    org_task: OrgTask,
    credentials_profile_block: OrgPrefectBlockv1,
    dbt_project_params: Union[DbtProjectParams, DbtCloudJobParams],
):
    """
    - create a prefect deployment for a single dbt command or dbt cloud job
    - save the deployment id to an OrgDataFlowv1 object
    - for dbt core operation; the credentials_profile_block is cli profile block
    - for dbt cloud job; the credentials_profile_block is dbt cloud credentials block
    """
    hash_code = generate_hash_id(8)
    deployment_name = f"manual-{org_task.org.slug}-{org_task.task.slug}-{hash_code}"

    tasks = []
    if org_task.task.type == "dbt":
        tasks = [
            setup_dbt_core_task_config(
                org_task, credentials_profile_block, dbt_project_params
            ).to_json()
        ]
    elif org_task.task.type == "dbtcloud":
        tasks = [
            setup_dbt_cloud_task_config(
                org_task, credentials_profile_block, dbt_project_params
            ).to_json()
        ]

    dataflow = prefect_service.create_dataflow_v1(
        PrefectDataFlowCreateSchema3(
            deployment_name=deployment_name,
            flow_name=deployment_name,
            orgslug=org_task.org.slug,
            deployment_params={
                "config": {
                    "tasks": tasks,
                    "org_slug": org_task.org.slug,
                }
            },
        ),
        MANUL_DBT_WORK_QUEUE,
    )

    # store deployment record in django db
    existing_dataflow = OrgDataFlowv1.objects.filter(
        deployment_id=dataflow["deployment"]["id"]
    ).first()
    if existing_dataflow:
        existing_dataflow.delete()

    new_dataflow = OrgDataFlowv1.objects.create(
        org=org_task.org,
        name=deployment_name,
        deployment_name=dataflow["deployment"]["name"],
        deployment_id=dataflow["deployment"]["id"],
        dataflow_type="manual",
    )

    DataflowOrgTask.objects.create(
        dataflow=new_dataflow,
        orgtask=org_task,
    )

    return new_dataflow


def delete_orgtask(org_task: OrgTask):
    """Delete an orgtask; along with its deployment if its there"""

    for dataflow_orgtask in DataflowOrgTask.objects.filter(
        orgtask=org_task
    ).all():  # only long running task like TASK_DBTRUN, TASK_AIRBYTESYNC will have dataflow
        # delete the manual deployment for this
        dataflow = dataflow_orgtask.dataflow
        if dataflow:
            logger.info(f"deleting manual deployment for {org_task.task.slug}")

            # do this in try catch because it can fail & throw error
            try:
                prefect_service.delete_deployment_by_id(dataflow.deployment_id)
            except Exception:
                pass
            logger.info("FINISHED deleting manual deployment for dbt run")
            logger.info("deleting OrgDataFlowv1")
            dataflow.delete()

        logger.info("deleting DataflowOrgTask")
        dataflow_orgtask.delete()

    logger.info("deleting org task %s", org_task.task.slug)
    org_task.delete()

    return None, None


def fetch_orgtask_lock(org_task: OrgTask):
    """fetch the lock status of an orgtask"""
    lock = TaskLock.objects.filter(orgtask=org_task).first()
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
            "status": lock_status,
            "task_slug": org_task.task.slug,
        }

    return None


def fetch_orgtask_lock_v1(org_task: OrgTask, lock: Union[TaskLock, None]):
    """fetch the lock status of an orgtask"""
    if lock:
        lock_status = TaskLockStatus.QUEUED
        if lock.flow_run_id:
            flow_run = PrefectFlowRun.objects.filter(flow_run_id=lock.flow_run_id).first()
            if flow_run:
                if flow_run.status in [
                    FLOW_RUN_SCHEDULED_STATE_TYPE,
                    FLOW_RUN_PENDING_STATE_TYPE,
                ]:
                    lock_status = TaskLockStatus.QUEUED
                elif flow_run.status == FLOW_RUN_RUNNING_STATE_TYPE:
                    lock_status = TaskLockStatus.RUNNING
                else:
                    lock_status = TaskLockStatus.COMPLETED

        return {
            "lockedBy": lock.locked_by.user.email,
            "lockedAt": lock.locked_at,
            "flowRunId": lock.flow_run_id,
            "status": lock_status,
            "task_slug": org_task.task.slug,
        }

    return None
