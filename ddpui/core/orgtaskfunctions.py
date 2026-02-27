"""
functions to work with transform related tasks or orgtasks in general
do not raise http errors here
"""

import uuid
from typing import Union
from pathlib import Path
import yaml

from ddpui.utils.file_storage.storage_factory import StorageFactory
from ddpui.models.tasks import OrgTask, Task, DataflowOrgTask, TaskLock, TaskLockStatus, TaskType
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
    FLOW_RUN_PENDING_STATE_TYPE,
    FLOW_RUN_RUNNING_STATE_TYPE,
    FLOW_RUN_SCHEDULED_STATE_TYPE,
    FLOW_RUN_TERMINAL_STATE_TYPES,
)
from ddpui.ddpdbt.schema import DbtCloudJobParams, DbtProjectParams
from ddpui.ddpprefect.schema import (
    PrefectDataFlowUpdateSchema3,
)
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
    task_types = (
        [TaskType.DBT, TaskType.GIT]
        if org.dbt.transform_type == TransformType.GIT
        else [TaskType.DBT]
    )
    for task in Task.objects.filter(type__in=task_types, is_system=True).all():
        org_task = OrgTask.objects.create(org=org, task=task, uuid=uuid.uuid4(), dbt=org.dbt)

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
        storage = StorageFactory.get_storage_adapter()
        content = storage.read_file(elementary_profiles_yml)
        config = yaml.safe_load(content)
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
            dbt=org.dbt,
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
    if org_task.task.type == TaskType.DBT:
        tasks = [
            setup_dbt_core_task_config(
                org_task, credentials_profile_block, dbt_project_params
            ).to_json()
        ]
    elif org_task.task.type == TaskType.DBTCLOUD:
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
        org_task.org.get_queue_config().transform_task_queue,  # manual dbt tasks queue
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
    """Delete an orgtask; along with any deployments it may be attached to"""

    # we first go through manual (system-generated) dataflows since the logic is straightforward
    for dataflow_orgtask in DataflowOrgTask.objects.filter(
        orgtask=org_task, dataflow__dataflow_type="manual"
    ):
        # delete the manual deployment for this
        dataflow = dataflow_orgtask.dataflow
        if dataflow:
            logger.info(f"deleting manual deployment for {org_task.task.slug}")

            # do this in try catch because it can fail & throw error
            try:
                prefect_service.delete_deployment_by_id(dataflow.deployment_id)
            except Exception as err:
                # we want to return an error if the deployment exists in prefect
                # but failed to be deleted
                # we want to ignore it if the deployment doesn't exist
                # hmmm
                logger.error(f"Failed to delete deployment {dataflow.deployment_id}: {err}")
            logger.info("FINISHED deleting manual deployment for orgtask")
            logger.info("deleting OrgDataFlowv1")
            dataflow.delete()

        logger.info("deleting DataflowOrgTask")
        dataflow_orgtask.delete()

    # now we do the orchestrated (user-generated, with or without a schedule) pipelines
    # here the deployment may contain a series of tasks and we only want to remove the tasks
    # which correspond to this OrgTask
    for dataflow_orgtask in DataflowOrgTask.objects.filter(
        orgtask=org_task, dataflow__dataflow_type="orchestrate"
    ):
        dataflow = dataflow_orgtask.dataflow
        if dataflow:
            # fetch config from prefect
            deployment = prefect_service.get_deployment(dataflow.deployment_id)
            # { name, deploymentId, tags, cron, isScheduleActive, parameters }
            # parameters = {config: {org_slug, tasks}}
            # tasks = list of
            #    {seq, slug, type, timeout, orgtask_uuid, connection_id, airbyte_server_block}
            parameters = deployment["parameters"]
            # logger.info(parameters)
            tasks_to_keep = []
            for task in parameters["config"]["tasks"]:
                if task.get("orgtask_uuid") == str(org_task.uuid):
                    logger.info(f"deleting task {task['slug']} from deployment")
                else:
                    tasks_to_keep.append(task)
            parameters["config"]["tasks"] = tasks_to_keep
            # logger.info(parameters)
            if len(parameters["config"]["tasks"]) > 0:
                payload = PrefectDataFlowUpdateSchema3(
                    deployment_params=parameters, cron=dataflow.cron
                )
                prefect_service.update_dataflow_v1(dataflow.deployment_id, payload)
                logger.info("updated deployment %s", dataflow.deployment_name)
            else:
                prefect_service.delete_deployment_by_id(dataflow.deployment_id)
                dataflow.delete()

        # the dataflow i.e. prefect deploymenet may or may not have been deleted. in either case
        # this orgtask is no longer attached to it so delete the mapping relation
        logger.info("deleting DataflowOrgTask")
        dataflow_orgtask.delete()

    logger.info("deleting org task %s", org_task.task.slug)
    org_task.delete()

    return None, None


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
                    if flow_run.status in FLOW_RUN_TERMINAL_STATE_TYPES:
                        TaskLock.objects.filter(orgtask=org_task).delete()
                        return None

        return {
            "lockedBy": lock.locked_by.user.email,
            "lockedAt": lock.locked_at,
            "flowRunId": lock.flow_run_id,
            "status": lock_status,
            "task_slug": org_task.task.slug,
        }

    return None
