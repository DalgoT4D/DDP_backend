"""
functions to work with transform related tasks or orgtasks in general
do not raise http errors here
"""

import uuid
from typing import Union
from pathlib import Path
import yaml
from ddpui.models.tasks import OrgTask, Task, DataflowOrgTask, TaskLock, TaskLockStatus, TaskType
from ddpui.models.org import (
    Org,
    OrgDataFlowv1,
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
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.ddpprefect.schema import (
    PrefectDataFlowUpdateSchema3,
)
from ddpui.ddpprefect import prefect_service
from ddpui.core.pipelinefunctions import (
    pipeline_with_orgtasks,
)
from ddpui.core.orchestrate.pipeline_service import PipelineService
from ddpui.utils.constants import TASK_GENERATE_EDR, LONG_RUNNING_TASKS
from ddpui.utils.helpers import generate_hash_id
from ddpui.models.flow_runs import PrefectFlowRun

logger = CustomLogger("ddpui")


def get_transform_task_queue(org: Org):
    """Return the queue config entry used by manual (transform) dbt deployments.

    Raises:
        ValueError: if the queue config is missing, the transform_task_queue
        key is not present, or the queue itself is unavailable/misconfigured.
    """
    queue_config = org.get_queue_config()
    if queue_config is None:
        raise ValueError(f"queue config not found for org {org.slug}")

    if not hasattr(queue_config, "transform_task_queue"):
        raise ValueError(f"transform_task_queue key missing in queue config for org {org.slug}")

    transform_queue = queue_config.transform_task_queue
    if transform_queue is None or not getattr(transform_queue, "name", None):
        raise ValueError(f"transform_task_queue is not configured for org {org.slug}")

    return transform_queue


def create_default_transform_tasks(org: Org, dbt_project_params: DbtProjectParams):
    """Create all the transform (git, dbt) tasks"""
    if org.dbt is None:
        raise ValueError("dbt is not configured for this org")

    for task in Task.objects.filter(type__in=[TaskType.DBT, TaskType.GIT], is_system=True).all():
        # Use get_or_create: LONG_RUNNING_TASKS run create_prefect_deployment_for_dbtcore_task,
        # which calls get_or_create_git_clone/pull_orgtask. Since git-clone (PK 13) has a higher
        # PK than the dbt tasks, it gets pre-created there before the loop reaches it — causing
        # a duplicate if we use plain create() here.
        org_task, _ = OrgTask.objects.get_or_create(org=org, task=task, dbt=org.dbt)

        if task.slug in LONG_RUNNING_TASKS:
            # create deployment (auto-prepends git + dbt-clean + dbt-deps for dbt tasks)
            create_prefect_deployment_for_dbtcore_task(org_task, dbt_project_params)

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
            dbt=org.dbt,
        )
    return org_task


def create_prefect_deployment_for_dbtcore_task(
    org_task: OrgTask,
    dbt_project_params: DbtProjectParams,
):
    """
    - create a prefect deployment for a single long-running dbt command
    - save the deployment id to an OrgDataFlowv1 object
    - credentials come from the org's dbt-profile Secret block (read by the runner
      via env["dbt-profile-secret-block"] at flow-run time)
    - only long-running tasks (dbt-run, dbt-test, dbt-seed) get a deployment;
      auto-managed prep tasks (git-pull, dbt-clean, dbt-deps) are chained inside
      long-running deployments and do not get one of their own
    """
    if org_task.task.slug not in LONG_RUNNING_TASKS:
        raise ValueError(
            f"cannot create deployment for {org_task.task.slug} — "
            f"only long-running tasks ({', '.join(LONG_RUNNING_TASKS)}) get deployments"
        )

    hash_code = generate_hash_id(8)
    deployment_name = f"manual-{org_task.org.slug}-{org_task.task.slug}-{hash_code}"

    transform_queue = get_transform_task_queue(org_task.org)

    tasks = []
    # orgtasks that will be mapped to the deployment via DataflowOrgTask; kept in
    # execution order. Full chain: git + dbt-clean + dbt-deps + primary dbt task.
    mapped_orgtasks = [org_task]

    if org_task.task.type == TaskType.DBT:
        # Auto-prepend git-pull-or-clone + dbt-clean + dbt-deps ahead of the primary
        # dbt task, mirroring the chain applied to orchestrated pipelines.
        org = org_task.org
        if getattr(transform_queue, "is_workpool_eks", False):
            git_orgtask = PipelineService.get_or_create_git_clone_orgtask(org)
        else:
            git_orgtask = PipelineService.get_or_create_git_pull_orgtask(org)

        chain_orgtasks = [
            git_orgtask,
            PipelineService.get_or_create_dbt_clean_orgtask(org),
            PipelineService.get_or_create_dbt_deps_orgtask(org),
            org_task,
        ]

        task_configs, err = pipeline_with_orgtasks(
            org,
            chain_orgtasks,
            dbt_project_params=dbt_project_params,
            gitrepo_url=org.dbt.gitrepo_url if org.dbt else None,
        )
        if err:
            raise ValueError(err)
        tasks = task_configs
        mapped_orgtasks = chain_orgtasks

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
        transform_queue,
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

    for idx, chained_orgtask in enumerate(mapped_orgtasks):
        DataflowOrgTask.objects.create(
            dataflow=new_dataflow,
            orgtask=chained_orgtask,
            seq=idx,
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
