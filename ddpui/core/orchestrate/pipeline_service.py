"""Orchestrate service for pipeline business logic

This module encapsulates all pipeline/dataflow-related business logic,
separating it from the API layer for better testability and maintainability.
"""

from typing import Dict, List, Optional, Any

from django.utils import timezone as djantotimezone

from ddpui.models.org import Org, OrgDataFlowv1, OrgPrefectBlockv1
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import OrgTask, DataflowOrgTask, TaskType, TaskLock, TaskLockStatus, Task
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.ddpprefect import prefect_service, AIRBYTESERVER
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
    PrefectDataFlowCreateSchema4,
    PrefectDataFlowUpdateSchema3,
)
from ddpui.schemas.org_task_schema import TaskParameters, ClearSelectedStreams
from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.utils.constants import (
    TASK_AIRBYTESYNC,
    TASK_DBTRUN,
    TASK_GITPULL,
    TASK_AIRBYTECLEAR,
    TASK_GITCLONE,
)
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.helpers import generate_hash_id
from ddpui.core.pipelinefunctions import (
    pipeline_with_orgtasks,
    lock_tasks_for_dataflow,
    setup_dbt_core_task_config,
    setup_airbyte_clear_streams_task_config,
    fetch_pipeline_lock_v1,
)
from ddpui.core.orgdbt_manager import DbtProjectManager

logger = CustomLogger("ddpui.orchestrate")


class PipelineNotFoundError(Exception):
    """Raised when pipeline is not found (404)"""

    def __init__(self, deployment_id: str):
        self.message = f"Pipeline with deployment_id {deployment_id} not found"
        self.deployment_id = deployment_id
        super().__init__(self.message)


class PipelineValidationError(Exception):
    """Raised when pipeline validation fails (400)"""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class PipelineConfigurationError(Exception):
    """Raised when pipeline configuration is invalid (422)"""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class PipelineServiceError(Exception):
    """Raised when external service fails (500)"""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class PipelineService:
    """Service class for pipeline orchestration business logic"""

    @staticmethod
    def _build_transform_tasks(
        org: Org, transform_tasks_payload: List, existing_task_configs: List
    ) -> tuple[List, List[OrgTask]]:
        """
        Build transform task configs and collect org tasks for mapping.
        Resolves transform tasks from payload, auto-adds the appropriate git step,
        and returns deployment task configs + ordered org tasks.

        Returns:
            tuple of (task_configs, map_org_tasks)
        """
        if not transform_tasks_payload:
            return [], []

        logger.info("Transform tasks being pushed to the pipeline")

        orgdbt = org.dbt
        dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, orgdbt)

        dbt_orgtasks = []
        git_orgtasks = []
        dbt_cloud_orgtasks = []

        transform_tasks_payload.sort(key=lambda task: task.seq)

        for transform_task in transform_tasks_payload:
            org_task = OrgTask.objects.filter(uuid=transform_task.uuid, org=org).first()
            if org_task is None:
                raise PipelineValidationError(
                    f"transform task with uuid {transform_task.uuid} not found"
                )

            if org_task.task.type == TaskType.DBT:
                dbt_orgtasks.append(org_task)
            elif org_task.task.type == TaskType.GIT:
                # Skip git tasks - they should not come from frontend anymore
                logger.warning(
                    f"Ignoring git task {org_task.task.slug} from frontend - git tasks are auto-managed"
                )
                continue
            elif org_task.task.type == TaskType.DBTCLOUD:
                dbt_cloud_orgtasks.append(org_task)

        logger.info(f"{len(dbt_orgtasks)} DBT cli tasks being pushed to the pipeline")
        logger.info(f"{len(dbt_cloud_orgtasks)} Dbt cloud tasks being pushed to the pipeline")

        # Add git step automatically based on workpool type
        if len(dbt_orgtasks) > 0:
            if PipelineService._is_workpool_eks(org):
                logger.info("EKS workpool detected, adding git clone step before DBT tasks")
                git_clone_orgtask = PipelineService._get_or_create_git_clone_orgtask(org)
                git_orgtasks.insert(0, git_clone_orgtask)
            else:
                logger.info("Non-EKS workpool detected, adding git pull step before DBT tasks")
                git_pull_orgtask = PipelineService._get_or_create_git_pull_orgtask(org)
                git_orgtasks.insert(0, git_pull_orgtask)

        # dbt cli profile block - only needed if we have DBT tasks
        cli_block = None
        if len(dbt_orgtasks) > 0:
            cli_block = orgdbt.cli_profile_block if orgdbt else None
            if not cli_block:
                raise PipelineConfigurationError("dbt cli profile not found")

        # dbt cloud creds block
        dbt_cloud_creds_block = None
        if len(dbt_cloud_orgtasks) > 0:
            dbt_cloud_creds_block = orgdbt.dbtcloud_creds_block if orgdbt else None
            if not dbt_cloud_creds_block:
                raise PipelineConfigurationError("dbt cloud creds block not found")

        # get the deployment task configs
        task_configs, error = pipeline_with_orgtasks(
            org,
            git_orgtasks + dbt_orgtasks + dbt_cloud_orgtasks,
            cli_block=cli_block,
            dbt_project_params=dbt_project_params,
            start_seq=len(existing_task_configs),
            dbt_cloud_creds_block=dbt_cloud_creds_block,
        )
        if error:
            raise PipelineConfigurationError(error)

        map_org_tasks = git_orgtasks + dbt_orgtasks + dbt_cloud_orgtasks
        return task_configs, map_org_tasks

    @staticmethod
    def create_pipeline(org: Org, payload: PrefectDataFlowCreateSchema4) -> Dict[str, Any]:
        """Create a new pipeline/dataflow"""

        if payload.name in [None, ""]:
            raise PipelineValidationError("must provide a name for the flow")

        tasks = []
        map_org_tasks = []

        # Handle connection sync tasks
        sync_orgtasks, airbyte_server_block = PipelineService._build_sync_tasks(
            org, payload.connections
        )
        if sync_orgtasks:
            task_configs, error = pipeline_with_orgtasks(
                org, sync_orgtasks, server_block=airbyte_server_block
            )
            if error:
                raise PipelineConfigurationError(error)
            tasks += task_configs
            map_org_tasks += sync_orgtasks

        logger.info(f"Pipeline has {len(sync_orgtasks)} airbyte syncs")

        # Handle transform tasks
        transform_task_configs, transform_org_tasks = PipelineService._build_transform_tasks(
            org, payload.transformTasks, tasks
        )
        tasks += transform_task_configs
        map_org_tasks += transform_org_tasks

        # create deployment
        try:
            hash_code = generate_hash_id(8)
            deployment_name = f"pipeline-{org.slug}-{hash_code}"
            dataflow = prefect_service.create_dataflow_v1(
                PrefectDataFlowCreateSchema3(
                    deployment_name=deployment_name,
                    flow_name=deployment_name,
                    orgslug=org.slug,
                    deployment_params={"config": {"tasks": tasks, "org_slug": org.slug}},
                    cron=payload.cron,
                ),
                org.get_queue_config().scheduled_pipeline_queue,
            )
        except Exception as error:
            logger.exception(error)
            raise PipelineServiceError("failed to create a pipeline") from error

        org_dataflow = OrgDataFlowv1.objects.create(
            org=org,
            name=payload.name,
            deployment_name=dataflow["deployment"]["name"],
            deployment_id=dataflow["deployment"]["id"],
            cron=payload.cron,
            dataflow_type="orchestrate",
        )

        for idx, org_task in enumerate(map_org_tasks):
            DataflowOrgTask.objects.create(dataflow=org_dataflow, orgtask=org_task, seq=idx)

        return {
            "deploymentId": org_dataflow.deployment_id,
            "name": org_dataflow.name,
            "deploymentName": org_dataflow.deployment_name,
            "cron": org_dataflow.cron,
        }

    @staticmethod
    def update_pipeline(
        org: Org, deployment_id: str, payload: PrefectDataFlowUpdateSchema3
    ) -> Dict[str, Any]:
        """Update an existing pipeline"""

        org_data_flow = OrgDataFlowv1.objects.filter(org=org, deployment_id=deployment_id).first()
        if not org_data_flow:
            raise PipelineNotFoundError(deployment_id)

        tasks = []
        map_org_tasks = []

        # Handle connection sync tasks
        sync_orgtasks, airbyte_server_block = PipelineService._build_sync_tasks(
            org, payload.connections
        )
        if sync_orgtasks:
            task_configs, error = pipeline_with_orgtasks(
                org, sync_orgtasks, server_block=airbyte_server_block
            )
            if error:
                raise PipelineConfigurationError(error)
            tasks += task_configs
            map_org_tasks += sync_orgtasks

        logger.info(f"Updating pipeline to have {len(sync_orgtasks)} airbyte syncs")

        # Handle transform tasks
        transform_task_configs, transform_org_tasks = PipelineService._build_transform_tasks(
            org, payload.transformTasks, tasks
        )
        tasks += transform_task_configs
        map_org_tasks += transform_org_tasks

        # update deployment
        payload.deployment_params = {"config": {"tasks": tasks, "org_slug": org.slug}}
        try:
            prefect_service.update_dataflow_v1(deployment_id, payload)
        except Exception as error:
            logger.exception(error)
            raise PipelineServiceError("failed to update a pipeline") from error

        # Delete mapping and re-map orgtasks to dataflow
        DataflowOrgTask.objects.filter(dataflow=org_data_flow).delete()
        for idx, org_task in enumerate(map_org_tasks):
            DataflowOrgTask.objects.create(dataflow=org_data_flow, orgtask=org_task, seq=idx)

        org_data_flow.cron = payload.cron if payload.cron else None
        org_data_flow.name = payload.name
        org_data_flow.save()

        return {"success": 1}

    @staticmethod
    def _build_sync_tasks(org: Org, connections: List) -> tuple[List[OrgTask], OrgPrefectBlockv1]:
        """Build Airbyte sync task configurations"""

        sync_orgtasks = []
        airbyte_server_block = None

        if len(connections) > 0:
            airbyte_server_block = OrgPrefectBlockv1.objects.filter(
                org=org, block_type=AIRBYTESERVER
            ).first()
            if not airbyte_server_block:
                raise PipelineConfigurationError("airbyte server block not found")

            logger.info("Connections being pushed to the pipeline")

            # only connections with org task will be pushed to pipeline
            connections.sort(key=lambda conn: conn.seq)
            for connection in connections:
                logger.info(connection)
                org_task = OrgTask.objects.filter(
                    org=org,
                    connection_id=connection.id,
                    task__slug=TASK_AIRBYTESYNC,
                ).first()
                if org_task is None:
                    logger.info(
                        f"connection id {connection.id} not found in org tasks; ignoring this airbyte sync"
                    )
                    continue
                # map this org task to dataflow
                sync_orgtasks.append(org_task)

        return sync_orgtasks, airbyte_server_block

    @staticmethod
    def get_pipelines(org: Org) -> List[Dict[str, Any]]:
        """Get all pipelines for an organization"""

        all_org_data_flows = OrgDataFlowv1.objects.filter(org=org, dataflow_type="orchestrate")

        dataflow_ids = all_org_data_flows.values_list("id", flat=True)
        all_dataflow_orgtasks = DataflowOrgTask.objects.filter(
            dataflow_id__in=dataflow_ids
        ).select_related("orgtask")

        all_org_task_ids = all_dataflow_orgtasks.values_list("orgtask_id", flat=True)
        all_org_task_locks = TaskLock.objects.filter(orgtask_id__in=all_org_task_ids)

        deployment_ids = [flow.deployment_id for flow in all_org_data_flows]
        all_last_runs = prefect_service.get_flow_runs_by_deployment_id_v1(
            deployment_ids=deployment_ids, limit=1, offset=0
        )

        # dictionary to hold {"id": status}
        is_deployment_active = {}

        # setting active/inactive status based on if the schedule is set or not
        for deployment in prefect_service.get_filtered_deployments(org.slug, deployment_ids):
            is_deployment_active[deployment["deploymentId"]] = (
                deployment["isScheduleActive"] if "isScheduleActive" in deployment else False
            )

        res = []

        for flow in all_org_data_flows:
            dataflow_orgtasks = [
                dfot for dfot in all_dataflow_orgtasks if dfot.dataflow_id == flow.id
            ]

            org_tasks: list[OrgTask] = [
                dataflow_orgtask.orgtask for dataflow_orgtask in dataflow_orgtasks
            ]
            orgtask_ids = [org_task.id for org_task in org_tasks]

            lock = None
            all_locks = [lock for lock in all_org_task_locks if lock.orgtask_id in orgtask_ids]
            if len(all_locks) > 0:
                lock = all_locks[0]

            runs = [run for run in all_last_runs if run["deployment_id"] == flow.deployment_id]

            if lock:
                lock = fetch_pipeline_lock_v1(flow, lock)

            res.append(
                {
                    "name": flow.name,
                    "deploymentId": flow.deployment_id,
                    "cron": flow.cron,
                    "deploymentName": flow.deployment_name,
                    "lastRun": runs[0] if runs and len(runs) > 0 else None,
                    "status": (
                        is_deployment_active[flow.deployment_id]
                        if flow.deployment_id in is_deployment_active
                        else False
                    ),
                    "lock": lock,
                    "queuedFlowRunWaitTime": (
                        prefect_service.estimate_time_for_next_queued_run_of_dataflow(flow)
                        if lock and (lock["status"] == TaskLockStatus.QUEUED)
                        else None
                    ),
                }
            )

        return res

    @staticmethod
    def get_pipeline_details(org: Org, deployment_id: str) -> Dict[str, Any]:
        """Get details of a specific pipeline"""

        # Find the org data flow
        org_data_flow = OrgDataFlowv1.objects.filter(org=org, deployment_id=deployment_id).first()

        if org_data_flow is None:
            raise PipelineNotFoundError(deployment_id)

        try:
            deployment = prefect_service.get_deployment(deployment_id)
            logger.info(deployment)
        except Exception as error:
            logger.exception(error)
            raise PipelineServiceError("failed to get deployment from prefect-proxy") from error

        connections = [
            {
                "id": dataflow_orgtask.orgtask.connection_id,
                "seq": dataflow_orgtask.seq,
                "name": airbyte_service.get_connection(
                    org.airbyte_workspace_id, dataflow_orgtask.orgtask.connection_id
                )[
                    "name"
                ],  # TODO: this call can be removed once the logic at frontend is updated
            }
            for dataflow_orgtask in DataflowOrgTask.objects.filter(
                dataflow=org_data_flow, orgtask__task__slug=TASK_AIRBYTESYNC
            )
            .all()
            .order_by("seq")
        ]

        transform_tasks = [
            {"uuid": dataflow_orgtask.orgtask.uuid, "seq": dataflow_orgtask.seq}
            for dataflow_orgtask in DataflowOrgTask.objects.filter(
                dataflow=org_data_flow,
                orgtask__task__type__in=[TaskType.DBT, TaskType.DBTCLOUD],
            )
            .all()
            .order_by("seq")
        ]

        has_transform = len(transform_tasks) > 0

        # differentiate between deploymentName and name
        deployment["deploymentName"] = deployment["name"]
        deployment["name"] = org_data_flow.name

        return {
            "name": org_data_flow.name,
            "deploymentName": deployment["deploymentName"],
            "cron": deployment["cron"],
            "connections": connections,
            "dbtTransform": "yes" if has_transform else "no",
            "transformTasks": transform_tasks,
            "isScheduleActive": deployment["isScheduleActive"],
        }

    @staticmethod
    def delete_pipeline(org: Org, deployment_id: str) -> Dict[str, Any]:
        """Delete a pipeline"""

        # Find the org data flow
        org_data_flow = OrgDataFlowv1.objects.filter(org=org, deployment_id=deployment_id).first()

        if not org_data_flow:
            raise PipelineNotFoundError(deployment_id)

        try:
            prefect_service.delete_deployment_by_id(deployment_id)
        except Exception as error:
            logger.exception(error)
            raise PipelineServiceError("failed to delete deployment from prefect") from error

        org_data_flow.delete()

        return {"success": 1}

    @staticmethod
    def set_pipeline_schedule(org: Org, deployment_id: str, status: str) -> Dict[str, Any]:
        """Set pipeline schedule to active/inactive"""

        if (
            (status is None)
            or (isinstance(status, str) is not True)
            or (status not in ["active", "inactive"])
        ):
            raise PipelineValidationError("incorrect status value")

        org_data_flow = OrgDataFlowv1.objects.filter(org=org, deployment_id=deployment_id).first()
        if org_data_flow is None:
            raise PipelineNotFoundError(deployment_id)

        try:
            prefect_service.set_deployment_schedule(deployment_id, status)
        except Exception as error:
            logger.exception(error)
            raise PipelineServiceError("failed to change flow state") from error

        return {"success": 1}

    @staticmethod
    def run_pipeline(
        org: Org, orguser: OrgUser, deployment_id: str, payload: Optional[TaskParameters] = None
    ) -> Dict[str, Any]:
        """Run a pipeline deployment"""

        dataflow = OrgDataFlowv1.objects.filter(org=org, deployment_id=deployment_id).first()
        if not dataflow:
            raise PipelineNotFoundError(deployment_id)

        dataflow_orgtasks = (
            DataflowOrgTask.objects.filter(dataflow=dataflow)
            .order_by("seq")
            .select_related("orgtask")
        )

        if dataflow_orgtasks.count() == 0:
            raise PipelineConfigurationError("no org task mapped to the deployment")

        orgdbt = org.dbt

        # ordered
        org_tasks: list[OrgTask] = [
            dataflow_orgtask.orgtask for dataflow_orgtask in dataflow_orgtasks
        ]

        locks = lock_tasks_for_dataflow(orguser=orguser, dataflow=dataflow, org_tasks=org_tasks)

        try:
            # allow parameter passing only for manual dbt runs and if there are parameters being passed
            flow_run_params = None
            if (
                len(org_tasks) == 1
                and dataflow.dataflow_type == "manual"
                and org_tasks[0].task.slug == TASK_DBTRUN
                and payload
                and (payload.flags or payload.options)
            ):
                logger.info("sending custom flow run params to the deployment run")
                orgtask = org_tasks[0]

                # save orgtask params to memory and not db
                orgtask.parameters = dict(payload)

                # fetch cli block
                cli_profile_block = orgdbt.cli_profile_block if orgdbt else None
                dbt_project_params: DbtProjectParams = DbtProjectManager.gather_dbt_project_params(
                    org, orgdbt
                )

                # dont set any parameters if cli block is not present or there is an error
                if cli_profile_block:
                    logger.info("found cli profile block")
                    flow_run_params = {
                        "config": {
                            "tasks": [
                                setup_dbt_core_task_config(
                                    orgtask,
                                    cli_profile_block,
                                    dbt_project_params,
                                ).to_json()
                            ],
                            "org_slug": org.slug,
                        }
                    }

            res = prefect_service.create_deployment_flow_run(deployment_id, flow_run_params)
            PrefectFlowRun.objects.create(
                deployment_id=deployment_id,
                flow_run_id=res["flow_run_id"],
                name=res.get("name", "name"),
                start_time=None,
                expected_start_time=djantotimezone.now(),
                total_run_time=-1,
                status="Scheduled",
                state_name="Scheduled",
                retries=0,
                orguser=orguser,
            )
        except Exception as error:
            for task_lock in locks:
                logger.info("deleting TaskLock %s", task_lock.orgtask.task.slug)
                task_lock.delete()
            logger.exception(error)
            raise PipelineServiceError("failed to start a run") from error

        for tasklock in locks:
            tasklock.flow_run_id = res["flow_run_id"]
            tasklock.save()

        return res

    @staticmethod
    def post_clear_selected_streams_run(
        org: Org, orguser: OrgUser, deployment_id: str, payload: ClearSelectedStreams
    ) -> Dict[str, Any]:
        """Clear selected streams in Airbyte for a given deployment."""

        # Validate connection_id and streams
        if not payload.connectionId:
            raise PipelineValidationError("connection_id is required")
        if not payload.streams:
            raise PipelineValidationError("streams list cannot be empty")

        dataflow = OrgDataFlowv1.objects.filter(org=org, deployment_id=deployment_id).first()
        if not dataflow:
            raise PipelineNotFoundError(deployment_id)

        dataflow_orgtasks = (
            DataflowOrgTask.objects.filter(dataflow=dataflow)
            .order_by("seq")
            .select_related("orgtask")
        )

        if dataflow_orgtasks.count() == 0:
            raise PipelineConfigurationError("no org task mapped to the deployment")

        org_tasks = [dataflow_orgtask.orgtask for dataflow_orgtask in dataflow_orgtasks]

        locks = lock_tasks_for_dataflow(orguser=orguser, dataflow=dataflow, org_tasks=org_tasks)

        try:
            flow_run_params = None

            if dataflow.dataflow_type != "manual":
                raise PipelineValidationError("This endpoint is only for manual dataflows")

            if len(org_tasks) != 1:
                raise PipelineValidationError(
                    "This endpoint is only for dataflows with a single task"
                )

            orgtask = org_tasks[0]

            if orgtask.task.slug != TASK_AIRBYTECLEAR:
                raise PipelineValidationError("This endpoint is only for Airbyte Clear tasks")

            if orgtask.connection_id != payload.connectionId:
                raise PipelineValidationError(
                    "Connection ID does not match the task's connection ID"
                )

            if not payload or not payload.streams:
                raise PipelineValidationError("Streams must be provided to clear")

            logger.info("sending custom flow run params to the deployment run")

            server_block = OrgPrefectBlockv1.objects.filter(
                org=org, block_type=AIRBYTESERVER
            ).first()

            if server_block:
                logger.info("found airbyte server block")
                flow_run_params = {
                    "config": {
                        "tasks": [
                            setup_airbyte_clear_streams_task_config(
                                orgtask,
                                server_block,
                                payload.streams,
                            ).to_json()
                        ],
                        "org_slug": org.slug,
                    }
                }

            res = prefect_service.create_deployment_flow_run(deployment_id, flow_run_params)
            PrefectFlowRun.objects.create(
                deployment_id=deployment_id,
                flow_run_id=res["flow_run_id"],
                name=res.get("name", "selected-stream-clear"),
                start_time=None,
                expected_start_time=djantotimezone.now(),
                total_run_time=-1,
                status="Scheduled",
                state_name="Scheduled",
                retries=0,
                orguser=orguser,
            )
        except Exception as error:
            for task_lock in locks:
                logger.info("deleting TaskLock %s", task_lock.orgtask.task.slug)
                task_lock.delete()
            logger.exception(error)
            raise PipelineServiceError(f"Failed to start a run {str(error)}")

        for tasklock in locks:
            tasklock.flow_run_id = res["flow_run_id"]
            tasklock.save()

        return res

    @staticmethod
    def _is_workpool_eks(org: Org) -> bool:
        """Check if the scheduled pipeline queue workpool is EKS"""
        queue_config = org.get_queue_config()
        scheduled_queue = queue_config.scheduled_pipeline_queue
        return getattr(scheduled_queue, "is_workpool_eks", False)

    @staticmethod
    def _get_or_create_git_clone_orgtask(org: Org) -> OrgTask:
        """Get or create git clone OrgTask for the organization"""
        git_clone_task = Task.objects.filter(slug=TASK_GITCLONE).first()
        if not git_clone_task:
            raise PipelineConfigurationError("git-clone task not found in database")

        orgdbt = org.dbt
        if not orgdbt:
            raise PipelineConfigurationError("dbt configuration not found for organization")

        git_clone_orgtask, created = OrgTask.objects.get_or_create(
            org=org, task=git_clone_task, dbt=orgdbt, defaults={"parameters": {}}
        )

        if created:
            logger.info(f"Created git clone OrgTask for org {org.slug}")

        return git_clone_orgtask

    @staticmethod
    def _get_or_create_git_pull_orgtask(org: Org) -> OrgTask:
        """Get or create git pull OrgTask for the organization"""
        git_pull_task = Task.objects.filter(slug=TASK_GITPULL).first()
        if not git_pull_task:
            raise PipelineConfigurationError("git-pull task not found in database")

        orgdbt = org.dbt
        if not orgdbt:
            raise PipelineConfigurationError("dbt configuration not found for organization")

        git_pull_orgtask, created = OrgTask.objects.get_or_create(
            org=org, task=git_pull_task, dbt=orgdbt, defaults={"parameters": {}}
        )

        if created:
            logger.info(f"Created git pull OrgTask for org {org.slug}")

        return git_pull_orgtask
