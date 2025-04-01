import os

from ninja import Router
from ninja.errors import HttpError

from ddpui import auth
from ddpui.ddpprefect import prefect_service
from ddpui.ddpairbyte import airbyte_service

from ddpui.ddpprefect import DBTCLIPROFILE, AIRBYTESERVER, DBTCLOUDCREDS
from ddpui.models.org import OrgDataFlowv1, OrgPrefectBlockv1
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import DataflowOrgTask, OrgTask
from ddpui.models.llm import LogsSummarizationType
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
    PrefectFlowRunSchema,
    PrefectDataFlowUpdateSchema3,
    PrefectDataFlowCreateSchema4,
    TaskStateSchema,
)
from ddpui.utils.constants import TASK_DBTRUN, TASK_AIRBYTESYNC
from ddpui.utils.custom_logger import CustomLogger
from ddpui.schemas.org_task_schema import TaskParameters
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.utils.prefectlogs import parse_prefect_logs
from ddpui.utils.helpers import generate_hash_id
from ddpui.core.pipelinefunctions import (
    setup_dbt_core_task_config,
    pipeline_with_orgtasks,
    fetch_pipeline_lock_v1,
    lock_tasks_for_dataflow,
)
from ddpui.celeryworkers.tasks import summarize_logs
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.auth import has_permission
from ddpui.models.tasks import TaskLock

pipeline_router = Router()
logger = CustomLogger("ddpui")


@pipeline_router.post("v1/flows/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_create_pipeline"])
def post_prefect_dataflow_v1(request, payload: PrefectDataFlowCreateSchema4):
    """Create a prefect deployment i.e. a ddp dataflow"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    if payload.name in [None, ""]:
        raise HttpError(400, "must provide a name for the flow")

    tasks = []  # This is main task array- containing airbyte and dbt task both.
    map_org_tasks = []  # seq of org tasks to be mapped in pipelin/ dataflow

    # push conection orgtasks in pipelin
    sync_orgtasks = []
    if len(payload.connections) > 0:
        org_server_block = OrgPrefectBlockv1.objects.filter(
            org=orguser.org, block_type=AIRBYTESERVER
        ).first()
        if not org_server_block:
            raise HttpError(400, "airbyte server block not found")

        logger.info("Connections being pushed to the pipeline")

        # only connections with org task will be pushed to pipeline
        payload.connections.sort(key=lambda conn: conn.seq)
        for connection in payload.connections:
            logger.info(connection)
            org_task = OrgTask.objects.filter(
                org=orguser.org,
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

        # get the deployment task configs
        task_configs, error = pipeline_with_orgtasks(
            orguser.org,
            sync_orgtasks,
            server_block=org_server_block,
        )
        if error:
            raise HttpError(400, error)
        tasks += task_configs

    map_org_tasks += sync_orgtasks
    logger.info(f"Pipline has {len(sync_orgtasks)} airbyte syncs")

    # push dbt pipeline orgtasks
    dbt_project_params: DbtProjectParams = None
    dbt_git_orgtasks = []
    dbt_cloud_orgtasks = []
    orgdbt = orguser.org.dbt
    if (
        payload.transformTasks and len(payload.transformTasks) > 0  ## For dbt cli & dbt cloud
    ):  # dont modify this block as its of rlocal
        logger.info("Dbt tasks being pushed to the pipeline")

        # dbt params
        dbt_project_params = DbtProjectManager.gather_dbt_project_params(orguser.org, orgdbt)

        payload.transformTasks.sort(key=lambda task: task.seq)  # sort the tasks by seq

        for transform_task in payload.transformTasks:
            org_task = OrgTask.objects.filter(uuid=transform_task.uuid).first()
            if org_task is None:
                logger.error(f"org task with {transform_task.uuid} not found")
                continue

            if org_task.task.type in ["dbt", "git"]:
                dbt_git_orgtasks.append(org_task)
            elif org_task.task.type == "dbtcloud":
                dbt_cloud_orgtasks.append(org_task)

        logger.info(f"{len(dbt_git_orgtasks)} Git/Dbt cli tasks being pushed to the pipeline")
        logger.info(f"{len(dbt_cloud_orgtasks)} Dbt cloud tasks being pushed to the pipeline")

        # dbt cli profile block
        cli_block = None
        if len(dbt_git_orgtasks) > 0:
            cli_block = OrgPrefectBlockv1.objects.filter(
                org=orguser.org, block_type=DBTCLIPROFILE
            ).first()
            if not cli_block:
                raise HttpError(400, "dbt cli profile not found")

        # dbt cloud creds block
        dbt_cloud_creds_block = None
        if len(dbt_cloud_orgtasks) > 0:
            dbt_cloud_creds_block = OrgPrefectBlockv1.objects.filter(
                org=orguser.org, block_type=DBTCLOUDCREDS
            ).first()
            if not dbt_cloud_creds_block:
                raise HttpError(400, "dbt cloud creds block not found")

        # get the deployment task configs
        task_configs, error = pipeline_with_orgtasks(
            orguser.org,
            dbt_git_orgtasks + dbt_cloud_orgtasks,
            cli_block=cli_block,
            dbt_project_params=dbt_project_params,
            start_seq=len(tasks),
            dbt_cloud_creds_block=dbt_cloud_creds_block,
        )
        if error:
            raise HttpError(400, error)
        tasks += task_configs

    map_org_tasks += dbt_git_orgtasks
    map_org_tasks += dbt_cloud_orgtasks

    # create deployment
    try:
        hash_code = generate_hash_id(8)
        deployment_name = f"pipeline-{orguser.org.slug}-{hash_code}"
        dataflow = prefect_service.create_dataflow_v1(
            PrefectDataFlowCreateSchema3(
                deployment_name=deployment_name,
                flow_name=deployment_name,
                orgslug=orguser.org.slug,
                deployment_params={"config": {"tasks": tasks, "org_slug": orguser.org.slug}},
                cron=payload.cron,
            )
        )
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to create a pipeline") from error

    org_dataflow = OrgDataFlowv1.objects.create(
        org=orguser.org,
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


@pipeline_router.get("v1/flows/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_pipelines"])
def get_prefect_dataflows_v1(request):
    """Fetch all flows/pipelines created in an organization"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    all_org_data_flows = OrgDataFlowv1.objects.filter(org=orguser.org, dataflow_type="orchestrate")

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
    for deployment in prefect_service.get_filtered_deployments(orguser.org.slug, deployment_ids):
        is_deployment_active[deployment["deploymentId"]] = (
            deployment["isScheduleActive"] if "isScheduleActive" in deployment else False
        )

    res = []

    for flow in all_org_data_flows:
        dataflow_orgtasks = [dfot for dfot in all_dataflow_orgtasks if dfot.dataflow_id == flow.id]

        org_tasks: list[OrgTask] = [
            dataflow_orgtask.orgtask for dataflow_orgtask in dataflow_orgtasks
        ]
        orgtask_ids = [org_task.id for org_task in org_tasks]

        lock = None
        all_locks = [lock for lock in all_org_task_locks if lock.orgtask_id in orgtask_ids]
        if len(all_locks) > 0:
            lock = all_locks[0]

        runs = [run for run in all_last_runs if run["deployment_id"] == flow.deployment_id]

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
                "lock": fetch_pipeline_lock_v1(flow, lock),
            }
        )

    return res


@pipeline_router.get("v1/flows/{deployment_id}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_pipeline"])
def get_prefect_dataflow_v1(request, deployment_id):
    """Fetch details of prefect deployment"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    # remove the org data flow
    org_data_flow = OrgDataFlowv1.objects.filter(
        org=orguser.org, deployment_id=deployment_id
    ).first()

    if org_data_flow is None:
        raise HttpError(404, "pipeline does not exist")

    try:
        deployment = prefect_service.get_deployment(deployment_id)
        logger.info(deployment)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to get deploymenet from prefect-proxy") from error

    connections = [
        {
            "id": dataflow_orgtask.orgtask.connection_id,
            "seq": dataflow_orgtask.seq,
            "name": airbyte_service.get_connection(
                orguser.org.airbyte_workspace_id, dataflow_orgtask.orgtask.connection_id
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
            dataflow=org_data_flow, orgtask__task__type__in=["git", "dbt", "dbtcloud"]
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


@pipeline_router.delete("v1/flows/{deployment_id}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_delete_pipeline"])
def delete_prefect_dataflow_v1(request, deployment_id):
    """Delete a prefect deployment along with its org data flow"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    # remove the org data flow
    org_data_flow = OrgDataFlowv1.objects.filter(
        org=orguser.org, deployment_id=deployment_id
    ).first()

    if not org_data_flow:
        raise HttpError(404, "pipeline not found")

    prefect_service.delete_deployment_by_id(deployment_id)

    org_data_flow.delete()

    return {"success": 1}


@pipeline_router.put("v1/flows/{deployment_id}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_edit_pipeline"])
def put_prefect_dataflow_v1(request, deployment_id, payload: PrefectDataFlowUpdateSchema3):
    """Edit the data flow / prefect deployment"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    # the org data flow
    org_data_flow = OrgDataFlowv1.objects.filter(
        org=orguser.org, deployment_id=deployment_id
    ).first()

    if not org_data_flow:
        raise HttpError(404, "pipeline not found")

    tasks = []
    map_org_tasks = []  # seq of org tasks to be mapped in pipeline/ dataflow

    # push sync tasks to pipeline
    sync_orgtasks = []

    if len(payload.connections) > 0:
        # check if pipeline has airbyte syncs
        org_server_block = OrgPrefectBlockv1.objects.filter(
            org=orguser.org, block_type=AIRBYTESERVER
        ).first()
        if not org_server_block:
            raise HttpError(400, "airbyte server block not found")

        payload.connections.sort(key=lambda conn: conn.seq)
        for connection in payload.connections:
            logger.info(connection)
            org_task = OrgTask.objects.filter(
                org=orguser.org,
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

        # get the deployment task configs
        task_configs, error = pipeline_with_orgtasks(
            orguser.org,
            sync_orgtasks,
            server_block=org_server_block,
        )
        if error:
            raise HttpError(400, error)
        tasks += task_configs

    map_org_tasks += sync_orgtasks
    logger.info(f"Updating pipline to have {len(sync_orgtasks)} airbyte syncs")

    # push dbt pipeline orgtasks
    dbt_project_params = None
    dbt_git_orgtasks = []
    dbt_cloud_orgtasks = []
    orgdbt = orguser.org.dbt
    if payload.transformTasks and len(payload.transformTasks) > 0:
        logger.info(f"Dbt tasks being pushed to the pipeline")

        # dbt params
        dbt_project_params: DbtProjectParams = DbtProjectManager.gather_dbt_project_params(
            orguser.org, orgdbt
        )

        payload.transformTasks.sort(key=lambda task: task.seq)  # sort the tasks by seq

        for transform_task in payload.transformTasks:
            org_task = OrgTask.objects.filter(uuid=transform_task.uuid).first()
            if org_task is None:
                logger.error(f"org task with {transform_task.uuid} not found")
                continue

            if org_task.task.type in ["dbt", "git"]:
                dbt_git_orgtasks.append(org_task)
            elif org_task.task.type == "dbtcloud":
                dbt_cloud_orgtasks.append(org_task)

        # dbt cli profile block
        cli_block = None
        if len(dbt_git_orgtasks) > 0:
            cli_block = OrgPrefectBlockv1.objects.filter(
                org=orguser.org, block_type=DBTCLIPROFILE
            ).first()
            if not cli_block:
                raise HttpError(400, "dbt cli profile not found")

        # dbt cloud creds block
        dbt_cloud_creds_block = None
        if len(dbt_cloud_orgtasks) > 0:
            dbt_cloud_creds_block = OrgPrefectBlockv1.objects.filter(
                org=orguser.org, block_type=DBTCLOUDCREDS
            ).first()
            if not dbt_cloud_creds_block:
                raise HttpError(400, "dbt cloud creds block not found")

        # get the deployment task configs
        task_configs, error = pipeline_with_orgtasks(
            orguser.org,
            dbt_git_orgtasks + dbt_cloud_orgtasks,
            cli_block=cli_block,
            dbt_project_params=dbt_project_params,
            start_seq=len(tasks),
            dbt_cloud_creds_block=dbt_cloud_creds_block,
        )
        if error:
            raise HttpError(400, error)
        tasks += task_configs

    map_org_tasks += dbt_git_orgtasks
    map_org_tasks += dbt_cloud_orgtasks

    # update deployment
    payload.deployment_params = {"config": {"tasks": tasks, "org_slug": orguser.org.slug}}
    try:
        prefect_service.update_dataflow_v1(deployment_id, payload)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to update a pipeline") from error

    # Delete mapping
    DataflowOrgTask.objects.filter(dataflow=org_data_flow).delete()

    # re-map orgtasks to dataflow
    for idx, org_task in enumerate(map_org_tasks):
        DataflowOrgTask.objects.create(dataflow=org_data_flow, orgtask=org_task, seq=idx)

    org_data_flow.cron = payload.cron if payload.cron else None
    org_data_flow.name = payload.name
    org_data_flow.save()

    return {"success": 1}


@pipeline_router.post(
    "flows/{deployment_id}/set_schedule/{status}", auth=auth.CustomAuthMiddleware()
)
@has_permission(["can_edit_pipeline"])
def post_deployment_set_schedule(request, deployment_id, status):
    """Set deployment schedule to active / inactive"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    if (
        (status is None)
        or (isinstance(status, str) is not True)
        or (status not in ["active", "inactive"])
    ):
        raise HttpError(422, "incorrect status value")

    try:
        prefect_service.set_deployment_schedule(deployment_id, status)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to change flow state") from error
    return {"success": 1}


################################## runs and logs related ######################################


@pipeline_router.post("v1/flows/{deployment_id}/flow_run/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_run_pipeline"])
def post_run_prefect_org_deployment_task(request, deployment_id, payload: TaskParameters = None):
    """
    Run deployment based task.
    Can run
        - airbtye sync
        - dbt run
        - quick run of pipeline
    """
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    dataflow = OrgDataFlowv1.objects.filter(org=orguser.org, deployment_id=deployment_id).first()

    dataflow_orgtasks = (
        DataflowOrgTask.objects.filter(dataflow=dataflow).order_by("seq").select_related("orgtask")
    )

    if dataflow_orgtasks.count() == 0:
        raise HttpError(400, "no org task mapped to the deployment")

    orgdbt = orguser.org.dbt

    # ordered
    org_tasks: list[OrgTask] = [dataflow_orgtask.orgtask for dataflow_orgtask in dataflow_orgtasks]

    locks = lock_tasks_for_dataflow(orguser=orguser, dataflow=dataflow, org_tasks=org_tasks)

    try:
        # allow parameter passing only for manual dbt runs and if the there are parameters being passed
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
            cli_profile_block = OrgPrefectBlockv1.objects.filter(
                org=orguser.org, block_type=DBTCLIPROFILE
            ).first()
            dbt_project_params: DbtProjectParams = DbtProjectManager.gather_dbt_project_params(
                orguser.org, orgdbt
            )

            # dont set any parameters if cli block is not present or there is an error
            if cli_profile_block and not error:
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
                        "org_slug": orguser.org.slug,
                    }
                }

        res = prefect_service.create_deployment_flow_run(deployment_id, flow_run_params)
    except Exception as error:
        for task_lock in locks:
            logger.info("deleting TaskLock %s", task_lock.orgtask.task.slug)
            task_lock.delete()
        logger.exception(error)
        raise HttpError(400, "failed to start a run") from error

    for tasklock in locks:
        tasklock.flow_run_id = res["flow_run_id"]
        tasklock.save()

    return res


@pipeline_router.get("flow_runs/{flow_run_id}/logs", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_pipeline"])
def get_flow_runs_logs(
    request, flow_run_id, task_run_id="", limit: int = 0, offset: int = 0
):  # pylint: disable=unused-argument
    """return the logs from a flow-run"""
    try:
        result = prefect_service.get_flow_run_logs(flow_run_id, task_run_id, limit, offset)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to retrieve logs") from error
    return result


@pipeline_router.get("flow_runs/{flow_run_id}/logsummary", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_pipeline"])
def get_flow_runs_logsummary(request, flow_run_id):  # pylint: disable=unused-argument
    """return the logs from a flow-run"""
    try:
        connection_info = {
            "host": os.getenv("PREFECT_HOST"),
            "port": os.getenv("PREFECT_PORT"),
            "database": os.getenv("PREFECT_DB"),
            "user": os.getenv("PREFECT_USER"),
            "password": os.getenv("PREFECT_PASSWORD"),
        }
        result = parse_prefect_logs(connection_info, flow_run_id)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to retrieve logs") from error
    return result


@pipeline_router.get(
    "flow_runs/{flow_run_id}",
    auth=auth.CustomAuthMiddleware(),
    response=PrefectFlowRunSchema,
)
@has_permission(["can_view_pipeline"])
def get_flow_run_by_id(request, flow_run_id):
    # pylint: disable=unused-argument
    """fetch a flow run from prefect"""
    try:
        flow_run = prefect_service.get_flow_run(flow_run_id)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to retrieve logs") from error
    return flow_run


@pipeline_router.get("flows/{deployment_id}/flow_runs/history", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_pipeline"])
def get_prefect_flow_runs_log_history(request, deployment_id, limit: int = 0, fetchlogs=True):
    # pylint: disable=unused-argument
    """Fetch all flow runs for the deployment and the logs for each flow run"""
    flow_runs = prefect_service.get_flow_runs_by_deployment_id(deployment_id, limit=limit)

    if fetchlogs:
        for flow_run in flow_runs:
            flow_run["logs"] = prefect_service.recurse_flow_run_logs(flow_run["id"], None)

    return flow_runs


@pipeline_router.get("v1/flows/{deployment_id}/flow_runs/history", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_pipeline"])
def get_prefect_flow_runs_log_history_v1(request, deployment_id, limit: int = 0, offset: int = 0):
    # pylint: disable=unused-argument
    """Fetch all flow runs for the deployment and the logs for each flow run"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(404, "organization not found")

    flow_runs = prefect_service.get_flow_runs_by_deployment_id_v1(
        deployment_id=deployment_id, limit=limit, offset=offset
    )

    for flow_run in flow_runs:
        runs = prefect_service.get_flow_run_graphs(flow_run["id"])
        # attach connection name to each flow run
        for run in runs:
            if "parameters" in run and run["kind"] == "flow-run":
                connection_id = run["parameters"]["connection_id"]
                connection = airbyte_service.get_connection(
                    orguser.org.airbyte_workspace_id, connection_id
                )
                run["parameters"]["connection_name"] = connection["name"]

        flow_run["runs"] = runs

    return flow_runs


@pipeline_router.get("v1/flow_runs/{flow_run_id}/logsummary", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_pipeline"])
def get_flow_runs_logsummary_v1(
    request, flow_run_id, task_id, regenerate: int = 0, request_uuid: str = None
):  # pylint: disable=unused-argument
    """
    Use llms to summarize logs. Summarize logs of a task run in the pipeline run
    """
    try:
        orguser: OrgUser = request.orguser
        task = summarize_logs.apply_async(
            kwargs={
                "orguser_id": orguser.id,
                "type": LogsSummarizationType.DEPLOYMENT,
                "flow_run_id": flow_run_id,
                "task_id": task_id,
                "regenerate": regenerate == 1,
            },
        )
        return {"task_id": task.id}
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to retrieve logs") from error


@pipeline_router.post("flow_runs/{flow_run_id}/set_state", auth=auth.CustomAuthMiddleware())
@has_permission(["can_edit_pipeline"])
def cancel_queued_manual_job(request, flow_run_id, payload: TaskStateSchema):
    """cancel a queued manual job"""
    try:
        orguser: OrgUser = request.orguser
        if orguser.org is None:
            raise HttpError(400, "register an organization first")

        flow_run = prefect_service.get_flow_run(flow_run_id)
        if flow_run is None:
            raise HttpError(400, "Please provide a valid flow_run_id")

        if "deployment_id" not in flow_run:
            raise HttpError(400, "Can only cancel flow_runs with deployment_id")

        dataflow = OrgDataFlowv1.objects.filter(
            org=orguser.org, deployment_id=flow_run["deployment_id"]
        ).first()
        if dataflow is None:
            raise HttpError(403, "You don't have access to this flow run")

        res = prefect_service.cancel_queued_manual_job(flow_run_id, payload.dict())
    except HttpError as http_error:
        # We handle HttpError separately to ensure the correct message is raised
        logger.exception(http_error)
        raise http_error
    except Exception as error:
        # For other exceptions,
        logger.exception(error)
        raise HttpError(400, "failed to cancel the queued manual job") from error
    return res
