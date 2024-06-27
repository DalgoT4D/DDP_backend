import os

from ninja import NinjaAPI
from ninja.errors import HttpError

from ninja.errors import ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError

from ddpui import auth
from ddpui.ddpprefect import prefect_service
from ddpui.ddpairbyte import airbyte_service

from ddpui.ddpprefect import (
    DBTCLIPROFILE,
    AIRBYTESERVER,
)
from ddpui.models.org import OrgDataFlowv1, OrgPrefectBlockv1
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import DataflowOrgTask, OrgTask
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
    PrefectFlowRunSchema,
    PrefectDataFlowUpdateSchema3,
    PrefectDataFlowCreateSchema4,
)
from ddpui.utils.constants import TASK_DBTRUN, TASK_AIRBYTESYNC
from ddpui.utils.custom_logger import CustomLogger
from ddpui.schemas.org_task_schema import TaskParameters
from ddpui.utils.prefectlogs import parse_prefect_logs
from ddpui.utils.helpers import generate_hash_id
from ddpui.core.pipelinefunctions import (
    setup_dbt_core_task_config,
    pipeline_with_orgtasks,
    fetch_pipeline_lock,
)
from ddpui.celeryworkers.tasks import summarize_deployment_flow_run_logs
from ddpui.core.dbtfunctions import gather_dbt_project_params
from ddpui.auth import has_permission

pipelineapi = NinjaAPI(urls_namespace="pipeline")
# http://127.0.0.1:8000/api/docs


logger = CustomLogger("ddpui")


@pipelineapi.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """
    Handle any ninja validation errors raised in the apis
    These are raised during request payload validation
    exc.errors is correct
    """
    return Response({"detail": exc.errors}, status=422)


@pipelineapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """
    Handle any pydantic errors raised in the apis
    These are raised during response payload validation
    exc.errors() is correct
    """
    return Response({"detail": exc.errors()}, status=500)


@pipelineapi.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception  # skipcq PYL-W0613
):  # pylint: disable=unused-argument
    """Handle any other exception raised in the apis"""
    logger.info(exc)
    return Response({"detail": "something went wrong"}, status=500)


@pipelineapi.post("v1/flows/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_create_pipeline"])
def post_prefect_dataflow_v1(request, payload: PrefectDataFlowCreateSchema4):
    """Create a prefect deployment i.e. a ddp dataflow"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    if payload.name in [None, ""]:
        raise HttpError(400, "must provide a name for the flow")

    tasks = []
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
    dbt_project_params = None
    dbt_git_orgtasks = []
    if payload.transformTasks and len(payload.transformTasks) > 0:
        logger.info("Dbt tasks being pushed to the pipeline")

        # dbt params
        dbt_project_params, error = gather_dbt_project_params(orguser.org)
        if error:
            raise HttpError(400, error)

        # dbt cli profile block
        cli_block = OrgPrefectBlockv1.objects.filter(
            org=orguser.org, block_type=DBTCLIPROFILE
        ).first()
        if not cli_block:
            raise HttpError(400, "dbt cli profile not found")

        payload.transformTasks.sort(key=lambda task: task.seq)  # sort the tasks by seq

        for transform_task in payload.transformTasks:
            org_task = OrgTask.objects.filter(uuid=transform_task.uuid).first()
            if org_task is None:
                logger.error(f"org task with {transform_task.uuid} not found")
                continue

            # map this org task to dataflow
            dbt_git_orgtasks.append(org_task)

        # get the deployment task configs
        task_configs, error = pipeline_with_orgtasks(
            orguser.org,
            dbt_git_orgtasks,
            cli_block=cli_block,
            dbt_project_params=dbt_project_params,
            start_seq=len(tasks),
        )
        if error:
            raise HttpError(400, error)
        tasks += task_configs

    map_org_tasks += dbt_git_orgtasks

    # create deployment
    try:
        hash_code = generate_hash_id(8)
        deployment_name = f"pipeline-{orguser.org.slug}-{hash_code}"
        dataflow = prefect_service.create_dataflow_v1(
            PrefectDataFlowCreateSchema3(
                deployment_name=deployment_name,
                flow_name=deployment_name,
                orgslug=orguser.org.slug,
                deployment_params={
                    "config": {"tasks": tasks, "org_slug": orguser.org.slug}
                },
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


@pipelineapi.get("v1/flows/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_pipelines"])
def get_prefect_dataflows_v1(request):
    """Fetch all flows/pipelines created in an organization"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    org_data_flows = OrgDataFlowv1.objects.filter(
        org=orguser.org,
        dataflow_type="orchestrate",
    ).all()

    deployment_ids = [flow.deployment_id for flow in org_data_flows]

    # dictionary to hold {"id": status}
    is_deployment_active = {}

    # setting active/inactive status based on if the schedule is set or not
    for deployment in prefect_service.get_filtered_deployments(
        orguser.org.slug, deployment_ids
    ):
        is_deployment_active[deployment["deploymentId"]] = (
            deployment["isScheduleActive"]
            if "isScheduleActive" in deployment
            else False
        )

    res = []

    for flow in org_data_flows:

        res.append(
            {
                "name": flow.name,
                "deploymentId": flow.deployment_id,
                "cron": flow.cron,
                "deploymentName": flow.deployment_name,
                "lastRun": prefect_service.get_last_flow_run_by_deployment_id(
                    flow.deployment_id
                ),
                "status": (
                    is_deployment_active[flow.deployment_id]
                    if flow.deployment_id in is_deployment_active
                    else False
                ),
                "lock": fetch_pipeline_lock(flow),
            }
        )

    return res


@pipelineapi.get("v1/flows/{deployment_id}", auth=auth.CustomAuthMiddleware())
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
            dataflow=org_data_flow, orgtask__task__type__in=["git", "dbt"]
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


@pipelineapi.delete("v1/flows/{deployment_id}", auth=auth.CustomAuthMiddleware())
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


@pipelineapi.put("v1/flows/{deployment_id}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_edit_pipeline"])
def put_prefect_dataflow_v1(
    request, deployment_id, payload: PrefectDataFlowUpdateSchema3
):
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
    if payload.transformTasks and len(payload.transformTasks) > 0:
        logger.info(f"Dbt tasks being pushed to the pipeline")

        # dbt params
        dbt_project_params, error = gather_dbt_project_params(orguser.org)
        if error:
            raise HttpError(400, error)

        # dbt cli profile block
        cli_block = OrgPrefectBlockv1.objects.filter(
            org=orguser.org, block_type=DBTCLIPROFILE
        ).first()
        if not cli_block:
            raise HttpError(400, "dbt cli profile not found")

        payload.transformTasks.sort(key=lambda task: task.seq)  # sort the tasks by seq

        for transform_task in payload.transformTasks:
            org_task = OrgTask.objects.filter(uuid=transform_task.uuid).first()
            if org_task is None:
                logger.error(f"org task with {transform_task.uuid} not found")
                continue

            # map this org task to dataflow
            dbt_git_orgtasks.append(org_task)

        # get the deployment task configs
        task_configs, error = pipeline_with_orgtasks(
            orguser.org,
            dbt_git_orgtasks,
            cli_block=cli_block,
            dbt_project_params=dbt_project_params,
            start_seq=len(tasks),
        )
        logger.info("HERE")
        logger.info(task_configs)
        if error:
            raise HttpError(400, error)
        tasks += task_configs

    map_org_tasks += dbt_git_orgtasks

    # update deployment
    payload.deployment_params = {
        "config": {"tasks": tasks, "org_slug": orguser.org.slug}
    }
    try:
        prefect_service.update_dataflow_v1(deployment_id, payload)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to update a pipeline") from error

    # Delete mapping
    DataflowOrgTask.objects.filter(dataflow=org_data_flow).delete()

    # re-map orgtasks to dataflow
    for idx, org_task in enumerate(map_org_tasks):
        DataflowOrgTask.objects.create(
            dataflow=org_data_flow, orgtask=org_task, seq=idx
        )

    org_data_flow.cron = payload.cron if payload.cron else None
    org_data_flow.name = payload.name
    org_data_flow.save()

    return {"success": 1}


@pipelineapi.post(
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


@pipelineapi.post(
    "v1/flows/{deployment_id}/flow_run/", auth=auth.CustomAuthMiddleware()
)
@has_permission(["can_run_pipeline"])
def post_run_prefect_org_deployment_task(
    request, deployment_id, payload: TaskParameters = None
):
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

    dataflow_orgtask = DataflowOrgTask.objects.filter(
        dataflow__deployment_id=deployment_id
    ).first()

    if dataflow_orgtask is None:
        raise HttpError(400, "no org task mapped to the deployment")

    locks = prefect_service.lock_tasks_for_deployment(deployment_id, orguser)

    try:
        # allow parameter passing only for manual dbt runs and if the there are parameters being passed
        flow_run_params = None
        if (
            dataflow_orgtask.dataflow.dataflow_type == "manual"
            and dataflow_orgtask.orgtask.task.slug == TASK_DBTRUN
            and payload
            and (payload.flags or payload.options)
        ):
            logger.info("sending custom flow run params to the deployment run")
            orgtask = dataflow_orgtask.orgtask

            # save orgtask params to memory and not db
            orgtask.parameters = dict(payload)

            # fetch cli block
            cli_profile_block = OrgPrefectBlockv1.objects.filter(
                org=orguser.org, block_type=DBTCLIPROFILE
            ).first()
            dbt_project_params, error = gather_dbt_project_params(orguser.org)

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


@pipelineapi.get("flow_runs/{flow_run_id}/logs", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_pipeline"])
def get_flow_runs_logs(
    request, flow_run_id, offset: int = 0
):  # pylint: disable=unused-argument
    """return the logs from a flow-run"""
    try:
        result = prefect_service.get_flow_run_logs(flow_run_id, offset)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to retrieve logs") from error
    return result


@pipelineapi.get("flow_runs/{flow_run_id}/logsummary", auth=auth.CustomAuthMiddleware())
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


@pipelineapi.get(
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


@pipelineapi.get(
    "flows/{deployment_id}/flow_runs/history", auth=auth.CustomAuthMiddleware()
)
@has_permission(["can_view_pipeline"])
def get_prefect_flow_runs_log_history(
    request, deployment_id, limit: int = 0, fetchlogs=True
):
    # pylint: disable=unused-argument
    """Fetch all flow runs for the deployment and the logs for each flow run"""
    flow_runs = prefect_service.get_flow_runs_by_deployment_id(
        deployment_id, limit=limit
    )

    if fetchlogs:
        for flow_run in flow_runs:
            logs_dict = prefect_service.get_flow_run_logs(flow_run["id"], 0)
            flow_run["logs"] = (
                logs_dict["logs"]["logs"] if "logs" in logs_dict["logs"] else []
            )

    return flow_runs


@pipelineapi.get(
    "v1/flow_runs/{flow_run_id}/logsummary", auth=auth.CustomAuthMiddleware()
)
@has_permission(["can_view_pipeline"])
def get_flow_runs_logsummary_v1(
    request, flow_run_id, regenerate
):  # pylint: disable=unused-argument
    """
    Use llms to summarize logs
    """
    try:
        orguser: OrgUser = request.orguser
        task = summarize_deployment_flow_run_logs.delay(
            flow_run_id, orguser.id, regenerate
        )
        return {"task_id": task.id}
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to retrieve logs") from error
