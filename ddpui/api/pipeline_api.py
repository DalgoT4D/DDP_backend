import os
from pathlib import Path
from datetime import datetime
import yaml

from ninja import NinjaAPI
from ninja.errors import HttpError

from ninja.errors import ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError

from ddpui import auth
from ddpui.ddpprefect import prefect_service
from ddpui.ddpairbyte import airbyte_service

from ddpui.ddpprefect import (
    DBTCORE,
    SHELLOPERATION,
    DBTCLIPROFILE,
    SECRET,
    AIRBYTECONNECTION,
    AIRBYTESERVER,
)
from ddpui.models.org import (
    OrgPrefectBlock,
    OrgWarehouse,
    OrgDataFlow,
    OrgDataFlowv1,
    OrgPrefectBlockv1,
)
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import Task, DataflowOrgTask, OrgTask, TaskLock
from ddpui.ddpprefect.schema import (
    PrefectAirbyteSync,
    PrefectDbtTaskSetup,
    PrefectDataFlowCreateSchema3,
    PrefectFlowRunSchema,
    PrefectDataFlowUpdateSchema3,
    PrefectSecretBlockCreate,
    PrefectShellTaskSetup,
    PrefectDataFlowCreateSchema4,
)

from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager
from ddpui.utils import timezone
from ddpui.utils.constants import (
    TASK_DBTRUN,
    TASK_GITPULL,
    TRANSFORM_TASKS_SEQ,
    AIRBYTE_SYNC_TIMEOUT,
)
from ddpui.utils.prefectlogs import parse_prefect_logs
from ddpui.utils.helpers import generate_hash_id
from ddpui.core.pipelinefunctions import pipeline_sync_tasks, pipeline_dbt_git_tasks
from ddpui.core.dbtfunctions import gather_dbt_project_params

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


@pipelineapi.post("v1/flows/", auth=auth.CanManagePipelines())
def post_prefect_dataflow_v1(request, payload: PrefectDataFlowCreateSchema4):
    """Create a prefect deployment i.e. a ddp dataflow"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    if payload.name in [None, ""]:
        raise HttpError(400, "must provide a name for the flow")

    tasks = []
    map_org_tasks = []  # map org tasks to dataflow

    # check if pipeline has airbyte syncs
    if len(payload.connections) > 0:
        org_server_block = OrgPrefectBlockv1.objects.filter(
            org=orguser.org, block_type=AIRBYTESERVER
        ).first()
        if not org_server_block:
            raise HttpError(400, "airbyte server block not found")

        # push sync tasks to pipeline
        (org_tasks, task_configs), error = pipeline_sync_tasks(
            orguser.org, payload.connections, org_server_block
        )
        if error:
            raise HttpError(400, error)
        tasks += task_configs
        map_org_tasks += org_tasks

    seq = len(tasks)
    logger.info(f"Pipline has {seq} airbyte syncs")

    # check if pipeline has dbt transformation
    if payload.dbtTransform == "yes":
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

        # push dbt pipeline tasks
        (org_tasks, task_configs), error = pipeline_dbt_git_tasks(
            orguser.org, cli_block, dbt_project_params, seq
        )
        if error:
            raise HttpError(400, error)
        tasks += task_configs
        map_org_tasks += org_tasks

        logger.info(f"Dbt tasks pushed to the pipeline")

    # create deployment
    try:
        hash_code = generate_hash_id(8)
        deployment_name = f"pipeline-{orguser.org.slug}-{hash_code}"
        dataflow = prefect_service.create_dataflow_v1(
            PrefectDataFlowCreateSchema3(
                deployment_name=deployment_name,
                flow_name=deployment_name,
                orgslug=orguser.org.slug,
                deployment_params={"config": {"tasks": tasks}},
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

    for org_task in map_org_tasks:
        DataflowOrgTask.objects.create(dataflow=org_dataflow, orgtask=org_task)

    return {
        "deploymentId": org_dataflow.deployment_id,
        "name": org_dataflow.name,
        "cron": org_dataflow.cron,
    }


@pipelineapi.get("v1/flows/", auth=auth.CanManagePipelines())
def get_prefect_dataflows_v1(request):
    """Fetch all flows/pipelines created in an organization"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    org_data_flows = OrgDataFlowv1.objects.filter(
        org=orguser.org, dataflow_type="orchestrate"
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
        org_task_ids = DataflowOrgTask.objects.filter(dataflow=flow).values_list(
            "orgtask_id", flat=True
        )

        lock = TaskLock.objects.filter(orgtask_id__in=org_task_ids).first()

        res.append(
            {
                "name": flow.name,
                "deploymentId": flow.deployment_id,
                "cron": flow.cron,
                "deploymentName": flow.deployment_name,
                "lastRun": prefect_service.get_last_flow_run_by_deployment_id(
                    flow.deployment_id
                ),
                "status": is_deployment_active[flow.deployment_id]
                if flow.deployment_id in is_deployment_active
                else False,
                "lock": {
                    "lockedBy": lock.locked_by.user.email,
                    "lockedAt": lock.locked_at,
                }
                if lock
                else None,
            }
        )

    return res


@pipelineapi.get("v1/flows/{deployment_id}", auth=auth.CanManagePipelines())
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
            "id": task["connection_id"],
            "seq": task["seq"],
            "name": airbyte_service.get_connection(
                orguser.org.airbyte_workspace_id, task["connection_id"]
            )["name"],
        }
        for task in deployment["parameters"]["config"]["tasks"]
        if task["type"] == AIRBYTECONNECTION
    ]

    has_transform = (
        len(
            [
                task
                for task in deployment["parameters"]["config"]["tasks"]
                if task["type"] in [DBTCORE, SHELLOPERATION]
            ]
        )
        > 0
    )

    # differentiate between deploymentName and name
    deployment["deploymentName"] = deployment["name"]
    deployment["name"] = org_data_flow.name

    return {
        "name": org_data_flow.name,
        "deploymentName": deployment["deploymentName"],
        "cron": deployment["cron"],
        "connections": connections,
        "dbtTransform": "yes" if has_transform else "no",
        "isScheduleActive": deployment["isScheduleActive"],
    }


@pipelineapi.delete("v1/flows/{deployment_id}", auth=auth.CanManagePipelines())
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


@pipelineapi.put("v1/flows/{deployment_id}", auth=auth.CanManagePipelines())
def put_prefect_dataflow_v1(
    request, deployment_id, payload: PrefectDataFlowUpdateSchema3
):
    """Edit the data flow / prefect deployment. For now only the schedules can be edited"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    # the org data flow
    org_data_flow = OrgDataFlowv1.objects.filter(
        org=orguser.org, deployment_id=deployment_id
    ).first()

    if not org_data_flow:
        raise HttpError(404, "pipeline not found")

    seq = 0  # global sequence for all tasks
    tasks = []
    map_org_tasks = []  # map org tasks to dataflow
    delete_dataflow_orgtask_type = []

    # check if pipeline has airbyte syncs
    org_server_block = OrgPrefectBlockv1.objects.filter(
        org=orguser.org, block_type=AIRBYTESERVER
    ).first()
    if not org_server_block:
        raise HttpError(400, "airbyte server block not found")

    # delete all airbyte sync DataflowOrgTask
    delete_dataflow_orgtask_type.append("airbyte")

    # push sync tasks to pipeline
    payload.connections.sort(key=lambda conn: conn.seq)
    for connection in payload.connections:
        logger.info(connection)
        org_task = OrgTask.objects.filter(
            org=orguser.org, connection_id=connection.id
        ).first()
        if org_task is None:
            logger.info(
                f"connection id {connection.id} not found in org tasks; ignoring this airbyte sync"
            )
            continue
        # map this org task to dataflow
        map_org_tasks.append(org_task)

        logger.info(
            f"connection id {connection.id} found in org tasks; pushing to pipeline"
        )
        seq += 1
        task_config = {
            "seq": seq,
            "slug": org_task.task.slug,
            "type": AIRBYTECONNECTION,
            "airbyte_server_block": org_server_block.block_name,
            "connection_id": connection.id,
            "timeout": AIRBYTE_SYNC_TIMEOUT,
        }
        tasks.append(task_config)

    logger.info(f"Pipline has {seq} airbyte syncs")

    # check if pipeline has dbt transformation
    if payload.dbtTransform == "yes":
        logger.info(f"Dbt tasks being pushed to the pipeline")

        # dbt params
        dbt_env_dir = Path(orguser.org.dbt.dbt_venv)
        if not dbt_env_dir.exists():
            raise HttpError(400, "create the dbt env first")

        dbt_binary = str(dbt_env_dir / "venv/bin/dbt")
        dbtrepodir = Path(os.getenv("CLIENTDBT_ROOT")) / orguser.org.slug / "dbtrepo"
        project_dir = str(dbtrepodir)
        target = orguser.org.dbt.default_schema

        # dbt cli profile block
        cli_block = OrgPrefectBlockv1.objects.filter(
            org=orguser.org, block_type=DBTCLIPROFILE
        ).first()
        if not cli_block:
            raise HttpError(400, "dbt cli profile not found")

        # delete all transform related DataflowOrgTask
        delete_dataflow_orgtask_type.append("dbt")
        delete_dataflow_orgtask_type.append("git")

        # push dbt pipeline tasks
        for org_task in OrgTask.objects.filter(
            org=orguser.org, task__type__in=["dbt", "git"]
        ).all():
            logger.info(
                f"found transform task {org_task.task.slug}; pushing to pipeline"
            )
            # map this org task to dataflow
            map_org_tasks.append(org_task)

            dbt_core_task_setup = PrefectDbtTaskSetup(
                seq=TRANSFORM_TASKS_SEQ[org_task.task.slug] + seq,
                slug=org_task.task.slug,
                commands=[f"{dbt_binary} {org_task.task.command} --target {target}"],
                type=DBTCORE,
                env={},
                working_dir=project_dir,
                profiles_dir=f"{project_dir}/profiles/",
                project_dir=project_dir,
                cli_profile_block=cli_block.block_name,
                cli_args=[],
            )

            task_config = dict(dbt_core_task_setup)

            # update task_config its a git pull task
            if org_task.task.slug == TASK_GITPULL:
                shell_env = {"secret-git-pull-url-block": ""}

                gitpull_secret_block = OrgPrefectBlockv1.objects.filter(
                    org=orguser.org, block_type=SECRET, block_name__contains="git-pull"
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
        logger.info(f"Dbt tasks pushed to the pipeline")

    # update deployment
    payload.deployment_params = {"config": {"tasks": tasks}}
    try:
        prefect_service.update_dataflow_v1(deployment_id, payload)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to update a pipeline") from error

    # Delete mapping
    DataflowOrgTask.objects.filter(
        dataflow=org_data_flow, orgtask__task__type__in=delete_dataflow_orgtask_type
    ).delete()

    # create mapping
    for org_task in map_org_tasks:
        DataflowOrgTask.objects.create(dataflow=org_data_flow, orgtask=org_task)

    org_data_flow.cron = payload.cron if payload.cron else None
    org_data_flow.name = payload.name
    org_data_flow.save()

    return {"success": 1}


@pipelineapi.post(
    "flows/{deployment_id}/set_schedule/{status}", auth=auth.CanManagePipelines()
)
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


@pipelineapi.post("v1/flows/{deployment_id}/flow_run/", auth=auth.CanManagePipelines())
def post_run_prefect_org_deployment_task(request, deployment_id):
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
        res = prefect_service.create_deployment_flow_run(deployment_id)
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


@pipelineapi.get("flow_runs/{flow_run_id}/logs", auth=auth.CanManagePipelines())
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


@pipelineapi.get("flow_runs/{flow_run_id}/logsummary", auth=auth.CanManagePipelines())
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
    auth=auth.CanManagePipelines(),
    response=PrefectFlowRunSchema,
)
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
    "flows/{deployment_id}/flow_runs/history", auth=auth.CanManagePipelines()
)
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
