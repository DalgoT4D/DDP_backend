from typing import List

from ninja import Router
from ninja.errors import HttpError


from ddpui.ddpprefect import prefect_service
from ddpui.ddpairbyte import airbyte_service

from ddpui.models.org import OrgDataFlowv1, Org
from ddpui.models.org_user import OrgUser
from ddpui.models.llm import LogsSummarizationType
from ddpui.ddpprefect.schema import (
    PrefectFlowRunSchema,
    PrefectDataFlowCreateSchema4,
    PrefectDataFlowUpdateSchema3,
    TaskStateSchema,
    PrefectGetDataflowsResponse,
)
from ddpui.utils.custom_logger import CustomLogger
from ddpui.schemas.org_task_schema import (
    TaskParameters,
    ClearSelectedStreams,
)
from ddpui.celeryworkers.tasks import summarize_logs
from ddpui.core.orchestrate.pipeline_service import (
    PipelineService,
    PipelineNotFoundError,
    PipelineValidationError,
    PipelineConfigurationError,
    PipelineServiceError,
)
from ddpui.auth import has_permission

pipeline_router = Router()
logger = CustomLogger("ddpui")


@pipeline_router.post("v1/flows/")
@has_permission(["can_create_pipeline"])
def post_prefect_dataflow_v1(request, payload: PrefectDataFlowCreateSchema4):
    """Create a prefect deployment i.e. a ddp dataflow"""
    orguser: OrgUser = request.orguser
    org: Org = orguser.org

    if org is None:
        raise HttpError(400, "register an organization first")

    try:
        result = PipelineService.create_pipeline(org, payload)
        return result
    except PipelineValidationError as error:
        raise HttpError(400, error.message) from error
    except PipelineConfigurationError as error:
        raise HttpError(422, error.message) from error
    except PipelineServiceError as error:
        raise HttpError(500, error.message) from error
    except Exception as error:
        logger.exception(f"Unexpected error in create_pipeline: {error}")
        raise HttpError(500, f"An unexpected error occurred: {str(error)}") from error


@pipeline_router.get("v1/flows/", response=List[PrefectGetDataflowsResponse])
@has_permission(["can_view_pipelines"])
def get_prefect_dataflows_v1(request):
    """Fetch all flows/pipelines created in an organization"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    try:
        result = PipelineService.get_pipelines(orguser.org)
        return result
    except PipelineServiceError as error:
        raise HttpError(500, error.message) from error
    except Exception as error:
        logger.exception(f"Unexpected error in get_prefect_dataflows_v1: {error}")
        raise HttpError(500, f"An unexpected error occurred: {str(error)}") from error


@pipeline_router.get("v1/flows/{deployment_id}")
@has_permission(["can_view_pipeline"])
def get_prefect_dataflow_v1(request, deployment_id):
    """Fetch details of prefect deployment"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    try:
        result = PipelineService.get_pipeline_details(orguser.org, deployment_id)
        return result
    except PipelineNotFoundError:
        raise HttpError(404, "pipeline does not exist")
    except PipelineServiceError as error:
        raise HttpError(500, error.message) from error
    except Exception as error:
        logger.exception(f"Unexpected error in get_pipeline_details: {error}")
        raise HttpError(500, f"An unexpected error occurred: {str(error)}") from error


@pipeline_router.delete("v1/flows/{deployment_id}")
@has_permission(["can_delete_pipeline"])
def delete_prefect_dataflow_v1(request, deployment_id):
    """Delete a prefect deployment along with its org data flow"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    try:
        result = PipelineService.delete_pipeline(orguser.org, deployment_id)
        return result
    except PipelineNotFoundError:
        raise HttpError(404, "pipeline not found")
    except PipelineServiceError as error:
        raise HttpError(500, error.message) from error
    except Exception as error:
        logger.exception(f"Unexpected error in delete_pipeline: {error}")
        raise HttpError(500, f"An unexpected error occurred: {str(error)}") from error


@pipeline_router.put("v1/flows/{deployment_id}")
@has_permission(["can_edit_pipeline"])
def put_prefect_dataflow_v1(request, deployment_id, payload: PrefectDataFlowUpdateSchema3):
    """Edit the data flow / prefect deployment"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    try:
        result = PipelineService.update_pipeline(orguser.org, deployment_id, payload)
        return result
    except PipelineNotFoundError:
        raise HttpError(404, "pipeline not found")
    except PipelineValidationError as error:
        raise HttpError(400, error.message) from error
    except PipelineConfigurationError as error:
        raise HttpError(422, error.message) from error
    except PipelineServiceError as error:
        raise HttpError(500, error.message) from error
    except Exception as error:
        logger.exception(f"Unexpected error in update_pipeline: {error}")
        raise HttpError(500, f"An unexpected error occurred: {str(error)}") from error


@pipeline_router.post("flows/{deployment_id}/set_schedule/{status}")
@has_permission(["can_edit_pipeline"])
def post_deployment_set_schedule(request, deployment_id, status):
    """Set deployment schedule to active / inactive"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    try:
        result = PipelineService.set_pipeline_schedule(orguser.org, deployment_id, status)
        return result
    except PipelineValidationError as error:
        raise HttpError(422, error.message) from error
    except PipelineServiceError as error:
        raise HttpError(500, error.message) from error
    except Exception as error:
        logger.exception(f"Unexpected error in set_pipeline_schedule: {error}")
        raise HttpError(500, f"An unexpected error occurred: {str(error)}") from error


################################## runs and logs related ######################################


@pipeline_router.post("v1/flows/{deployment_id}/flow_run/")
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

    try:
        result = PipelineService.run_pipeline(orguser.org, orguser, deployment_id, payload)
        return result
    except PipelineNotFoundError:
        raise HttpError(404, "pipeline not found")
    except PipelineConfigurationError as error:
        raise HttpError(422, error.message) from error
    except PipelineServiceError as error:
        raise HttpError(500, error.message) from error
    except Exception as error:
        logger.exception(f"Unexpected error in run_pipeline: {error}")
        raise HttpError(500, f"An unexpected error occurred: {str(error)}") from error


@pipeline_router.post("/v1/flows/{deployment_id}/clear_streams/")
@has_permission(["can_run_pipeline"])
def clear_selected_streams_api(request, deployment_id: str, payload: ClearSelectedStreams):
    """Clear selected streams in Airbyte for a given deployment."""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "Register an organization first")

    try:
        result = PipelineService.post_clear_selected_streams_run(
            orguser.org, orguser, deployment_id, payload
        )
        return result
    except PipelineNotFoundError:
        raise HttpError(404, "Deployment not found")
    except PipelineValidationError as error:
        raise HttpError(400, error.message) from error
    except PipelineConfigurationError as error:
        raise HttpError(422, error.message) from error
    except PipelineServiceError as error:
        raise HttpError(500, error.message) from error
    except Exception as error:
        logger.exception(f"Unexpected error in clear_selected_streams: {error}")
        raise HttpError(500, f"An unexpected error occurred: {str(error)}") from error


@pipeline_router.get("flow_runs/{flow_run_id}/logs")
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


@pipeline_router.get(
    "flow_runs/{flow_run_id}",
    response=PrefectFlowRunSchema,
)
@has_permission(["can_view_pipeline"])
def get_flow_run_by_id(request, flow_run_id):
    # pylint: disable=unused-argument
    """fetch a flow run from prefect"""
    try:
        flow_run = prefect_service.get_flow_run_poll(flow_run_id)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to retrieve logs") from error
    return flow_run


@pipeline_router.get("flows/{deployment_id}/flow_runs/history")
@has_permission(["can_view_pipeline"])
def get_prefect_flow_runs_log_history(request, deployment_id, limit: int = 0, fetchlogs=True):
    # pylint: disable=unused-argument
    """Fetch all flow runs for the deployment and the logs for each flow run"""
    flow_runs = prefect_service.get_flow_runs_by_deployment_id(deployment_id, limit=limit)

    if fetchlogs:
        for flow_run in flow_runs:
            flow_run["logs"] = prefect_service.recurse_flow_run_logs(flow_run["id"], None)

    return flow_runs


@pipeline_router.get("v1/flows/{deployment_id}/flow_runs/history")
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


@pipeline_router.get("v1/flow_runs/{flow_run_id}/logsummary")
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


@pipeline_router.post("flow_runs/{flow_run_id}/set_state")
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

        cancellation_params = {"state": payload.state.dict(), "force": str(payload.force)}
        res = prefect_service.cancel_queued_manual_job(flow_run_id, cancellation_params)
    except HttpError as http_error:
        # We handle HttpError separately to ensure the correct message is raised
        logger.exception(http_error)
        raise http_error
    except Exception as error:
        # For other exceptions,
        logger.exception(error)
        raise HttpError(400, "failed to cancel the queued manual job") from error
    return res
