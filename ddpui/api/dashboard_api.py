from ninja import NinjaAPI
from ninja.errors import ValidationError

from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError

# dependencies
from ddpui.ddpprefect import prefect_service
from ddpui import auth

# models
from ddpui.models.org import OrgDataFlow, OrgDataFlowv1
from ddpui.models.orgjobs import BlockLock, DataflowBlock
from ddpui.models.tasks import DataflowOrgTask, TaskLock
from ddpui.auth import has_permission


dashboardapi = NinjaAPI(urls_namespace="dashboard")


@dashboardapi.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """
    Handle any ninja validation errors raised in the apis
    These are raised during request payload validation
    exc.errors is correct
    """
    return Response({"detail": exc.errors}, status=422)


@dashboardapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """
    Handle any pydantic errors raised in the apis
    These are raised during response payload validation
    exc.errors() is correct
    """
    return Response({"detail": exc.errors()}, status=500)


@dashboardapi.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception  # skipcq PYL-W0613
):  # pylint: disable=unused-argument
    """Handle any other exception raised in the apis"""
    return Response({"detail": "something went wrong"}, status=500)


@dashboardapi.get("/", auth=auth.CanManagePipelines())
@has_permission(["can_view_dashboard"])
def get_dashboard(request):
    """Fetch all flows/pipelines created in an organization"""
    orguser = request.orguser

    org_data_flows = OrgDataFlow.objects.filter(
        org=orguser.org, dataflow_type="orchestrate"
    ).all()

    res = []

    # fetch 50 (default limit) flow runs for each flow
    for flow in org_data_flows:
        block_ids = DataflowBlock.objects.filter(dataflow=flow).values("opb__block_id")
        # if there is one there will typically be several - a sync,
        # a git-run, a git-test... we return the userinfo only for the first one
        lock = BlockLock.objects.filter(
            opb__block_id__in=[x["opb__block_id"] for x in block_ids]
        ).first()
        res.append(
            {
                "name": flow.name,
                "deploymentId": flow.deployment_id,
                "cron": flow.cron,
                "deploymentName": flow.deployment_name,
                "runs": prefect_service.get_flow_runs_by_deployment_id(
                    flow.deployment_id, 50
                ),
                "lock": (
                    {
                        "lockedBy": lock.locked_by.user.email,
                        "lockedAt": lock.locked_at,
                    }
                    if lock
                    else None
                ),
            }
        )

    # we might add more stuff here , system logs etc.

    return res


@dashboardapi.get("/v1", auth=auth.CanManagePipelines())
@has_permission(["can_view_dashboard"])
def get_dashboard_v1(request):
    """Fetch all flows/pipelines created in an organization"""
    orguser = request.orguser

    org_data_flows = OrgDataFlowv1.objects.filter(
        org=orguser.org, dataflow_type="orchestrate"
    ).all()

    res = []

    # fetch 50 (default limit) flow runs for each flow
    for flow in org_data_flows:
        orgtask_ids = DataflowOrgTask.objects.filter(dataflow=flow).values_list(
            "orgtask__id", flat=True
        )
        # if there is one there will typically be several - a sync,
        # a git-run, a git-test... we return the userinfo only for the first one
        lock = TaskLock.objects.filter(orgtask__id__in=orgtask_ids).first()
        res.append(
            {
                "name": flow.name,
                "deploymentId": flow.deployment_id,
                "cron": flow.cron,
                "deploymentName": flow.deployment_name,
                "runs": prefect_service.get_flow_runs_by_deployment_id(
                    flow.deployment_id, 50
                ),
                "lock": (
                    {
                        "lockedBy": lock.locked_by.user.email,
                        "lockedAt": lock.locked_at,
                    }
                    if lock
                    else None
                ),
            }
        )

    # we might add more stuff here , system logs etc.

    return res
