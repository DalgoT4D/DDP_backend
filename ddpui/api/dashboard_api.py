from ninja import NinjaAPI
from ninja.errors import ValidationError

from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError
from django.forms import model_to_dict
from django.db.models import Prefetch

# dependencies
from ddpui import auth

# models
from ddpui.models.org import OrgDataFlowv1
from ddpui.models.tasks import DataflowOrgTask, TaskLock
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.auth import has_permission
from ddpui.ddpprefect import prefect_service

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


@dashboardapi.get("/v1", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_dashboard"])
def get_dashboard_v1(request):
    """Fetch all flows/pipelines created in an organization"""
    orguser = request.orguser

    org_data_flows = OrgDataFlowv1.objects.filter(
        org=orguser.org, dataflow_type="orchestrate"
    ).prefetch_related(
        Prefetch(
            "datafloworgtasks",
            queryset=DataflowOrgTask.objects.all()
            .select_related("orgtask")
            .prefetch_related(
                Prefetch("orgtask__tasklock", queryset=TaskLock.objects.all()),
            ),
        )
    )

    res = []

    # fetch 50 (default limit) flow runs for each flow
    for flow in org_data_flows:
        # if there is one there will typically be several - a sync,
        # a git-run, a git-test... we return the userinfo only for the first one
        lock = None
        for dataflow_orgtask in flow.datafloworgtasks.all():
            orgtask = dataflow_orgtask.orgtask
            if hasattr(orgtask, "tasklock"):
                lock = orgtask.tasklock
                break

        res.append(
            {
                "name": flow.name,
                "deploymentId": flow.deployment_id,
                "cron": flow.cron,
                "deploymentName": flow.deployment_name,
                "runs": prefect_service.get_flow_runs_by_deployment_id_v1(
                    flow.deployment_id, limit=50, offset=0
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
