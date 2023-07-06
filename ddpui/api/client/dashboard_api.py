from ninja import NinjaAPI
from ninja.errors import HttpError, ValidationError

from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError

# dependencies
from ddpui.ddpprefect import prefect_service
from ddpui import auth

# models
from ddpui.models.org import OrgDataFlow


dashboardapi = NinjaAPI(urls_namespace="dashboard")

@dashboardapi.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """Handle any ninja validation errors raised in the apis"""
    return Response({"error": exc.errors}, status=422)


@dashboardapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """Handle any pydantic errors raised in the apis"""
    return Response({"error": exc.errors()}, status=422)


@dashboardapi.exception_handler(HttpError)
def ninja_http_error_handler(
    request, exc: HttpError
):  # pylint: disable=unused-argument
    """
    Handle any http errors raised in the apis
    TODO: should we put request.orguser.org.slug into the error message here
    """
    return Response({"error": " ".join(exc.args)}, status=exc.status_code)


@dashboardapi.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception
):  # pylint: disable=unused-argument
    """Handle any other exception raised in the apis"""
    return Response({"error": " ".join(exc.args)}, status=500)


@dashboardapi.get("/", auth=auth.CanManagePipelines())
def get_dashboard(request):
    """Fetch all flows/pipelines created in an organization"""
    orguser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    org_data_flows = (
        OrgDataFlow.objects.filter(org=orguser.org).exclude(cron=None).all()
    )

    res = []

    # fetch 50 (default limit) flow runs for each flow
    for flow in org_data_flows:
        res.append(
            {
                "name": flow.name,
                "deploymentId": flow.deployment_id,
                "cron": flow.cron,
                "deploymentName": flow.deployment_name,
                "runs": prefect_service.get_flow_runs_by_deployment_id(
                    flow.deployment_id, 50
                ),
            }
        )

    # we might add more stuff here , system logs etc.

    return res
