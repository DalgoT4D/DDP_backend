from ninja import NinjaAPI
from ninja.errors import ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError

from ddpui.api.airbyte_api import airbyte_router
from ddpui.api.dashboard_api import dashboard_router
from ddpui.api.data_api import data_router
from ddpui.api.dbt_api import dbt_router
from ddpui.api.notifications_api import notification_router
from ddpui.api.orgtask_api import orgtask_router
from ddpui.api.pipeline_api import pipeline_router
from ddpui.api.superset_api import superset_router
from ddpui.api.task_api import task_router
from ddpui.api.transform_api import transform_router
from ddpui.api.user_org_api import user_org_router
from ddpui.api.user_preferences_api import userpreference_router
from ddpui.api.org_preferences_api import orgpreference_router
from ddpui.api.warehouse_api import warehouse_router
from ddpui.api.webhook_api import webhook_router


src_api = NinjaAPI(
    urls_namespace="api",
    title="Dalgo backend apis",
    description="Open source ELT orchestrator",
    docs_url="/api/docs",
)


@src_api.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """
    Handle any ninja validation errors raised in the apis
    These are raised during request payload validation
    exc.errors is correct
    """
    return Response({"detail": exc.errors}, status=422)


@src_api.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """
    Handle any pydantic errors raised in the apis
    These are raised during response payload validation
    exc.errors() is correct
    """
    return Response({"detail": exc.errors()}, status=500)


@src_api.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception
):  # pylint: disable=unused-argument # skipcq PYL-W0613
    """Handle any other exception raised in the apis"""
    print(exc)
    return Response({"detail": "something went wrong"}, status=500)


# tag routes to specify sections in docs
airbyte_router.tags = ["Airbyte"]
dashboard_router.tags = ["Dashboard"]
data_router.tags = ["Data"]
dbt_router.tags = ["Dbt"]
notification_router.tags = ["Notifications"]
orgtask_router.tags = ["OrgTask"]
pipeline_router.tags = ["Pipeline"]
superset_router.tags = ["Superset"]
task_router.tags = ["Task"]
transform_router.tags = ["Transform"]
user_org_router.tags = ["UserOrg"]
userpreference_router.tags = ["UserPreference"]
orgpreference_router.tags =["OrgPreference"]
warehouse_router.tags = ["Warehouse"]
webhook_router.tags = ["Webhook"]

# mount all the module routes
src_api.add_router("/api/airbyte/", airbyte_router)
src_api.add_router("/api/dashboard/", dashboard_router)
src_api.add_router("/api/data/", data_router)
src_api.add_router("/api/dbt/", dbt_router)
src_api.add_router("/api/notifications/", notification_router)
src_api.add_router("/api/prefect/tasks/", orgtask_router)
src_api.add_router("/api/prefect/", pipeline_router)
src_api.add_router("/api/superset/", superset_router)
src_api.add_router("/api/tasks/", task_router)
src_api.add_router("/api/transform/", transform_router)
src_api.add_router("/api/userpreferences/", userpreference_router)
src_api.add_router("/api/warehouse/", warehouse_router)
src_api.add_router("/api/", user_org_router)
src_api.add_router("/webhooks/", webhook_router)
src_api.add_router("/api/orgpreferences/",orgpreference_router )