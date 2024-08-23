from ninja import NinjaAPI
from ninja.errors import ValidationError

from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError
from django.forms import model_to_dict
from django.db.models import Prefetch

# dependencies
from ddpui import auth

# models

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


@dashboardapi.get("/v1", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_dashboard"])
def get_dashboard_v1(request):
    """Fetch all flows/pipelines created in an organization"""
    
    # Removed: Data fetching and processing code
    # Placeholder response after removing detailed logic
    return {
        "message": "Dashboard data fetching functionality has been removed."
    }
