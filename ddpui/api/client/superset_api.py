import os
import json
from ninja import NinjaAPI
import requests

from ninja.errors import HttpError
from ddpui.utils.custom_logger import CustomLogger
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgWarehouse
from ddpui import auth
from ddpui.utils import secretsmanager
from ninja.errors import HttpError, ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError


supersetapi = NinjaAPI(urls_namespace="superset")

logger = CustomLogger("ddpui")


@supersetapi.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """
    Handle any ninja validation errors raised in the apis
    These are raised during request payload validation
    exc.errors is correct
    """
    return Response({"detail": exc.errors}, status=422)


@supersetapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """
    Handle any pydantic errors raised in the apis
    These are raised during response payload validation
    exc.errors() is correct
    """
    return Response({"detail": exc.errors()}, status=500)


@supersetapi.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception
):  # pylint: disable=unused-argument
    """Handle any other exception raised in the apis"""
    logger.exception(exc)
    return Response({"detail": "something went wrong"}, status=500)


@supersetapi.post(
    "embed_token/{dashboard_uuid}/",
    auth=auth.AnyOrgUser(),
)
def post_fetch_embed_token(request, dashboard_uuid):  # pylint: disable=unused-argument
    """endpoint to fetch the embed token of a dashboard from superset"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if warehouse is None:
        raise HttpError(400, "create a warehouse first")

    # {username: "", password: "", first_name: "", last_name: ""}
    credentials = secretsmanager.retrieve_superset_usage_dashboard_credentials(
        warehouse
    )

    access_token = None
    csrf_token = None
    embed_token = None
    # Hit the superset endpoint /api/v1/security/login
    try:
        response = requests.post(
            f"{os.getenv('SUPERSET_USAGE_DASHBOARD_API_URL')}/security/login",
            json={
                "password": credentials["password"],
                "username": credentials["username"],
                "refresh": True,
                "provider": "db",
            },
        )
        response.raise_for_status()
        access_token = response.json()["access_token"]

    except requests.exceptions.RequestException as err:
        logger.error(
            "Something went wrong when trying to fetch jwt token from superset usage dashboard domain : %s",
            str(err),
        )
        raise HttpError(500, "couldn't connect to superset")

    # Hit the superset endpoint /api/v1/security/csrf_token
    try:
        response = requests.get(
            f"{os.getenv('SUPERSET_USAGE_DASHBOARD_API_URL')}/security/csrf_token",
            headers={"Authorization": f"Bearer {access_token}"},  # skipcq: PTC-W1006
        )
        response.raise_for_status()
        csrf_token = response.json()["result"]

    except requests.exceptions.RequestException as err:
        logger.error(
            "Something went wrong trying to fetch the csrf token from superset usage dashboard domain : %s",
            str(err),
        )
        raise HttpError(500, "couldn't connect to superset")

    # Hit the superset endpoint /api/v1/security/guest_token
    try:
        response = requests.post(
            f"{os.getenv('SUPERSET_USAGE_DASHBOARD_API_URL')}/security/guest_token",
            json={
                "user": {
                    "username": credentials["username"],
                    "first_name": credentials["first_name"],
                    "last_name": credentials["last_name"],
                },
                "resources": [{"type": "dashboard", "id": dashboard_uuid}],
                "rls": [{}],  # TODO: add the correct RLS here
            },
            headers={
                "Authorization": f"Bearer {access_token}",
                "X-CSRFToken": csrf_token,
            },  # skipcq: PTC-W1006
        )
        response.raise_for_status()
        embed_token = response.json()["token"]

    except requests.exceptions.RequestException as err:
        logger.error(
            "Something went wrong trying to fetch the csrf token from superset usage dashboard domain : %s",
            str(err),
        )
        raise HttpError(500, "couldn't connect to superset")

    return {"embed_token": embed_token}
