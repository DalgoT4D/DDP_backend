import os
import requests

from ninja import NinjaAPI, Router
from ninja.errors import HttpError, ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError

from ddpui.utils.custom_logger import CustomLogger
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgWarehouse
from ddpui import auth
from ddpui.utils import secretsmanager
from ddpui.auth import has_permission

superset_router = Router()
logger = CustomLogger("ddpui")


@superset_router.post(
    "embed_token/{dashboard_uuid}/",
    auth=auth.CustomAuthMiddleware(),
)
@has_permission(["can_view_usage_dashboard"])
def post_fetch_embed_token(request, dashboard_uuid):  # pylint: disable=unused-argument
    """endpoint to fetch the embed token of a dashboard from superset"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if warehouse is None:
        raise HttpError(400, "create a warehouse first")

    if orguser.org.viz_url is None:
        raise HttpError(
            400,
            "your superset subscription is not active, please contact the Dalgo team",
        )

    superset_creds = os.getenv("SUPERSET_USAGE_CREDS_SECRET_ID")
    if superset_creds is None:
        raise HttpError(
            400,
            "superset usage credentials are missing",
        )

    # {username: "", password: "", first_name: "", last_name: ""} # skipcq: PY-W0069
    credentials = secretsmanager.retrieve_superset_usage_dashboard_credentials(superset_creds)
    if credentials is None:
        raise HttpError(400, "superset usage credentials are missing")

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
            timeout=10,
        )
        response.raise_for_status()
        access_token = response.json()["access_token"]

    except requests.exceptions.RequestException as err:
        logger.error(
            "Something went wrong when trying to fetch jwt token from superset usage dashboard domain : %s",
            str(err),
        )
        # pylint:disable=raise-missing-from
        raise HttpError(500, "couldn't connect to superset")

    # Hit the superset endpoint /api/v1/security/csrf_token
    cookies = None
    try:
        response = requests.get(
            f"{os.getenv('SUPERSET_USAGE_DASHBOARD_API_URL')}/security/csrf_token",
            headers={"Authorization": f"Bearer {access_token}"},  # skipcq: PTC-W1006
            timeout=10,
        )
        if "Set-Cookie" in response.headers:
            cookies = response.headers["Set-Cookie"]
        response.raise_for_status()
        csrf_token = response.json()["result"]

    except requests.exceptions.RequestException as err:
        logger.error(
            "Something went wrong trying to fetch the csrf token from superset usage dashboard domain : %s",
            str(err),
        )
        # pylint:disable=raise-missing-from
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
                "rls": [{"clause": f"org='{orguser.org.slug.replace('-', '_')}'"}],
            },
            headers={
                "Authorization": f"Bearer {access_token}",
                "X-CSRFToken": csrf_token,
                "Content-Type": "application/json",
                "Referer": f"{os.getenv('SUPERSET_USAGE_DASHBOARD_API_URL')}",
            },  # skipcq: PTC-W1006
            cookies={"session": cookies.split("=")[1]},
            timeout=10,
        )
        response.raise_for_status()
        embed_token = response.json()["token"]

    except requests.exceptions.RequestException as err:
        logger.error(
            "Something went wrong trying to fetch the guest/embed token from superset usage dashboard domain : %s",
            str(err),
        )
        # pylint:disable=raise-missing-from
        raise HttpError(500, "couldn't connect to superset")

    return {"embed_token": embed_token}
