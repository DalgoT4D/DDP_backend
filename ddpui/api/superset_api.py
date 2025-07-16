import os
import urllib.parse
import requests
import json
import urllib
from typing import Optional

from ninja import Router, Schema
from ninja.errors import HttpError
from django.http import HttpResponse, FileResponse

from ddpui.utils.custom_logger import CustomLogger
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgWarehouse
from ddpui.utils import secretsmanager
from ddpui.auth import has_permission
from ddpui.services.superset_service import SupersetService

superset_router = Router()
logger = CustomLogger("ddpui")


class SupersetDalgoUserCreds(Schema):
    username: str
    password: str


@superset_router.post("embed_token/{dashboard_uuid}/")
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


@superset_router.post("dalgo_user_creds/")
def post_superset_admin_creds(request, payload: SupersetDalgoUserCreds):
    """Save Superset admin credentials for the org in secrets manager"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    # Save to secrets manager
    secret_id = secretsmanager.save_dalgo_user_superset_credentials(payload.dict())

    orguser.org.dalgouser_superset_creds_key = secret_id
    orguser.org.save()

    return {"success": True}


@superset_router.get("dashboards/")
def get_dashboards(
    request,
    page: int = 0,
    page_size: int = 20,
    search: Optional[str] = None,
    status: Optional[str] = None,
):
    """List dashboards with enhanced features including search, filter, and thumbnails"""
    orguser: OrgUser = request.orguser

    if not orguser.org or not orguser.org.viz_url:
        return {"result": [], "count": 0}

    # SupersetService will raise HttpError if something goes wrong
    service = SupersetService(orguser.org)
    data = service.get_dashboards(page, page_size, search, status)

    # Enhance with thumbnail URLs
    for dashboard in data.get("result", []):
        dashboard["thumbnail_url"] = f"/api/superset/dashboards/{dashboard['id']}/thumbnail/"

    return data


@superset_router.get("dashboards/{dashboard_id}/embed_info/")
def get_single_dashboard_embed_info(request, dashboard_id: str):
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    if orguser.org.viz_url is None:
        raise HttpError(
            400,
            "your superset subscription is not active, please contact the Dalgo team",
        )

    dalgo_user_secret_key = orguser.org.dalgouser_superset_creds_key
    credentials = secretsmanager.retrieve_dalgo_user_superset_credentials(dalgo_user_secret_key)
    if credentials is None:
        raise HttpError(400, "superset admin credentials are missing for this org")

    credentials: SupersetDalgoUserCreds = SupersetDalgoUserCreds(**credentials)

    # Authenticate and get access token
    try:
        response = requests.post(
            f"{orguser.org.viz_url}api/v1/security/login",
            json={
                "password": credentials.password,
                "username": credentials.username,
                "refresh": True,
                "provider": "db",
            },
            timeout=10,
        )
        response.raise_for_status()
        access_token = response.json()["access_token"]
    except requests.exceptions.RequestException as err:
        logger.error("Error fetching jwt token from superset: %s", str(err))
        raise HttpError(500, "couldn't connect to superset")

    # Hit the superset endpoint /api/v1/security/csrf_token
    cookies = None
    try:
        response = requests.get(
            f"{orguser.org.viz_url}api/v1/security/csrf_token",
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

    embed_info = None

    try:
        url = f"{orguser.org.viz_url}api/v1/dashboard/{dashboard_id}/embedded"

        response = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "X-CSRFToken": csrf_token,
                "Referer": f"{orguser.org.viz_url.rstrip('/')}",
            },
            timeout=30,
            cookies={"session": cookies.split("=")[1]},
            json={"allowed_domains": []},
        )

        response.raise_for_status()

        embed_info = response.json().get("result", {})
    except requests.exceptions.RequestException as err:
        logger.error(
            "Error creating embed info for dashboard %s from superset: %s",
            err,
            dashboard_id,
        )

    embed_info["host"] = orguser.org.viz_url.rstrip("/")

    # fetch the guest token with rls
    try:
        response = requests.post(
            f"{orguser.org.viz_url}api/v1/security/guest_token",
            json={
                "user": {
                    "username": credentials.username,
                },
                "resources": [{"type": "dashboard", "id": embed_info.get("uuid")}],
                "rls": [],
            },
            headers={
                "Authorization": f"Bearer {access_token}",
                "X-CSRFToken": csrf_token,
                "Content-Type": "application/json",
                "Referer": f"{orguser.org.viz_url.rstrip('/')}",
            },  # skipcq: PTC-W1006
            cookies={"session": cookies.split("=")[1]},
            timeout=10,
        )
        response.raise_for_status()
        embed_info["guest_token"] = response.json()["token"]
    except requests.exceptions.RequestException as err:
        logger.error(err)
        logger.error(
            "Something went wrong trying to fetch the guest/embed token from superset usage dashboard domain: %s",
            str(err),
        )
        # pylint:disable=raise-missing-from
        raise HttpError(500, "couldn't connect to superset")

    return embed_info


@superset_router.get("dashboards/{dashboard_id}/")
def get_dashboard_by_id(request, dashboard_id: str):
    """Get single dashboard details"""
    orguser: OrgUser = request.orguser

    if not orguser.org or not orguser.org.viz_url:
        raise HttpError(400, "Superset not configured for this organization")

    # SupersetService will raise HttpError if something goes wrong
    service = SupersetService(orguser.org)
    return service.get_dashboard_by_id(dashboard_id)


@superset_router.post("dashboards/{dashboard_id}/guest_token/")
def post_dashboard_guest_token(request, dashboard_id: str):
    """Generate guest token for dashboard embedding"""
    orguser: OrgUser = request.orguser

    if not orguser.org or not orguser.org.viz_url:
        raise HttpError(400, "Superset not configured for this organization")

    # SupersetService will raise HttpError if something goes wrong
    service = SupersetService(orguser.org)

    # Get or create embedded UUID for this dashboard
    embedded_uuid = service.get_or_create_embedded_uuid(dashboard_id)

    if not embedded_uuid:
        raise HttpError(400, "Failed to get embedded UUID for dashboard")

    # Generate guest token using the embedded UUID
    guest_token_data = service.get_guest_token(embedded_uuid)

    return {
        "guest_token": guest_token_data["token"],
        "expires_in": 300,  # 5 minutes
        "dashboard_uuid": embedded_uuid,
        "superset_domain": orguser.org.viz_url.rstrip("/"),
    }


@superset_router.get("dashboards/{dashboard_id}/thumbnail/")
def get_dashboard_thumbnail(request, dashboard_id: str):
    """Get dashboard thumbnail with caching"""
    orguser: OrgUser = request.orguser

    if not orguser.org or not orguser.org.viz_url:
        raise HttpError(400, "Superset not configured for this organization")

    service = SupersetService(orguser.org)
    thumbnail = service.get_dashboard_thumbnail(dashboard_id)

    if not thumbnail:
        # Return placeholder image
        # First check if placeholder exists, if not return 404
        placeholder_path = os.path.join(
            os.path.dirname(__file__), "..", "assets", "dashboard-placeholder.svg"
        )
        if os.path.exists(placeholder_path):
            return FileResponse(placeholder_path, content_type="image/svg+xml")
        else:
            raise HttpError(404, "Dashboard thumbnail not found")

    return HttpResponse(thumbnail, content_type="image/png")
