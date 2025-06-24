import os
import urllib.parse
import requests
import json
import urllib

from ninja import Router, Schema
from ninja.errors import HttpError

from ddpui.utils.custom_logger import CustomLogger
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgWarehouse
from ddpui.utils import secretsmanager
from ddpui.auth import has_permission

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
def get_dashboards(request):
    """Endpoint to list all dashboards from Superset using org admin creds"""
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

    # List dashboards
    try:
        query_dict = {"page": 0, "page_size": 10}
        formatted_json = json.dumps(query_dict)
        query_params = urllib.parse.quote(formatted_json)

        url = f"{orguser.org.viz_url}api/v1/dashboard/?q={query_params}"

        response = requests.get(
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )

        logger.info("sending request to superset with URL: %s", url)
        response.raise_for_status()
        dashboards = response.json().get("result", [])
    except requests.exceptions.RequestException as err:
        logger.error("Error fetching dashboards from superset: %s", str(err))
        raise HttpError(500, "couldn't connect to superset")

    return dashboards


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
    """Endpoint to get details of a specific dashboard from Superset using org admin creds"""
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

    # Get single dashboard details
    try:
        url = f"{orguser.org.viz_url}api/v1/dashboard/{dashboard_id}"

        response = requests.get(
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )

        logger.info("Fetching dashboard details with URL: %s", url)
        response.raise_for_status()
        dashboard = response.json().get("result", {})

    except requests.exceptions.RequestException as err:
        logger.error("Error fetching dashboard details from superset: %s", str(err))
        raise HttpError(500, "couldn't fetch dashboard details from superset")

    return dashboard
