import os
from ninja import Router
from ninja.errors import HttpError
from ddpui import auth
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_supersets import OrgSupersets
from ddpui.models.org_plans import OrgPlans
from django.utils import timezone
from ddpui.schemas.org_preferences_schema import (
    CreateOrgPreferencesSchema,
    UpdateOrgPreferencesSchema,
    UpdateLLMOptinSchema,
    UpdateDiscordNotificationsSchema,
    CreateOrgSupersetDetailsSchema,
)
from ddpui.auth import has_permission
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpprefect import (
    prefect_service,
)
from ddpui.core.orgdbt_manager import DbtProjectManager
import requests
import subprocess

orgpreference_router = Router()


@orgpreference_router.post("/", auth=auth.CustomAuthMiddleware())
def create_org_preferences(request, payload: CreateOrgPreferencesSchema):
    orguser: OrgUser = request.orguser
    org = orguser.org
    payload.org = org
    """Creates preferences for an organization"""
    if OrgPreferences.objects.filter(org=org).exists():
        raise HttpError(400, "Organization preferences already exist")

    payload_data = payload.dict(exclude={"org"})

    org_preferences = OrgPreferences.objects.create(
        org=org, **payload_data  # Use the rest of the payload
    )

    preferences = {
        "llm_optin": org_preferences.llm_optin,
        "llm_optin_approved_by": org_preferences.llm_optin_approved_by,
        "llm_optin_date": org_preferences.llm_optin_date,
        "enable_discord_notifications": org_preferences.enable_discord_notifications,
        "discord_webhook": org_preferences.discord_webhook,
    }

    return {"success": True, "res": preferences}


@orgpreference_router.put("/llm_approval", auth=auth.CustomAuthMiddleware())
@has_permission(["can_edit_llm_settings"])
def update_org_preferences(request, payload: UpdateLLMOptinSchema):
    """Updates llm preferences for the logged-in user's organization"""

    orguser: OrgUser = request.orguser
    org = orguser.org

    org_preferences = OrgPreferences.objects.filter(org=org).first()
    if org_preferences is None:
        raise HttpError(400, "Preferences for this organization not found")

    if payload.llm_optin is True:
        llm_optin_approved_by = request.orguser
        llm_optin_date = timezone.now()
    else:
        llm_optin_approved_by = None
        llm_optin_date = None

    org_preferences.llm_optin = payload.llm_optin
    org_preferences.llm_optin_approved_by = llm_optin_approved_by
    org_preferences.llm_optin_date = llm_optin_date
    org_preferences.save()

    return {"success": True, "res": 1}


@orgpreference_router.put("/enable-discord-notifications", auth=auth.CustomAuthMiddleware())
@has_permission(["can_edit_discord_notifications_settings"])
def update_discord_notifications(request, payload: UpdateDiscordNotificationsSchema):
    """Updates Discord notifications preferences for the logged-in user's organization."""

    orguser: OrgUser = request.orguser
    org = orguser.org

    org_preferences = OrgPreferences.objects.filter(org=org).first()
    if org_preferences is None:
        raise HttpError(400, "Preferences for this organization not found")

    if payload.enable_discord_notifications:
        if not org_preferences.discord_webhook and not payload.discord_webhook:
            raise HttpError(400, "Discord webhook is required to enable notifications.")
        discord_webhook = payload.discord_webhook or org_preferences.discord_webhook
    else:
        discord_webhook = None

    org_preferences.enable_discord_notifications = payload.enable_discord_notifications
    org_preferences.discord_webhook = discord_webhook
    org_preferences.save()

    response_data = {
        "enable_discord_notifications": org_preferences.enable_discord_notifications,
        "discord_webhook": org_preferences.discord_webhook,
    }

    return {"success": True, "res": response_data}


@orgpreference_router.get("/", auth=auth.CustomAuthMiddleware())
def get_org_preferences(request):
    """Gets preferences for an organization based on the logged-in user's organization"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    try:
        org_preferences = OrgPreferences.objects.select_related("llm_optin_approved_by").get(
            org=org
        )
    except OrgPreferences.DoesNotExist:
        raise HttpError(404, "Organization preferences not found")

    approved_by_data = None
    if org_preferences.llm_optin_approved_by and org_preferences.llm_optin_approved_by.user:
        user = org_preferences.llm_optin_approved_by.user
        approved_by_data = {"user_id": user.pk, "email": user.email}

    preferences = {
        "org": {
            "name": org.name,
            "slug": org.slug,
            "type": org.type,
        },
        "llm_optin": org_preferences.llm_optin,
        "llm_optin_approved_by": approved_by_data,
        "llm_optin_date": org_preferences.llm_optin_date,
        "enable_discord_notifications": org_preferences.enable_discord_notifications,
        "discord_webhook": org_preferences.discord_webhook,
    }

    return {"success": True, "res": preferences}


@orgpreference_router.get("/org-superset", auth=auth.CustomAuthMiddleware())
def get_org_superset_details(request):
    """Gets preferences for an organization by org_id"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_superset = OrgSupersets.objects.filter(org=org).first()
    if org_superset is None:
        raise HttpError(400, "Organizations superset details not found")

    response_data = {
        "org": {
            "name": org.name,
            "slug": org.slug,
            "type": org.type,
        },
        "superset_version": org_superset.superset_version,
    }
    return {"success": True, "res": response_data}


@orgpreference_router.post("/org-superset", auth=auth.CustomAuthMiddleware())
def create_org_superset_details(request, payload: CreateOrgSupersetDetailsSchema):
    """Creates or updates an entry in db with org superset details."""
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_superset, created = OrgSupersets.objects.update_or_create(org=org, defaults=payload.dict())

    response_data = {
        "org": {
            "name": org.name,
            "slug": org.slug,
            "type": org.type,
        },
        "superset_version": org_superset.superset_version,
        "created": created,
    }

    return {"success": True, "res": response_data}


@orgpreference_router.get("/toolinfo", auth=auth.CustomAuthMiddleware())
def get_tools_versions(request):
    """get versions of the tools used in the system"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    org_superset = OrgSupersets.objects.filter(org=org).first()
    if org_superset is None:
        raise HttpError(400, "Supserset not found.")

    versions = []

    # Airbyte Version
    versions.append({"Airbyte": {"version": "0.58"}})
    # this api will eventually work at some Airbyte version... but not at 0.58
    # try:
    #     abhost = os.getenv("AIRBYTE_SERVER_HOST")
    #     abport = os.getenv("AIRBYTE_SERVER_PORT")
    #     abver = os.getenv("AIRBYTE_SERVER_APIVER")
    #     airbyte_url = f"http://{abhost}:{abport}/api/{abver}/instance_configuration"
    #     airbyte_response = requests.get(airbyte_url, timeout=5)
    #     if airbyte_response.status_code == 200:
    #         airbyte_data = airbyte_response.json()
    #         versions.append({"Airbyte": {"version": airbyte_data.get("version")}})
    #     else:
    #         versions.append({"Airbyte": {"version": "Not available"}})
    # except Exception:
    #     versions.append({"Airbyte": {"version": "Not available"}})

    # Prefect Version
    try:
        prefect_host = os.getenv("PREFECT_SERVER_HOST")
        prefect_port = os.getenv("PREFECT_SERVER_PORT")
        prefect_url = f"http://{prefect_host}:{prefect_port}/api/admin/version"
        prefect_response = requests.get(prefect_url, timeout=5)
        if prefect_response.status_code == 200:
            version = prefect_response.text.strip().strip('"')
            versions.append({"Prefect": {"version": version}})
        else:
            versions.append({"Prefect": {"version": "Not available"}})
    except Exception:
        versions.append({"Prefect": {"version": "Not available"}})

    dbt_venv = os.getenv("DBT_VENV")
    # dbt Version
    try:
        dbt_version_command = [os.path.join(dbt_venv, "venv", "bin", "dbt"), "--version"]
        dbt_output = subprocess.check_output(dbt_version_command, text=True)
        for line in dbt_output.splitlines():
            if "installed:" in line:
                versions.append({"DBT": {"version": line.split(":")[1].strip()}})
                break
        else:
            versions.append({"DBT": {"version": "Not available"}})
    except Exception:
        versions.append({"DBT": {"version": "Not available"}})

    # Elementary Version
    try:
        elementary_version_command = [os.path.join(dbt_venv, "venv", "bin", "edr"), "--version"]
        elementary_output = subprocess.check_output(elementary_version_command, text=True)
        for line in elementary_output.splitlines():
            if "Elementary version" in line:
                versions.append({"Elementary": {"version": line.split()[-1].strip()}})
                break
        else:
            versions.append({"Elementary": {"version": "Not available"}})
    except Exception:
        versions.append({"Elementary": {"version": "Not available"}})

    # Superset Version
    versions.append({"Superset": {"version": org_superset.superset_version}})

    return {"success": True, "res": versions}


@orgpreference_router.get("/org-plan", auth=auth.CustomAuthMiddleware())
def get_org_plans(request):
    """Gets preferences for an organization based on the logged-in user's organization"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_plan = OrgPlans.objects.filter(org=org).first()
    if org_plan is None:
        raise HttpError(400, "Org's Plan not found")

    response_data = {
        "org": {
            "name": org.name,
            "slug": org.slug,
            "type": org.type,
        },
        "base_plan": org_plan.base_plan,
        "superset_included": org_plan.superset_included,
        "subscription_duration": org_plan.subscription_duration,
        "features": org_plan.features,
        "start_date": org_plan.start_date,
        "end_date": org_plan.end_date,
        "can_upgrade_plan": org_plan.can_upgrade_plan,
    }
    return {"success": True, "res": response_data}
