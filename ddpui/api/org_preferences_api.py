from ninja import Router
from ninja.errors import HttpError
from ddpui import auth
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_supersets import OrgSupersets
from django.utils import timezone
from ddpui.schemas.org_preferences_schema import (
    CreateOrgPreferencesSchema,
    UpdateOrgPreferencesSchema,
    UpdateLLMOptinSchema,
    UpdateDiscordNotificationsSchema,
    CreateOrgSupersetDetailsSchema,
)
from ddpui.utils.constants import (
    AIRBYTE_URL_TO_GET_VERSION,
    PREFECT_URL_TO_GET_VERSION,
    DBT_VERSION_COMMAND,
    ELEMENTARY_VERSION_COMMAND,
)
from ddpui.auth import has_permission
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
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

    # Create OrgPreferences
    org_preferences = OrgPreferences.objects.create(
        org=org, **payload_data  # Use the rest of the payload
    )

    preferences = {
        "trial_start_date": org_preferences.trial_start_date,
        "trial_end_date": org_preferences.trial_end_date,
        "llm_optin": org_preferences.llm_optin,
        "llm_optin_approved_by": org_preferences.llm_optin_approved_by,
        "llm_optin_date": org_preferences.llm_optin_date,
        "enable_discord_notifications": org_preferences.enable_discord_notifications,
        "discord_webhook": org_preferences.discord_webhook,
    }

    return {"success": True, "res": preferences}


# @orgpreference_router.put("/{org_id}/", auth=auth.CustomAuthMiddleware())
# def update_org_preferences(request, payload: UpdateOrgPreferencesSchema, org_id: int):
#     """Updates preferences for an organization"""
#     payload.llm_optin_approved_by = request.orguser
#     try:
#         org_preferences = OrgPreferences.objects.get(org_id=org_id)
#     except OrgPreferences.DoesNotExist:
#         raise HttpError(404, "Preferences for this organization not found")

#     #check this line please.
#     # if org.type=="demo":
#     #     if payload.trial_start_date is None or payload.trial_end_date is None:
#     #         raise HttpError(400, "Trial accounts must have trial_start_date and trial_end_date.")


#     if payload.llm_optin is True:
#         # if not request.orguser.has_permission("can_edit_llm_settings"):
#         #     raise HttpError(403, "You do not have permission to edit LLM settings.")

#         payload.llm_optin_date = timezone.now()
#         if payload.llm_optin_approved_by is None or payload.llm_optin_date is None:
#             raise HttpError(400, "If LLM opt-in is true, both llm_optin_approved_by and llm_optin_date must be provided.")

#     # Update fields only if they are present in the payload

#     org_preferences.save()

#     return {"success": True, "res": 1}


@orgpreference_router.put("/llm_approval", auth=auth.CustomAuthMiddleware())
@has_permission(["can_edit_llm_settings"])
def update_org_preferences(request, payload: UpdateLLMOptinSchema):
    """Updates llm preferences for the logged-in user's organization"""

    # Get the organization associated with the current user
    orguser: OrgUser = request.orguser
    org = orguser.org

    try:
        # Fetch organization preferences for the user's organization
        org_preferences = OrgPreferences.objects.get(org=org)
    except OrgPreferences.DoesNotExist:
        raise HttpError(404, "Preferences for this organization not found")

    # Determine fields based on the llm_optin value
    if payload.llm_optin is True:
        llm_optin_approved_by = request.orguser
        llm_optin_date = timezone.now()
    else:
        # Set them to None for False
        llm_optin_approved_by = None
        llm_optin_date = None

    # Update the OrgPreferences instance
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

    try:
        org_preferences = OrgPreferences.objects.get(org=org)
    except OrgPreferences.DoesNotExist:
        raise HttpError(404, "Preferences for this organization not found")

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
    org = orguser.org  # Get the organization associated with the current user

    try:
        # Fetch organization preferences for the user's organization
        org_preferences = OrgPreferences.objects.select_related("org", "llm_optin_approved_by").get(
            org=org
        )
    except OrgPreferences.DoesNotExist:
        raise HttpError(404, "Organization preferences not found")

    # Prepare data for the user who approved LLM opt-in, if available
    approved_by_data = None
    if org_preferences.llm_optin_approved_by and org_preferences.llm_optin_approved_by.user:
        user = org_preferences.llm_optin_approved_by.user
        approved_by_data = {"user_id": user.pk, "email": user.email}

    # Structure the preferences response
    preferences = {
        "org": {
            "id": org_preferences.org.id,
            "name": org_preferences.org.name,
            "slug": org_preferences.org.slug,
            "type": org_preferences.org.type,
        },
        "trial_start_date": org_preferences.trial_start_date,
        "trial_end_date": org_preferences.trial_end_date,
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

    try:
        org_superset = OrgSupersets.objects.select_related(
            "org",
        ).get(org=org)

    except OrgSupersets.DoesNotExist:
        raise HttpError(404, "Organizations superset details not found")

    response_data = {
        "org": {
            "id": org_superset.org.id,
            "name": org_superset.org.name,
            "slug": org_superset.org.slug,
            "type": org_superset.org.type,
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
    orguser: OrgUser = request.orguser
    org = orguser.org

    try:
        # Validate org_id
        org_superset = OrgSupersets.objects.get(org=org)
    except OrgSupersets.DoesNotExist:
        return {"success": False, "error": "Organization not found"}

    versions = []

    # Airbyte Version
    try:
        airbyte_url = AIRBYTE_URL_TO_GET_VERSION
        airbyte_response = requests.get(airbyte_url)
        if airbyte_response.status_code == 200:
            airbyte_data = airbyte_response.json()
            versions.append({"Airbyte": {"version": airbyte_data.get("version")}})
        else:
            versions.append({"Airbyte": {"version": "Not available"}})
    except Exception:
        versions.append({"Airbyte": {"version": "Not available"}})

    # Prefect Version
    try:
        prefect_url = PREFECT_URL_TO_GET_VERSION
        prefect_response = requests.get(prefect_url)
        if prefect_response.status_code == 200:
            version = prefect_response.text.strip().strip('"')
            versions.append({"Prefect": {"version": version}})
        else:
            versions.append({"Prefect": {"version": "Not available"}})
    except Exception:
        versions.append({"Prefect": {"version": "Not available"}})

    # dbt Version
    try:
        dbt_version_command = DBT_VERSION_COMMAND
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
        elementary_version_command = ELEMENTARY_VERSION_COMMAND
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
