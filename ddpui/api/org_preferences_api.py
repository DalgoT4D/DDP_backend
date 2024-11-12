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
    CreateOrgSupersetDetailsSchema
)
from ddpui.utils.constants import (
    AIRBYTE_URL_TO_GET_VERSION,
    PREFECT_URL_TO_GET_VERSION,
    DBT_VERSION_COMMAND,
    ELEMENTARY_VERSION_COMMAND
)
from ddpui.auth import has_permission
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
import requests
import subprocess
orgpreference_router = Router()



@orgpreference_router.post("/", auth=auth.CustomAuthMiddleware())
def create_org_preferences(request, payload: CreateOrgPreferencesSchema):
    print(payload, "payload")
    """Creates preferences for an organization"""
    if OrgPreferences.objects.filter(org_id=payload.org_id).exists():
        raise HttpError(400, "Organization preferences already exist")

    org_preferences = OrgPreferences.objects.create(
        **payload.dict()
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

@orgpreference_router.put("/{org_id}/llm_approval", auth=auth.CustomAuthMiddleware())
@has_permission(['can_edit_llm_settings'])
def update_org_preferences(request, payload: UpdateLLMOptinSchema, org_id: int):
    """Updates llm preferences for an organization"""

    try:
        org_preferences = OrgPreferences.objects.get(org_id=org_id)
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

@orgpreference_router.put("/{org_id}/enable-discord-notifications", auth=auth.CustomAuthMiddleware())
@has_permission(['can_edit_discord_notifications_settings'])
def update_org_preferences(request, payload: UpdateDiscordNotificationsSchema, org_id: int):
    """Updates preferences for an organization"""

    try:
        org_preferences = OrgPreferences.objects.get(org_id=org_id)
    except OrgPreferences.DoesNotExist:
        raise HttpError(404, "Preferences for this organization not found")
    
    # Check for discord_link in the database when enabling notifications
    if payload.enable_discord_notifications:
        if not org_preferences.discord_webhook and not payload.discord_webhook:
            raise HttpError(400, "Discord link is missing. Please add a Discord link to enable notifications.")

    # Update the OrgPreferences instance
    org_preferences.enable_discord_notifications = payload.enable_discord_notifications
    if payload.discord_webhook:
        org_preferences.discord_webhook =payload.discord_webhook
    org_preferences.save()

    return {"success": True, "res": 1}





@orgpreference_router.get("/{org_id}/", auth=auth.CustomAuthMiddleware())
def get_org_preferences(request, org_id: int):
    """Gets preferences for an organization by org_id"""
    try:
        org_preferences = OrgPreferences.objects.select_related(
        "org", 
        "llm_optin_approved_by"
    ).get(org_id=org_id)
    except OrgPreferences.DoesNotExist:
        raise HttpError(404, "Organization preferences not found")
    
    approved_by_data = None
    if org_preferences.llm_optin_approved_by and org_preferences.llm_optin_approved_by.user:
        user = org_preferences.llm_optin_approved_by.user 
        approved_by_data = {
            "user_id": user.pk,           
            "email": user.email             
        }

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

# api to get more superset related information.
@orgpreference_router.get("/{org_id}/org-superset", auth=auth.CustomAuthMiddleware())
def get_org_superset_details(request, org_id: int):
    """Gets preferences for an organization by org_id"""
    try:
        org_superset = OrgSupersets.objects.select_related(
        "org", 
    ).get(org_id=org_id)
        
    except OrgSupersets.DoesNotExist:
        raise HttpError(404, "Organizations superset details not found")
    
    response_data = {
        "org": {
            "id": org_superset.org.id,
            "name": org_superset.org.name,
            "slug": org_superset.org.slug,
            "type": org_superset.org.type,
        },
        "superset_version": org_superset.superset_version
    }
    return {"success": True, "res": response_data}



@orgpreference_router.post("/{org_id}/org-superset", auth=auth.CustomAuthMiddleware())
def create_org_superset_details(request, payload:CreateOrgSupersetDetailsSchema, org_id: int):
    """Creates an entry in db with org superset details"""
    try:
        org = Org.objects.get(id=org_id)
    except Org.DoesNotExist:
        raise HttpError(404, "Organization not found")

    org_superset, created = OrgSupersets.objects.update_or_create(
        org=org,  
        **payload.dict()
    )

    response_data = {
        "org": {
            "id": org.id,
            "name": org.name,
            "slug": org.slug,
            "type": org.type,
        },
        "id": org_superset.id,  
        "superset_version": org_superset.superset_version
    }

    return {"success": True, "res": response_data}

@orgpreference_router.get("/{org_id}/versions", auth=auth.CustomAuthMiddleware())
def get_tools_versions( request, org_id: int):
        versions = {}

        # Airbyte Version
        try:
            airbyte_url = AIRBYTE_URL_TO_GET_VERSION 
            airbyte_response = requests.get(airbyte_url)
            if airbyte_response.status_code == 200:
                airbyte_data = airbyte_response.json()
                versions["Airbyte"] = airbyte_data.get("version")
            else:
                versions["Airbyte"] = "Error: Unable to retrieve version"
        except Exception as e:
            versions["Airbyte"] = f"Error: {e}"

        # Prefect Version
        try:
            prefect_url =PREFECT_URL_TO_GET_VERSION
            prefect_response = requests.get(prefect_url)
            if prefect_response.status_code == 200:
                versions["Prefect"] = prefect_response.text.strip()
            else:
                versions["Prefect"] = "Error: Unable to retrieve version"
        except Exception as e:
            versions["Prefect"] = f"Error: {e}"

        # dbt Version
        try:
            dbt_version_command = DBT_VERSION_COMMAND
            dbt_output = subprocess.check_output(dbt_version_command, text=True)
            for line in dbt_output.splitlines():
                if "installed:" in line:
                    versions["dbt"] = line.split(":")[1].strip()
                    break
        except Exception as e:
            versions["dbt"] = f"Error: {e}"

        # Elementary Version
        try:
            elementary_version_command = ELEMENTARY_VERSION_COMMAND
            elementary_output = subprocess.check_output(elementary_version_command, text=True)
            for line in elementary_output.splitlines():
                if "Elementary version" in line:
                    versions["Elementary"] = line.split()[-1].strip()
                    break
        except Exception as e:
            versions["Elementary"] = f"Error: {e}"

        # Superset_version
        try:
            org_superset = OrgSupersets.objects.get(org_id=org_id)
        except OrgSupersets.DoesNotExist:
            raise HttpError(404, "Organizations superset details not found")
        
        versions["Superset"] = org_superset.superset_version

        return {"success": True, "res": versions}