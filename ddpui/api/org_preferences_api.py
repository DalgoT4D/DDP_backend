from ninja import Router
from ninja.errors import HttpError
from django.utils import timezone
from ddpui import auth
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_supersets import OrgSupersets
from ddpui.models.org_plans import OrgPlans
from ddpui.schemas.org_preferences_schema import (
    CreateOrgPreferencesSchema,
    UpdateLLMOptinSchema,
    UpdateDiscordNotificationsSchema,
    CreateOrgSupersetDetailsSchema,
)
from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser
from ddpui.ddpdbt import dbt_service
from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpprefect import (
    prefect_service,
)

orgpreference_router = Router()


@orgpreference_router.post("/", auth=auth.CustomAuthMiddleware())
def create_org_preferences(request, payload: CreateOrgPreferencesSchema):
    """Creates preferences for an organization"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    payload.org = org
    if OrgPreferences.objects.filter(org=org).exists():
        raise HttpError(400, "Organization preferences already exist")

    payload_data = payload.dict(exclude={"org"})

    org_preferences = OrgPreferences.objects.create(
        org=org, **payload_data  # Use the rest of the payload
    )

    return {"success": True, "res": org_preferences.to_json()}


@orgpreference_router.put("/llm_approval", auth=auth.CustomAuthMiddleware())
@has_permission(["can_edit_llm_settings"])
def update_org_preferences(request, payload: UpdateLLMOptinSchema):
    """Updates llm preferences for the logged-in user's organization"""

    orguser: OrgUser = request.orguser
    org = orguser.org

    org_preferences = OrgPreferences.objects.filter(org=org).first()
    if org_preferences is None:
        org_preferences = OrgPreferences.objects.create(org=org)

    if payload.llm_optin is True:
        org_preferences.llm_optin = True
        org_preferences.llm_optin_approved_by = orguser
        org_preferences.llm_optin_date = timezone.now()
    else:
        org_preferences.llm_optin = False
        org_preferences.llm_optin_approved_by = None
        org_preferences.llm_optin_date = None

    org_preferences.save()

    return {"success": True, "res": org_preferences.to_json()}


@orgpreference_router.put("/enable-discord-notifications", auth=auth.CustomAuthMiddleware())
@has_permission(["can_edit_discord_notifications_settings"])
def update_discord_notifications(request, payload: UpdateDiscordNotificationsSchema):
    """Updates Discord notifications preferences for the logged-in user's organization."""

    orguser: OrgUser = request.orguser
    org = orguser.org

    org_preferences = OrgPreferences.objects.filter(org=org).first()
    if org_preferences is None:
        org_preferences = OrgPreferences.objects.create(org=org)

    if payload.enable_discord_notifications:
        if not org_preferences.discord_webhook and not payload.discord_webhook:
            raise HttpError(400, "Discord webhook is required to enable notifications.")
        if payload.discord_webhook:
            org_preferences.discord_webhook = payload.discord_webhook
        org_preferences.enable_discord_notifications = True
    else:
        org_preferences.discord_webhook = None
        org_preferences.enable_discord_notifications = False

    org_preferences.save()

    return {"success": True, "res": org_preferences.to_json()}


@orgpreference_router.get("/", auth=auth.CustomAuthMiddleware())
def get_org_preferences(request):
    """Gets preferences for an organization based on the logged-in user's organization"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_preferences = OrgPreferences.objects.filter(org=org).first()
    if org_preferences is None:
        org_preferences = OrgPreferences.objects.create(org=org)

    return {"success": True, "res": org_preferences.to_json()}


@orgpreference_router.get("/toolinfo", auth=auth.CustomAuthMiddleware())
def get_tools_versions(request):
    """get versions of the tools used in the system"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    versions = []

    airbyte_version = airbyte_service.get_airbyte_version()
    versions.append({"Airbyte": {"version": airbyte_version}})

    prefect_version = prefect_service.get_prefect_server_version()
    versions.append({"Prefect": {"version": prefect_version}})

    dbt_version = dbt_service.get_dbt_version()
    versions.append({"DBT": {"version": dbt_version}})

    edr_version = dbt_service.get_edr_version()
    versions.append({"Elementary": {"version": edr_version}})

    org_superset = OrgSupersets.objects.filter(org=org).first()
    superset_version = org_superset.superset_version if org_superset else "Not available"
    versions.append({"Superset": {"version": superset_version}})

    return {"success": True, "res": versions}


@orgpreference_router.get("/org-plan", auth=auth.CustomAuthMiddleware())
def get_org_plans(request):
    """Gets preferences for an organization based on the logged-in user's organization"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_plan = OrgPlans.objects.filter(org=org).first()
    if org_plan is None:
        raise HttpError(400, "Org's Plan not found")

    return {"success": True, "res": org_plan.to_json()}
