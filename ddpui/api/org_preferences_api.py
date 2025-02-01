import os
from ninja import Router
from ninja.errors import HttpError
from django.utils import timezone
from ddpui import auth
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_supersets import OrgSupersets
from ddpui.models.org_plans import OrgPlans
from ddpui.models.userpreferences import UserPreferences
from ddpui.schemas.org_preferences_schema import (
    CreateOrgPreferencesSchema,
    UpdateLLMOptinSchema,
    UpdateDiscordNotificationsSchema,
    CreateOrgSupersetDetailsSchema,
)
from ddpui.core.notifications_service import create_notification
from ddpui.schemas.notifications_api_schemas import NotificationDataSchema
from django.db import transaction
from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser
from ddpui.ddpdbt import dbt_service
from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpprefect import (
    prefect_service,
)
from ddpui.utils.awsses import send_text_message

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
@transaction.atomic
def update_org_preferences(request, payload: UpdateLLMOptinSchema):
    """Updates llm preferences for the logged-in user's organization"""

    orguser: OrgUser = request.orguser
    org = orguser.org

    org_preferences = OrgPreferences.objects.filter(org=org).first()
    user_preferences = UserPreferences.objects.filter(orguser=orguser).first()

    if org_preferences is None:
        org_preferences = OrgPreferences.objects.create(org=org)

    if user_preferences is None:
        user_preferences = UserPreferences.objects.create(orguser=orguser)

    if payload.llm_optin is True:
        org_preferences.llm_optin = True
        org_preferences.llm_optin_approved_by = orguser
        org_preferences.llm_optin_date = timezone.now()
        user_preferences.disclaimer_shown = True
        org_preferences.enable_llm_request = False
        org_preferences.enable_llm_requested_by = None
    else:
        org_preferences.llm_optin = False
        org_preferences.llm_optin_approved_by = None
        org_preferences.llm_optin_date = None
    user_preferences.save()
    org_preferences.save()

    # sending notification to all users in the org.
    if payload.llm_optin is True:
        recipients: list[OrgUser] = OrgUser.objects.filter(org=org).all()

        notification_payload = NotificationDataSchema(
            author=orguser.user.email,
            message=f"The AI LLM Data Analysis feature is now enabled for {org.name}.",
            email_subject=f"{org.name}: AI LLM Data Analysis feature enabled",
            urgent=False,
            scheduled_time=None,
            recipients=[recipient.id for recipient in recipients],
        )

        error, res = create_notification(notification_payload)
        if res and "errors" in res and len(res["errors"]) > 0:
            raise HttpError(400, "Issue with creating the request notification")

    return {"success": True, "res": org_preferences.to_json()}


@orgpreference_router.put("/enable-discord-notifications", auth=auth.CustomAuthMiddleware())
@has_permission(["can_edit_org_notification_settings"])
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

    org_superset = OrgSupersets.objects.filter(org=org).first()

    versions = []

    ver = airbyte_service.get_current_airbyte_version()
    versions.append({"Airbyte": {"version": ver if ver else "Not available"}})

    # Prefect Version
    ver = prefect_service.get_prefect_version()
    versions.append({"Prefect": {"version": ver if ver else "Not available"}})

    # dbt Version
    ver = dbt_service.get_dbt_version(org)
    versions.append({"DBT": {"version": ver if ver else "Not available"}})

    # elementary Version
    ver = dbt_service.get_edr_version(org)
    versions.append({"Elementary": {"version": ver if ver else "Not available"}})

    # Superset Version
    versions.append(
        {
            "Superset": {
                "version": org_superset.superset_version if org_superset else "Not available"
            }
        }
    )

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


@orgpreference_router.post("/org-plan/upgrade", auth=auth.CustomAuthMiddleware())
@has_permission(["can_initiate_org_plan_upgrade"])
def initiate_upgrade_dalgo_plan(request):
    """User can click on the upgrade button from the settings panel
    which will trigger email to biz dev team"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_plan = OrgPlans.objects.filter(org=org).first()

    if not org_plan:
        raise HttpError(400, "Org's Plan not found")

    # trigger emails only once
    if org_plan.upgrade_requested:
        return {"success": True, "res": "Upgrade request already sent"}

    biz_dev_emails = os.getenv("BIZ_DEV_EMAILS", []).split(",")

    message = "Upgrade plan request from org: {org_name} with plan: {plan_name}".format(
        org_name=org.name, plan_name=org_plan.features
    )
    subject = "Upgrade plan request from org: {org_name}".format(org_name=org.name)

    for email in biz_dev_emails:
        send_text_message(email, subject, message)

    org_plan.upgrade_requested = True
    org_plan.save()

    return {"success": True}
