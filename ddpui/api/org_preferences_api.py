import json
import os
from ninja import Router
from ninja.errors import HttpError
from django.utils import timezone
from ddpui import auth
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_supersets import OrgSupersets
from ddpui.models.org_plans import OrgPlans
from ddpui.models.userpreferences import UserPreferences
from ddpui.models.dashboard_chat import OrgAIContext
from ddpui.schemas.org_preferences_schema import (
    CreateOrgPreferencesSchema,
    UpdateLLMOptinSchema,
    UpdateDiscordNotificationsSchema,
    OrgAIDashboardChatSettingsResponse,
    UpdateOrgAIDashboardChatSchema,
    OrgAIDashboardChatStatusResponse,
)
from ddpui.core.notifications.notifications_functions import create_notification
from ddpui.schemas.notifications_api_schemas import NotificationDataSchema
from django.db import transaction
from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser
from ddpui.ddpdbt import elementary_service
from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpprefect import (
    prefect_service,
)
from ddpui.utils.awsses import send_text_message
from ddpui.utils.redis_client import RedisClient
from ddpui.utils.feature_flags import get_all_feature_flags_for_org

orgpreference_router = Router()


def _get_or_create_org_preferences(org):
    org_preferences = OrgPreferences.objects.filter(org=org).first()
    if org_preferences is None:
        org_preferences = OrgPreferences.objects.create(org=org)
    return org_preferences


def _get_or_create_org_ai_context(org):
    context, _ = OrgAIContext.objects.get_or_create(org=org)
    return context


def _is_dashboard_chat_feature_enabled(org) -> bool:
    return get_all_feature_flags_for_org(org).get("AI_DASHBOARD_CHAT", False)


def _ensure_dashboard_chat_feature_enabled(org) -> None:
    """Hide dashboard chat management APIs unless the feature flag is enabled."""
    if not _is_dashboard_chat_feature_enabled(org):
        raise HttpError(404, "Chat with dashboards is not enabled for this organization")


def _is_dbt_configured(org) -> bool:
    return org.dbt is not None


def _serialize_ai_dashboard_chat_settings(org, org_preferences, org_context):
    org_dbt = org.dbt
    return OrgAIDashboardChatSettingsResponse(
        feature_flag_enabled=_is_dashboard_chat_feature_enabled(org),
        ai_data_sharing_enabled=bool(org_preferences.ai_data_sharing_enabled),
        ai_data_sharing_consented_by=(
            org_preferences.ai_data_sharing_consented_by.user.email
            if org_preferences.ai_data_sharing_consented_by
            else None
        ),
        ai_data_sharing_consented_at=org_preferences.ai_data_sharing_consented_at,
        org_context_markdown=org_context.markdown,
        org_context_updated_by=org_context.updated_by.user.email if org_context.updated_by else None,
        org_context_updated_at=org_context.updated_at,
        dbt_configured=_is_dbt_configured(org),
        docs_generated_at=org_dbt.docs_generated_at if org_dbt else None,
        vector_last_ingested_at=org_dbt.vector_last_ingested_at if org_dbt else None,
    )


def _serialize_ai_dashboard_chat_status(org, org_preferences):
    org_dbt = org.dbt
    feature_flag_enabled = _is_dashboard_chat_feature_enabled(org)
    ai_data_sharing_enabled = bool(org_preferences.ai_data_sharing_enabled)
    dbt_configured = _is_dbt_configured(org)
    vector_last_ingested_at = org_dbt.vector_last_ingested_at if org_dbt else None

    return OrgAIDashboardChatStatusResponse(
        feature_flag_enabled=feature_flag_enabled,
        ai_data_sharing_enabled=ai_data_sharing_enabled,
        chat_available=(
            feature_flag_enabled
            and ai_data_sharing_enabled
            and dbt_configured
            and vector_last_ingested_at is not None
        ),
        dbt_configured=dbt_configured,
        docs_generated_at=org_dbt.docs_generated_at if org_dbt else None,
        vector_last_ingested_at=vector_last_ingested_at,
    )


@orgpreference_router.post("/")
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


@orgpreference_router.put("/llm_approval")
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


@orgpreference_router.put("/enable-discord-notifications")
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


@orgpreference_router.get("/")
def get_org_preferences(request):
    """Gets preferences for an organization based on the logged-in user's organization"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_preferences = OrgPreferences.objects.filter(org=org).first()
    if org_preferences is None:
        org_preferences = OrgPreferences.objects.create(org=org)

    return {"success": True, "res": org_preferences.to_json()}


@orgpreference_router.get("/ai-dashboard-chat")
@has_permission(["can_manage_org_settings"])
def get_ai_dashboard_chat_settings(request):
    """Load org-level dashboard chat settings and org AI context."""
    orguser: OrgUser = request.orguser
    org = orguser.org

    _ensure_dashboard_chat_feature_enabled(org)
    org_preferences = _get_or_create_org_preferences(org)
    org_context = _get_or_create_org_ai_context(org)

    return {
        "success": True,
        "res": _serialize_ai_dashboard_chat_settings(org, org_preferences, org_context).dict(),
    }


@orgpreference_router.put("/ai-dashboard-chat")
@has_permission(["can_manage_org_settings"])
@transaction.atomic
def update_ai_dashboard_chat_settings(request, payload: UpdateOrgAIDashboardChatSchema):
    """Update org-level dashboard chat consent and org AI context."""
    orguser: OrgUser = request.orguser
    org = orguser.org

    _ensure_dashboard_chat_feature_enabled(org)
    org_preferences = _get_or_create_org_preferences(org)
    org_context = _get_or_create_org_ai_context(org)
    target_ai_data_sharing_enabled = (
        payload.ai_data_sharing_enabled
        if payload.ai_data_sharing_enabled is not None
        else org_preferences.ai_data_sharing_enabled
    )

    if (
        payload.ai_data_sharing_enabled is True
        and org_preferences.ai_data_sharing_enabled is False
    ):
        org_preferences.ai_data_sharing_consented_by = orguser
        org_preferences.ai_data_sharing_consented_at = timezone.now()

    if payload.ai_data_sharing_enabled is not None:
        org_preferences.ai_data_sharing_enabled = payload.ai_data_sharing_enabled

    if payload.org_context_markdown is not None:
        if not target_ai_data_sharing_enabled:
            raise HttpError(
                409,
                "Enable AI data sharing before updating organization AI context",
            )
        org_context.markdown = payload.org_context_markdown
        org_context.updated_by = orguser
        org_context.updated_at = timezone.now()
        org_context.save()

    org_preferences.updated_at = timezone.now()
    org_preferences.save()

    return {
        "success": True,
        "res": _serialize_ai_dashboard_chat_settings(org, org_preferences, org_context).dict(),
    }

@orgpreference_router.get("/ai-dashboard-chat/status")
def get_ai_dashboard_chat_status(request):
    """Return feature readiness for dashboard chat for the current org."""
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_preferences = _get_or_create_org_preferences(org)

    return {"success": True, "res": _serialize_ai_dashboard_chat_status(org, org_preferences).dict()}


@orgpreference_router.get("/toolinfo")
def get_tools_versions(request):
    """get versions of the tools used in the system"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_superset = OrgSupersets.objects.filter(org=org).first()

    redis_key = f"org_tools_versions_{org.slug}"
    redis_client = RedisClient.get_instance()
    versions = redis_client.get(redis_key)
    if versions:
        return {"success": True, "res": json.loads(versions)}

    versions = []

    ver = airbyte_service.get_current_airbyte_version()
    versions.append({"Airbyte": {"version": ver if ver else "Not available"}})

    # Prefect Version
    ver = prefect_service.get_prefect_version()
    versions.append({"Prefect": {"version": ver if ver else "Not available"}})

    # dbt Version
    ver = elementary_service.get_dbt_version(org)
    versions.append({"DBT": {"version": ver if ver else "Not available"}})

    # elementary Version
    ver = elementary_service.get_edr_version(org)
    versions.append({"Elementary": {"version": ver if ver else "Not available"}})

    # Superset Version
    versions.append(
        {
            "Superset": {
                "version": org_superset.superset_version if org_superset else "Not available"
            }
        }
    )

    redis_client.set(redis_key, json.dumps(versions), ex=24 * 3600)

    return {"success": True, "res": versions}


@orgpreference_router.get("/org-plan")
def get_org_plans(request):
    """Gets preferences for an organization based on the logged-in user's organization"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_plan = OrgPlans.objects.filter(org=org).first()
    if org_plan is None:
        raise HttpError(400, "Org's Plan not found")

    return {"success": True, "res": org_plan.to_json()}


@orgpreference_router.post("/org-plan/upgrade")
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
