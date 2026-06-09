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
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import (
    DashboardChatMetadataArtifact,
    DashboardChatMetadataArtifactStatus,
    DashboardChatPIIColumnOverride,
    OrgAIContext,
)
from ddpui.core.dashboard_chat.metadata.build_service import (
    DashboardChatMetadataBuildService,
    summarize_dashboard_metadata_status,
)
from ddpui.core.dashboard_chat.metadata.pii_overrides import (
    load_pii_overrides_for_org,
    metadata_table_identity,
    payload_column_keys,
    pii_override_key,
)
from ddpui.core.dashboard_chat.metadata.schemas import DashboardChatMetadataArtifactPayload
from ddpui.celeryworkers.tasks import build_dashboard_chat_metadata_artifacts
from ddpui.schemas.org_preferences_schema import (
    CreateOrgPreferencesSchema,
    UpdateLLMOptinSchema,
    UpdateDiscordNotificationsSchema,
    OrgAIDashboardChatSettingsResponse,
    UpdateOrgAIDashboardChatSchema,
    OrgAIDashboardChatStatusResponse,
    OrgAIDashboardChatMetadataStatusResponse,
    OrgAIDashboardChatPIIColumnsResponse,
    TriggerOrgAIDashboardChatMetadataBuildSchema,
    UpdateDashboardChatPIIColumnOverridesSchema,
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


def _serialize_ai_dashboard_chat_settings(org, org_preferences, org_context):
    org_dbt = org.dbt
    native_dashboards = list(
        Dashboard.objects.filter(org=org, dashboard_type="native").order_by("id")
    )
    metadata_summary = summarize_dashboard_metadata_status(native_dashboards)
    return OrgAIDashboardChatSettingsResponse(
        feature_flag_enabled=get_all_feature_flags_for_org(org).get(
            "AI_DASHBOARD_CHAT",
            False,
        ),
        ai_data_sharing_enabled=bool(org_preferences.ai_data_sharing_enabled),
        dashboard_chat_share_pii_with_llms=bool(org_preferences.dashboard_chat_share_pii_with_llms),
        ai_data_sharing_consented_by=(
            org_preferences.ai_data_sharing_consented_by.user.email
            if org_preferences.ai_data_sharing_consented_by
            else None
        ),
        ai_data_sharing_consented_at=org_preferences.ai_data_sharing_consented_at,
        org_context_markdown=org_context.markdown,
        org_context_updated_by=org_context.updated_by.user.email
        if org_context.updated_by
        else None,
        org_context_updated_at=org_context.updated_at,
        dbt_configured=org_dbt is not None,
        metadata_last_built_at=metadata_summary["last_built_at"],
        metadata_ready_dashboard_count=metadata_summary["ready_dashboard_count"],
        metadata_total_dashboard_count=metadata_summary["total_dashboard_count"],
    )


def _serialize_ai_dashboard_chat_status(org, org_preferences):
    org_dbt = org.dbt
    native_dashboards = list(
        Dashboard.objects.filter(org=org, dashboard_type="native").order_by("id")
    )
    metadata_summary = summarize_dashboard_metadata_status(native_dashboards)
    feature_flag_enabled = get_all_feature_flags_for_org(org).get(
        "AI_DASHBOARD_CHAT",
        False,
    )
    ai_data_sharing_enabled = bool(org_preferences.ai_data_sharing_enabled)
    dbt_configured = org_dbt is not None
    last_built_at = metadata_summary["last_built_at"]

    return OrgAIDashboardChatStatusResponse(
        feature_flag_enabled=feature_flag_enabled,
        ai_data_sharing_enabled=ai_data_sharing_enabled,
        chat_available=(
            feature_flag_enabled
            and ai_data_sharing_enabled
            and dbt_configured
            and metadata_summary["ready_dashboard_count"] > 0
        ),
        dbt_configured=dbt_configured,
        metadata_last_built_at=last_built_at,
        metadata_ready_dashboard_count=metadata_summary["ready_dashboard_count"],
        metadata_total_dashboard_count=metadata_summary["total_dashboard_count"],
    )


def _load_dashboard_chat_metadata_payloads(
    org,
) -> list[tuple[Dashboard, DashboardChatMetadataArtifactPayload]]:
    artifacts = (
        DashboardChatMetadataArtifact.objects.filter(
            dashboard__org=org,
            dashboard__dashboard_type="native",
            status=DashboardChatMetadataArtifactStatus.READY,
        )
        .select_related("dashboard")
        .order_by("dashboard_id")
    )
    payloads: list[tuple[Dashboard, DashboardChatMetadataArtifactPayload]] = []
    for artifact in artifacts:
        if not artifact.artifact_json:
            continue
        try:
            payload = DashboardChatMetadataArtifactPayload.model_validate(artifact.artifact_json)
        except Exception:
            continue
        payloads.append((artifact.dashboard, payload))
    return payloads


def _serialize_dashboard_chat_pii_columns(org) -> OrgAIDashboardChatPIIColumnsResponse:
    payloads = _load_dashboard_chat_metadata_payloads(org)
    overrides = load_pii_overrides_for_org(org)
    columns: list[dict] = []
    for dashboard, payload in payloads:
        for table in payload.tables:
            schema_name, bare_table_name, full_table_name = metadata_table_identity(table)
            for column in table.columns:
                key = pii_override_key(schema_name, bare_table_name, column.column_name)
                override_pii = overrides.get(key)
                inferred_pii = bool(column.pii)
                effective_pii = inferred_pii if override_pii is None else override_pii
                columns.append(
                    {
                        "dashboard_id": dashboard.id,
                        "dashboard_title": str(dashboard.title or ""),
                        "schema_name": schema_name,
                        "table_name": bare_table_name,
                        "full_table_name": full_table_name,
                        "model_name": table.model_name,
                        "column_name": column.column_name,
                        "data_type": column.data_type,
                        "description": column.description,
                        "semantic_role": column.semantic_role,
                        "value_semantics": column.value_semantics,
                        "inferred_pii": inferred_pii,
                        "override_pii": override_pii,
                        "effective_pii": effective_pii,
                    }
                )

    columns.sort(
        key=lambda item: (
            item["dashboard_title"].lower(),
            item["full_table_name"].lower(),
            item["column_name"].lower(),
        )
    )
    return OrgAIDashboardChatPIIColumnsResponse(
        columns=columns,
        total_column_count=len(columns),
        pii_column_count=sum(1 for column in columns if column["effective_pii"]),
    )


@orgpreference_router.post("/")
def create_org_preferences(request, payload: CreateOrgPreferencesSchema):
    """Creates preferences for an organization"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    payload.org = org
    if OrgPreferences.objects.filter(org=org).exists():
        raise HttpError(400, "Organization preferences already exist")

    payload_data = payload.model_dump(exclude={"org"})

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

    if not get_all_feature_flags_for_org(org).get("AI_DASHBOARD_CHAT", False):
        raise HttpError(404, "Chat with dashboards is not enabled for this organization")
    org_preferences, _ = OrgPreferences.objects.get_or_create(org=org)
    org_context, _ = OrgAIContext.objects.get_or_create(org=org)

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

    if not get_all_feature_flags_for_org(org).get("AI_DASHBOARD_CHAT", False):
        raise HttpError(404, "Chat with dashboards is not enabled for this organization")
    org_preferences, _ = OrgPreferences.objects.get_or_create(org=org)
    org_context, _ = OrgAIContext.objects.get_or_create(org=org)
    target_ai_data_sharing_enabled = (
        payload.ai_data_sharing_enabled
        if payload.ai_data_sharing_enabled is not None
        else org_preferences.ai_data_sharing_enabled
    )

    if payload.ai_data_sharing_enabled is True and org_preferences.ai_data_sharing_enabled is False:
        org_preferences.ai_data_sharing_consented_by = orguser
        org_preferences.ai_data_sharing_consented_at = timezone.now()

    if payload.ai_data_sharing_enabled is not None:
        org_preferences.ai_data_sharing_enabled = payload.ai_data_sharing_enabled

    if payload.dashboard_chat_share_pii_with_llms is not None:
        org_preferences.dashboard_chat_share_pii_with_llms = (
            payload.dashboard_chat_share_pii_with_llms
        )

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

    org_preferences, _ = OrgPreferences.objects.get_or_create(org=org)

    return {
        "success": True,
        "res": _serialize_ai_dashboard_chat_status(org, org_preferences).dict(),
    }


@orgpreference_router.get(
    "/ai-dashboard-chat/metadata/status",
    response=OrgAIDashboardChatMetadataStatusResponse,
)
@has_permission(["can_manage_org_settings"])
def get_ai_dashboard_chat_metadata_status(request):
    """Return per-dashboard metadata build status for the current org."""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if not get_all_feature_flags_for_org(org).get("AI_DASHBOARD_CHAT", False):
        raise HttpError(404, "Chat with dashboards is not enabled for this organization")
    return OrgAIDashboardChatMetadataStatusResponse(
        **summarize_dashboard_metadata_status(
            list(Dashboard.objects.filter(org=org, dashboard_type="native").order_by("id"))
        )
    )


@orgpreference_router.get(
    "/ai-dashboard-chat/pii-columns",
    response=OrgAIDashboardChatPIIColumnsResponse,
)
@has_permission(["can_manage_org_settings"])
def get_ai_dashboard_chat_pii_columns(request):
    """Return metadata columns and reviewed PII state for the current org."""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if not get_all_feature_flags_for_org(org).get("AI_DASHBOARD_CHAT", False):
        raise HttpError(404, "Chat with dashboards is not enabled for this organization")
    return _serialize_dashboard_chat_pii_columns(org)


@orgpreference_router.put(
    "/ai-dashboard-chat/pii-columns",
    response=OrgAIDashboardChatPIIColumnsResponse,
)
@has_permission(["can_manage_org_settings"])
@transaction.atomic
def update_ai_dashboard_chat_pii_columns(
    request,
    payload: UpdateDashboardChatPIIColumnOverridesSchema,
):
    """Persist user-reviewed PII overrides for dashboard chat metadata columns."""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if not get_all_feature_flags_for_org(org).get("AI_DASHBOARD_CHAT", False):
        raise HttpError(404, "Chat with dashboards is not enabled for this organization")

    metadata_payloads = [
        metadata_payload for _, metadata_payload in _load_dashboard_chat_metadata_payloads(org)
    ]
    available_keys = payload_column_keys(metadata_payloads)
    for override in payload.overrides:
        schema_name = str(override.schema_name or "").strip()
        table_name = str(override.table_name or "").strip()
        column_name = str(override.column_name or "").strip()
        key = pii_override_key(schema_name, table_name, column_name)
        if key not in available_keys:
            raise HttpError(
                404,
                f"Column {schema_name}.{table_name}.{column_name} was not found in ready metadata",
            )
        DashboardChatPIIColumnOverride.objects.update_or_create(
            org=org,
            schema_name=key[0],
            table_name=key[1],
            column_name=key[2],
            defaults={
                "pii": bool(override.pii),
                "updated_by": orguser,
            },
        )

    return _serialize_dashboard_chat_pii_columns(org)


@orgpreference_router.post(
    "/ai-dashboard-chat/metadata/build",
    response=OrgAIDashboardChatMetadataStatusResponse,
)
@has_permission(["can_manage_org_settings"])
def build_ai_dashboard_chat_metadata(
    request,
    payload: TriggerOrgAIDashboardChatMetadataBuildSchema,
):
    """Manually build dashboard metadata artifacts for the current org."""
    orguser: OrgUser = request.orguser
    org = orguser.org

    if not get_all_feature_flags_for_org(org).get("AI_DASHBOARD_CHAT", False):
        raise HttpError(404, "Chat with dashboards is not enabled for this organization")
    if org.dbt is None:
        raise HttpError(409, "Configure dbt before building dashboard chat metadata")

    dashboards = list(Dashboard.objects.filter(org=org, dashboard_type="native").order_by("id"))
    if payload.dashboard_id is not None:
        dashboards = [dashboard for dashboard in dashboards if dashboard.id == payload.dashboard_id]
        if not dashboards:
            raise HttpError(404, "Dashboard not found")

    builder_model = str(payload.builder_model or "o4-mini")
    build_service = DashboardChatMetadataBuildService()
    build_service.mark_dashboards_building(
        dashboards=dashboards,
        builder_model=builder_model,
    )
    build_dashboard_chat_metadata_artifacts.delay(
        org.id,
        [dashboard.id for dashboard in dashboards],
        builder_model,
        orguser.id,
    )
    return OrgAIDashboardChatMetadataStatusResponse(
        **summarize_dashboard_metadata_status(
            list(Dashboard.objects.filter(org=org, dashboard_type="native").order_by("id"))
        )
    )


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
