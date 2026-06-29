"""Org settings services for dashboard chat."""

from __future__ import annotations

from django.utils import timezone

from ddpui.core.dashboard_chat.metadata.build_service import (
    DashboardChatMetadataBuildService,
    summarize_dashboard_metadata_status,
)
from ddpui.core.dashboard_chat.metadata.pii_overrides import (
    serialize_dashboard_chat_pii_columns,
    update_dashboard_chat_pii_column_overrides,
)
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import OrgAIContext
from ddpui.models.org import Org
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_user import OrgUser
from ddpui.schemas.org_preferences_schema import (
    OrgAIDashboardChatMetadataStatusResponse,
    OrgAIDashboardChatPIIColumnsResponse,
    OrgAIDashboardChatSettingsResponse,
    OrgAIDashboardChatStatusResponse,
    TriggerOrgAIDashboardChatMetadataBuildSchema,
    UpdateDashboardChatPIIColumnOverridesSchema,
    UpdateOrgAIDashboardChatSchema,
)
from ddpui.utils.feature_flags import get_all_feature_flags_for_org


class DashboardChatFeatureDisabledError(Exception):
    """Raised when dashboard chat is disabled for an org."""


class DashboardChatDbtNotConfiguredError(Exception):
    """Raised when dashboard chat metadata cannot build without dbt config."""


class DashboardChatDashboardNotFoundError(Exception):
    """Raised when a requested dashboard does not belong to the org."""


class DashboardChatAIDataSharingDisabledError(Exception):
    """Raised when org AI context is updated before AI data sharing is enabled."""


def get_dashboard_chat_settings(org: Org) -> OrgAIDashboardChatSettingsResponse:
    """Return org-level dashboard chat settings and context."""
    _ensure_dashboard_chat_enabled(org)
    org_preferences, _ = OrgPreferences.objects.get_or_create(org=org)
    org_context, _ = OrgAIContext.objects.get_or_create(org=org)
    return _serialize_dashboard_chat_settings(org, org_preferences, org_context)


def update_dashboard_chat_settings(
    *,
    org: Org,
    orguser: OrgUser,
    payload: UpdateOrgAIDashboardChatSchema,
) -> OrgAIDashboardChatSettingsResponse:
    """Update dashboard chat consent, PII sharing, and org context settings."""
    _ensure_dashboard_chat_enabled(org)
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
            raise DashboardChatAIDataSharingDisabledError(
                "Enable AI data sharing before updating organization AI context"
            )
        org_context.markdown = payload.org_context_markdown
        org_context.updated_by = orguser
        org_context.updated_at = timezone.now()
        org_context.save()

    org_preferences.updated_at = timezone.now()
    org_preferences.save()

    return _serialize_dashboard_chat_settings(org, org_preferences, org_context)


def get_dashboard_chat_status(org: Org) -> OrgAIDashboardChatStatusResponse:
    """Return feature readiness for dashboard chat."""
    org_preferences, _ = OrgPreferences.objects.get_or_create(org=org)
    metadata_summary = summarize_dashboard_metadata_status(_native_dashboards(org))
    feature_flag_enabled = _dashboard_chat_enabled(org)
    ai_data_sharing_enabled = bool(org_preferences.ai_data_sharing_enabled)
    dbt_configured = org.dbt is not None

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
        metadata_last_built_at=metadata_summary["last_built_at"],
        metadata_ready_dashboard_count=metadata_summary["ready_dashboard_count"],
        metadata_total_dashboard_count=metadata_summary["total_dashboard_count"],
    )


def get_dashboard_chat_metadata_status(org: Org) -> OrgAIDashboardChatMetadataStatusResponse:
    """Return per-dashboard metadata status for dashboard chat settings."""
    _ensure_dashboard_chat_enabled(org)
    return OrgAIDashboardChatMetadataStatusResponse(
        **summarize_dashboard_metadata_status(_native_dashboards(org))
    )


def get_dashboard_chat_pii_columns(org: Org) -> OrgAIDashboardChatPIIColumnsResponse:
    """Return metadata columns and reviewed PII state for dashboard chat."""
    _ensure_dashboard_chat_enabled(org)
    return serialize_dashboard_chat_pii_columns(org)


def update_dashboard_chat_pii_columns(
    *,
    org: Org,
    orguser: OrgUser,
    payload: UpdateDashboardChatPIIColumnOverridesSchema,
) -> OrgAIDashboardChatPIIColumnsResponse:
    """Persist user-reviewed PII overrides for dashboard chat metadata columns."""
    _ensure_dashboard_chat_enabled(org)
    return update_dashboard_chat_pii_column_overrides(
        org=org,
        orguser=orguser,
        payload=payload,
    )


def trigger_dashboard_chat_metadata_build(
    *,
    org: Org,
    orguser: OrgUser,
    payload: TriggerOrgAIDashboardChatMetadataBuildSchema,
) -> OrgAIDashboardChatMetadataStatusResponse:
    """Mark requested dashboard metadata as building and enqueue the build task."""
    _ensure_dashboard_chat_enabled(org)
    if org.dbt is None:
        raise DashboardChatDbtNotConfiguredError(
            "Configure dbt before building dashboard chat metadata"
        )

    dashboards = _native_dashboards(org)
    if payload.dashboard_id is not None:
        dashboards = [dashboard for dashboard in dashboards if dashboard.id == payload.dashboard_id]
        if not dashboards:
            raise DashboardChatDashboardNotFoundError("Dashboard not found")

    builder_model = str(payload.builder_model or "o4-mini")
    DashboardChatMetadataBuildService().mark_dashboards_building(
        dashboards=dashboards,
        builder_model=builder_model,
    )
    # Keep the API import path from loading the central Celery task module.
    from ddpui.celeryworkers.tasks import build_dashboard_chat_metadata_artifacts

    build_dashboard_chat_metadata_artifacts.delay(
        org.id,
        [dashboard.id for dashboard in dashboards],
        builder_model,
        orguser.id,
    )
    return get_dashboard_chat_metadata_status(org)


def _serialize_dashboard_chat_settings(
    org: Org,
    org_preferences: OrgPreferences,
    org_context: OrgAIContext,
) -> OrgAIDashboardChatSettingsResponse:
    metadata_summary = summarize_dashboard_metadata_status(_native_dashboards(org))
    return OrgAIDashboardChatSettingsResponse(
        feature_flag_enabled=_dashboard_chat_enabled(org),
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
        dbt_configured=org.dbt is not None,
        metadata_last_built_at=metadata_summary["last_built_at"],
        metadata_ready_dashboard_count=metadata_summary["ready_dashboard_count"],
        metadata_total_dashboard_count=metadata_summary["total_dashboard_count"],
    )


def _ensure_dashboard_chat_enabled(org: Org) -> None:
    if not _dashboard_chat_enabled(org):
        raise DashboardChatFeatureDisabledError(
            "Chat with dashboards is not enabled for this organization"
        )


def _dashboard_chat_enabled(org: Org) -> bool:
    return bool(get_all_feature_flags_for_org(org).get("AI_DASHBOARD_CHAT", False))


def _native_dashboards(org: Org) -> list[Dashboard]:
    return list(Dashboard.objects.filter(org=org, dashboard_type="native").order_by("id"))
