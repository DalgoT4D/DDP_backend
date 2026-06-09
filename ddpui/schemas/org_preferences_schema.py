from typing import Optional
from datetime import datetime
from ninja import Schema


class CreateOrgPreferencesSchema(Schema):
    """Schema for creating organization preferences."""

    org: Optional[int] = None
    trial_start_date: Optional[datetime] = None
    trial_end_date: Optional[datetime] = None
    llm_optin: Optional[bool] = False
    llm_optin_approved_by: Optional[int] = None
    llm_optin_date: Optional[datetime] = None
    enable_discord_notifications: Optional[bool] = False
    discord_webhook: Optional[str] = None


class UpdateOrgPreferencesSchema(Schema):
    """Schema for updating organization preferences."""

    trial_start_date: Optional[datetime] = None
    trial_end_date: Optional[datetime] = None
    llm_optin: Optional[bool] = None
    llm_optin_approved_by: Optional[int] = None
    llm_optin_date: Optional[datetime] = None
    enable_discord_notifications: Optional[bool] = None
    discord_webhook: Optional[str] = None


class UpdateLLMOptinSchema(Schema):
    """Schema for updating organization LLM approval preference."""

    llm_optin: bool


class UpdateDiscordNotificationsSchema(Schema):
    """Schema for updating organization discord notification settings."""

    enable_discord_notifications: bool
    discord_webhook: Optional[str] = None


class OrgAIDashboardChatSettingsResponse(Schema):
    """Response schema for org-level dashboard chat settings."""

    feature_flag_enabled: bool
    ai_data_sharing_enabled: bool
    dashboard_chat_share_pii_with_llms: bool
    ai_data_sharing_consented_by: Optional[str]
    ai_data_sharing_consented_at: Optional[datetime]
    org_context_markdown: str
    org_context_updated_by: Optional[str]
    org_context_updated_at: Optional[datetime]
    dbt_configured: bool
    metadata_last_built_at: Optional[datetime]
    metadata_ready_dashboard_count: int = 0
    metadata_total_dashboard_count: int = 0


class UpdateOrgAIDashboardChatSchema(Schema):
    """Request schema for org-level dashboard chat settings updates."""

    ai_data_sharing_enabled: Optional[bool] = None
    dashboard_chat_share_pii_with_llms: Optional[bool] = None
    org_context_markdown: Optional[str] = None


class OrgAIDashboardChatStatusResponse(Schema):
    """Response schema for dashboard chat readiness."""

    feature_flag_enabled: bool
    ai_data_sharing_enabled: bool
    chat_available: bool
    dbt_configured: bool
    metadata_last_built_at: Optional[datetime]
    metadata_ready_dashboard_count: int = 0
    metadata_total_dashboard_count: int = 0


class DashboardChatMetadataArtifactStatusItem(Schema):
    """One dashboard metadata artifact summary."""

    dashboard_id: int
    dashboard_title: str
    status: str
    table_count: int
    chart_count: int
    builder_model: str
    source_fingerprint: str
    built_at: Optional[datetime]
    error_message: Optional[str] = None


class OrgAIDashboardChatMetadataStatusResponse(Schema):
    """Org-scoped dashboard metadata build/status summary."""

    dashboards: list[DashboardChatMetadataArtifactStatusItem]
    total_dashboard_count: int
    ready_dashboard_count: int
    failed_dashboard_count: int
    stale_dashboard_count: int
    missing_dashboard_count: int
    last_built_at: Optional[datetime]


class TriggerOrgAIDashboardChatMetadataBuildSchema(Schema):
    """Request schema for manual metadata builds."""

    dashboard_id: Optional[int] = None
    builder_model: Optional[str] = "o4-mini"


class DashboardChatPIIColumnReviewItem(Schema):
    """One metadata column with inferred and user-reviewed PII state."""

    dashboard_id: int
    dashboard_title: str
    schema_name: str
    table_name: str
    full_table_name: str
    model_name: str
    column_name: str
    data_type: str
    description: str
    semantic_role: str
    value_semantics: str
    inferred_pii: bool
    override_pii: Optional[bool] = None
    effective_pii: bool


class OrgAIDashboardChatPIIColumnsResponse(Schema):
    """Org-scoped list of metadata columns available for PII review."""

    columns: list[DashboardChatPIIColumnReviewItem]
    total_column_count: int
    pii_column_count: int


class DashboardChatPIIColumnOverrideInput(Schema):
    """User-reviewed PII value for one metadata column."""

    schema_name: str = ""
    table_name: str
    column_name: str
    pii: bool


class UpdateDashboardChatPIIColumnOverridesSchema(Schema):
    """Batch update request for dashboard-chat PII overrides."""

    overrides: list[DashboardChatPIIColumnOverrideInput]


class CreateOrgSupersetDetailsSchema(Schema):
    """Schema for creating organization superset details."""

    superset_version: Optional[str] = None
    container_name: Optional[str] = None
