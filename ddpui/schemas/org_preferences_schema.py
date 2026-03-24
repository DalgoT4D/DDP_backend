from typing import Optional
from datetime import datetime
from ninja import Schema


class CreateOrgPreferencesSchema(Schema):
    """Schema for creating organization preferences."""

    org: Optional[int]
    trial_start_date: Optional[datetime]
    trial_end_date: Optional[datetime]
    llm_optin: Optional[bool] = False
    llm_optin_approved_by: Optional[int]
    llm_optin_date: Optional[datetime]
    enable_discord_notifications: Optional[bool] = False
    discord_webhook: Optional[str]


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
    discord_webhook: Optional[str]


class OrgAIDashboardChatSettingsResponse(Schema):
    """Response schema for org-level dashboard chat settings."""

    feature_flag_enabled: bool
    ai_data_sharing_enabled: bool
    ai_data_sharing_consented_by: Optional[str]
    ai_data_sharing_consented_at: Optional[datetime]
    org_context_markdown: str
    org_context_updated_by: Optional[str]
    org_context_updated_at: Optional[datetime]
    dbt_configured: bool
    docs_generated_at: Optional[datetime]
    vector_last_ingested_at: Optional[datetime]


class UpdateOrgAIDashboardChatSchema(Schema):
    """Request schema for org-level dashboard chat settings updates."""

    ai_data_sharing_enabled: Optional[bool] = None
    org_context_markdown: Optional[str] = None


class OrgAIDashboardChatStatusResponse(Schema):
    """Response schema for dashboard chat readiness."""

    feature_flag_enabled: bool
    ai_data_sharing_enabled: bool
    chat_available: bool
    dbt_configured: bool
    docs_generated_at: Optional[datetime]
    vector_last_ingested_at: Optional[datetime]


class CreateOrgSupersetDetailsSchema(Schema):
    """Schema for creating organization superset details."""

    superset_version: Optional[str]
    container_name: Optional[str]
