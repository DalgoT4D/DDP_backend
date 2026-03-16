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
    ai_data_sharing_enabled: Optional[bool] = False
    ai_data_sharing_consented_by: Optional[int] = None
    ai_data_sharing_consented_at: Optional[datetime] = None
    ai_org_context_markdown: Optional[str] = ""
    ai_org_context_updated_by: Optional[int] = None
    ai_org_context_updated_at: Optional[datetime] = None
    ai_chat_source_config: Optional[dict] = None


class UpdateOrgPreferencesSchema(Schema):
    """Schema for updating organization preferences."""

    trial_start_date: Optional[datetime] = None
    trial_end_date: Optional[datetime] = None
    llm_optin: Optional[bool] = None
    llm_optin_approved_by: Optional[int] = None
    llm_optin_date: Optional[datetime] = None
    enable_discord_notifications: Optional[bool] = None
    discord_webhook: Optional[str] = None
    ai_data_sharing_enabled: Optional[bool] = None
    ai_data_sharing_consented_by: Optional[int] = None
    ai_data_sharing_consented_at: Optional[datetime] = None
    ai_org_context_markdown: Optional[str] = None
    ai_org_context_updated_by: Optional[int] = None
    ai_org_context_updated_at: Optional[datetime] = None
    ai_chat_source_config: Optional[dict] = None


class UpdateLLMOptinSchema(Schema):
    """Schema for updating organization LLM approval preference."""

    llm_optin: bool


class AiChatSourceConfigSchema(Schema):
    """Feature-source toggles for dashboard chat ingestion and retrieval."""

    org_context: bool = True
    dashboard_context: bool = True
    dbt_manifest: bool = True
    dbt_catalog: bool = True


class UpdateAiDashboardChatSettingsSchema(Schema):
    """Payload for AI dashboard chat settings."""

    ai_data_sharing_enabled: Optional[bool] = None
    ai_org_context_markdown: Optional[str] = None
    ai_chat_source_config: Optional[AiChatSourceConfigSchema] = None


class AiDashboardChatSettingsResponseSchema(Schema):
    """Response payload for AI dashboard chat settings."""

    feature_flag_enabled: bool
    ai_data_sharing_enabled: bool
    ai_data_sharing_consented_by: Optional[str] = None
    ai_data_sharing_consented_at: Optional[datetime] = None
    ai_org_context_markdown: str = ""
    ai_org_context_updated_by: Optional[str] = None
    ai_org_context_updated_at: Optional[datetime] = None
    ai_chat_source_config: AiChatSourceConfigSchema
    dbt_configured: bool = False
    ai_docs_generated_at: Optional[datetime] = None
    ai_vector_last_ingested_at: Optional[datetime] = None


class AiDashboardChatStatusResponseSchema(Schema):
    """Effective org-level availability for dashboard chat."""

    feature_flag_enabled: bool
    ai_data_sharing_enabled: bool
    chat_available: bool
    dbt_configured: bool = False
    ai_docs_generated_at: Optional[datetime] = None
    ai_vector_last_ingested_at: Optional[datetime] = None


class AiDashboardChatSettingsEnvelopeSchema(Schema):
    """Standard success envelope for AI dashboard chat settings endpoints."""

    success: bool
    res: AiDashboardChatSettingsResponseSchema


class AiDashboardChatStatusEnvelopeSchema(Schema):
    """Standard success envelope for AI dashboard chat status endpoint."""

    success: bool
    res: AiDashboardChatStatusResponseSchema


class UpdateDiscordNotificationsSchema(Schema):
    """Schema for updating organization discord notification settings."""

    enable_discord_notifications: bool
    discord_webhook: Optional[str]


class CreateOrgSupersetDetailsSchema(Schema):
    """Schema for creating organization superset details."""

    superset_version: Optional[str]
    container_name: Optional[str]
