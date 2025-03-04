from typing import Optional
from datetime import datetime
from ninja import Schema


class CreateOrgPreferencesSchema(Schema):
    """Schema for creating organization preferences."""

    org: Optional[int]
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


class CreateOrgSupersetDetailsSchema(Schema):
    """Schema for creating organization superset details."""

    superset_version: Optional[str]
    container_name: Optional[str]
