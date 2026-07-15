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


class UpdateSharingPreferencesSchema(Schema):
    """Schema for updating org-level Resource Sharing preferences
    (Task 11 Part B). Admin-gated -- see `update_sharing_preferences`.

    D1: per-role level defaults, replacing the old audience+level pair.
    """

    allow_public_sharing: Optional[bool] = None
    default_analyst_level: Optional[str] = None
    default_member_level: Optional[str] = None


class CreateOrgSupersetDetailsSchema(Schema):
    """Schema for creating organization superset details."""

    superset_version: Optional[str] = None
    container_name: Optional[str] = None
