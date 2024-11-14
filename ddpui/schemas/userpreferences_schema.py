from typing import Optional
from ninja import Schema


class CreateUserPreferencesSchema(Schema):
    """Schema for creating user preferences for the user."""

    enable_discord_notifications: bool
    discord_webhook: Optional[str] = None
    enable_email_notifications: bool
    llm_optin: bool


class UpdateUserPreferencesSchema(Schema):
    """Schema for updating user preferences for the user."""

    enable_discord_notifications: Optional[bool] = None
    discord_webhook: Optional[str] = None
    enable_email_notifications: Optional[bool] = None
    llm_optin: Optional[bool]
