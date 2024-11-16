from typing import Optional
from ninja import Schema


class CreateUserPreferencesSchema(Schema):
    """Schema for creating user preferences for the user."""

    enable_email_notifications: bool


class UpdateUserPreferencesSchema(Schema):
    """Schema for updating user preferences for the user."""

    enable_email_notifications: Optional[bool] = None
