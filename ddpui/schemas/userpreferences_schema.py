from typing import Optional, Literal
from ninja import Schema


class CreateUserPreferencesSchema(Schema):
    """Schema for creating user preferences for the user."""

    enable_email_notifications: bool
    disclaimer_shown: Optional[bool] = None
    last_visited_transform_tab: Optional[Literal["ui", "github"]] = None


class UpdateUserPreferencesSchema(Schema):
    """Schema for updating user preferences for the user."""

    enable_email_notifications: Optional[bool] = None
    disclaimer_shown: Optional[bool] = None
    last_visited_transform_tab: Optional[Literal["ui", "github"]] = None
