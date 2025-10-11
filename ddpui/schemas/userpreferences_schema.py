from typing import Optional
from ninja import Schema


class CreateUserPreferencesSchema(Schema):
    """Schema for creating user preferences for the user."""

    enable_email_notifications: bool
    disclaimer_shown: Optional[bool] = None
    subscribe_schema_change_notifications: Optional[bool] = True
    subscribe_job_failure_notifications: Optional[bool] = True
    subscribe_dbt_test_failure_notifications: Optional[bool] = True


class UpdateUserPreferencesSchema(Schema):
    """Schema for updating user preferences for the user."""

    enable_email_notifications: Optional[bool] = None
    disclaimer_shown: Optional[bool] = None
    subscribe_schema_change_notifications: Optional[bool] = None
    subscribe_job_failure_notifications: Optional[bool] = None
    subscribe_dbt_test_failure_notifications: Optional[bool] = None
