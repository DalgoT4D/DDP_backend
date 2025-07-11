from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel
from enum import Enum
from ninja import Schema


class SentToEnum(str, Enum):
    """
    Schema for sent_to field in create notification
    api payload.
    """

    ALL_USERS = "all_users"
    ALL_ORG_USERS = "all_org_users"
    SINGLE_USER = "single_user"


class NotificationCategoryEnum(str, Enum):
    """
    Schema for notification categories
    """

    INCIDENT = "incident"
    SCHEMA_CHANGE = "schema_change"
    JOB_FAILURE = "job_failure"
    LATE_RUN = "late_run"
    DBT_TEST_FAILURE = "dbt_test_failure"
    SYSTEM = "system"


class CreateNotificationPayloadSchema(BaseModel):
    """Schema for creating a new notification api."""

    author: str
    message: str
    sent_to: SentToEnum
    urgent: Optional[bool] = False
    scheduled_time: Optional[datetime] = None
    user_email: Optional[str] = None
    manager_or_above: Optional[bool] = False
    org_slug: Optional[str] = None
    category: Optional[NotificationCategoryEnum] = NotificationCategoryEnum.SYSTEM

    class Config:
        use_enum_values = True


class UpdateReadStatusSchema(Schema):
    """Schema for updating the read status of a notification."""

    notification_id: int
    read_status: bool


class UpdateReadStatusSchemav1(Schema):
    """Schema for updating the read status of a notification."""

    notification_ids: list[int]
    read_status: bool


class NotificationDataSchema(Schema):
    """Schema use to call the notification service function for creating a notification"""

    author: str
    message: str
    email_subject: str
    urgent: Optional[bool] = False
    scheduled_time: Optional[datetime] = None
    recipients: List[int]  # list of orguser ids
    category: Optional[str] = "system"


class UserNotificationPreferencesSchema(Schema):
    """Schema for user notification preferences"""

    enable_email_notifications: bool
    subscribe_incident: bool
    subscribe_schema_change: bool
    subscribe_job_failure: bool
    subscribe_late_run: bool
    subscribe_dbt_test_failure: bool
    subscribe_system: bool


class NotificationFiltersSchema(Schema):
    """Schema for filtering notifications"""

    category: Optional[NotificationCategoryEnum] = None
    urgent_only: Optional[bool] = False


class UserNotificationPreferencesResponseSchema(Schema):
    """Schema for returning user notification preferences"""

    email_notifications_enabled: bool
    incident_notifications: bool
    schema_change_notifications: bool
    job_failure_notifications: bool
    late_run_notifications: bool
    dbt_test_failure_notifications: bool
    system_notifications: bool


class NotificationResponseSchema(Schema):
    """Schema for notification response with category info"""

    id: int
    message: str
    timestamp: datetime
    urgent: bool
    read_status: bool
    category: str
    author: str


class UrgentNotificationSchema(Schema):
    """Schema for urgent notifications to display in notification bar"""

    id: int
    message: str
    category: str
    timestamp: datetime
