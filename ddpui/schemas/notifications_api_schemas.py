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
    LATE_RUNS = "late_runs"
    DBT_TEST_FAILURE = "dbt_test_failure"


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
    category: Optional[NotificationCategoryEnum] = NotificationCategoryEnum.INCIDENT

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
    category: Optional[str] = "incident"


class CategorySubscriptionSchema(Schema):
    """Schema for updating category subscription preferences"""

    subscribe_incident_notifications: Optional[bool] = None
    subscribe_schema_change_notifications: Optional[bool] = None
    subscribe_job_failure_notifications: Optional[bool] = None
    subscribe_late_runs_notifications: Optional[bool] = None
    subscribe_dbt_test_failure_notifications: Optional[bool] = None
