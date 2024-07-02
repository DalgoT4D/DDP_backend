from typing import List, Optional
from ninja import Schema
from datetime import datetime

# class CreateUserPreferencesSchema(Schema):
#     enable_discord_notifications: bool
#     enable_email_notifications: bool
#     discord_webhook: Optional[str] = None

# class UpdateUserPreferencesSchema(Schema):
#     enable_discord_notifications: Optional[bool] = None
#     enable_email_notifications: Optional[bool] = None
#     discord_webhook: Optional[str] = None

class CreateNotificationSchema(Schema):
    author: str
    message: str
    urgent: bool
    scheduled_time: Optional[datetime] = None
    recipients: List[int]

# class UpdateReadStatusSchema(Schema):
#     notification_id: int
#     read_status: bool

# class DeleteNotificationSchema(Schema):
#     notification_id: int