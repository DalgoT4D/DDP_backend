from typing import List, Optional
from datetime import datetime
from ninja import Schema


class CreateNotificationSchema(Schema):
    """Schema for creating a new notification."""

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
