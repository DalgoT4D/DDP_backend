from django.db import models
from ddpui.models.org_user import OrgUser


class Notification(models.Model):
    """Model to store notifications for users"""

    author = models.EmailField()
    message = models.TextField()
    email_subject = models.TextField()
    timestamp = models.DateTimeField(auto_now_add=True)
    urgent = models.BooleanField(default=False)
    scheduled_time = models.DateTimeField(null=True, blank=True)
    sent_time = models.DateTimeField(null=True, blank=True)
    # Structured payload for notifications a frontend row can act on directly
    # (e.g. access-request Approve/Deny). Nullable and generic: null keeps
    # rendering as plain text, and future actionable types reuse the field.
    metadata = models.JSONField(null=True, blank=True, default=None)


class NotificationRecipient(models.Model):
    """Model to store notification recipients and their read status"""

    notification = models.ForeignKey(
        Notification, on_delete=models.CASCADE, related_name="notifications_received"
    )
    recipient = models.ForeignKey(OrgUser, on_delete=models.CASCADE, related_name="recipients")
    read_status = models.BooleanField(default=False)
    task_id = models.TextField()
