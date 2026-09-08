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
    # additive, for the admin broadcast path (features/admin-portal/plan.md §4.1):
    # null/empty target_org_ids = whole platform; a pre-existing row keeps
    # target_org_ids=NULL ("audience unknown, legacy") and both channels on.
    target_org_ids = models.JSONField(null=True, blank=True)
    send_in_app = models.BooleanField(default=True)
    send_email = models.BooleanField(default=True)


class NotificationRecipient(models.Model):
    """Model to store notification recipients and their read status"""

    notification = models.ForeignKey(
        Notification, on_delete=models.CASCADE, related_name="notifications_received"
    )
    recipient = models.ForeignKey(OrgUser, on_delete=models.CASCADE, related_name="recipients")
    read_status = models.BooleanField(default=False)
    task_id = models.TextField()
