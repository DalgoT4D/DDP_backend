from django.db import models
from ddpui.models.org_user import OrgUser

class Notification(models.Model):
    author = models.EmailField()
    message = models.TextField()
    timestamp = models.DateTimeField(auto_now_add=True)
    urgent = models.BooleanField(default=False)
    scheduled_time = models.DateTimeField(null=True, blank=True)
    sent_time = models.DateTimeField(null=True, blank=True)

class NotificationRecipient(models.Model):
    notification = models.ForeignKey(Notification, on_delete=models.CASCADE, related_name='recipients')
    recipient = models.ForeignKey(OrgUser, on_delete=models.CASCADE, related_name='notifications_received')
    read_status = models.BooleanField(default=False)

class UserPreference(models.Model):
    user = models.OneToOneField(OrgUser, on_delete=models.CASCADE, related_name='preferences')
    enable_discord_notifications = models.BooleanField(default=True)
    enable_email_notifications = models.BooleanField(default=True)
    discord_webhook = models.URLField(blank=True, null=True)