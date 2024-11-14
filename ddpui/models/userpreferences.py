from django.db import models
from ddpui.models.org_user import OrgUser
from django.utils import timezone


class UserPreferences(models.Model):
    """Model to store user preferences for notifications"""

    orguser = models.OneToOneField(OrgUser, on_delete=models.CASCADE, related_name="preferences")
    enable_discord_notifications = models.BooleanField(default=False)
    discord_webhook = models.URLField(blank=True, null=True)
    enable_email_notifications = models.BooleanField(default=False)
    llm_optin = models.BooleanField(default=False)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(default=timezone.now)
