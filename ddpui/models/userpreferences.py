from django.db import models
from django.utils import timezone
from ddpui.models.org_user import OrgUser


class UserPreferences(models.Model):
    """Model to store user preferences for notifications"""

    orguser = models.OneToOneField(OrgUser, on_delete=models.CASCADE, related_name="preferences")
    enable_discord_notifications = models.BooleanField(default=False)  # deprecated
    discord_webhook = models.URLField(blank=True, null=True)  # deprecated
    enable_email_notifications = models.BooleanField(default=False)
    llm_optin = models.BooleanField(default=False)  # deprecated
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(default=timezone.now)

    def to_json(self) -> dict:
        """Return a dict representation of the model"""
        return {
            "enable_email_notifications": self.enable_email_notifications,
        }
