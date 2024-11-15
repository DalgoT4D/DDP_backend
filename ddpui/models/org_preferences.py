from django.db import models
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from django.utils import timezone


class OrgPreferences(models.Model):
    """Model to store org preferences for settings panel"""

    org = models.OneToOneField(Org, on_delete=models.CASCADE, related_name="preferences")
    llm_optin = models.BooleanField(default=False)
    llm_optin_approved_by = models.ForeignKey(
        OrgUser, on_delete=models.CASCADE, related_name="approvedby", null=True, blank=True
    )
    llm_optin_date = models.DateTimeField(null=True, blank=True)
    enable_discord_notifications = models.BooleanField(default=False)
    discord_webhook = models.URLField(blank=True, null=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(default=timezone.now)

    def to_json(self) -> dict:
        """Return a dict representation of the model"""
        return {
            "org": {
                "name": self.org.name,
                "slug": self.org.slug,
                "type": self.org.type,
            },
            "llm_optin": str(self.llm_optin),
            "llm_optin_approved_by": (
                self.llm_optin_approved_by.user.email if self.llm_optin_approved_by else None
            ),
            "llm_optin_date": self.llm_optin_date.isoformat() if self.llm_optin_date else None,
            "enable_discord_notifications": str(self.enable_discord_notifications),
            "discord_webhook": self.discord_webhook,
        }
