from django.db import models
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.general_access import AccessLevel
from django.utils import timezone


class OrgPreferences(models.Model):
    """Model to store org preferences for settings panel"""

    org = models.OneToOneField(Org, on_delete=models.CASCADE, related_name="preferences")
    llm_optin = models.BooleanField(default=False)
    llm_optin_approved_by = models.ForeignKey(
        OrgUser, on_delete=models.CASCADE, related_name="approvedby", null=True, blank=True
    )
    llm_optin_date = models.DateTimeField(null=True, blank=True)
    enable_llm_request = models.BooleanField(default=False)
    enable_llm_requested_by = models.ForeignKey(
        OrgUser, on_delete=models.CASCADE, related_name="llm_request", null=True, blank=True
    )
    enable_discord_notifications = models.BooleanField(default=False)
    discord_webhook = models.URLField(blank=True, null=True)

    # Org-level defaults for newly created shareable resources. VIEW/VIEW
    # (not NONE/NONE) preserves the pre-existing product default for orgs
    # whose row is auto-created before they ever configure these levels.
    default_analyst_level = models.CharField(
        max_length=5, choices=AccessLevel.choices, default=AccessLevel.VIEW
    )
    default_member_level = models.CharField(
        max_length=5, choices=AccessLevel.choices, default=AccessLevel.VIEW
    )
    allow_public_sharing = models.BooleanField(default=True)

    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(default=timezone.now)

    def to_json(self) -> dict:
        """Return a dict representation of the model"""
        return {
            "org": {
                "name": self.org.name,
                "slug": self.org.slug,
            },
            "llm_optin": bool(self.llm_optin),
            "llm_optin_approved_by": (
                self.llm_optin_approved_by.user.email if self.llm_optin_approved_by else None
            ),
            "llm_optin_date": self.llm_optin_date.isoformat() if self.llm_optin_date else None,
            "enable_discord_notifications": bool(self.enable_discord_notifications),
            "discord_webhook": self.discord_webhook,
            "allow_public_sharing": bool(self.allow_public_sharing),
            "default_analyst_level": self.default_analyst_level,
            "default_member_level": self.default_member_level,
        }
