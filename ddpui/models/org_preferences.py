from django.db import models
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from django.utils import timezone


def default_ai_chat_source_config() -> dict:
    """Default source toggle state for dashboard chat context ingestion."""
    return {
        "org_context": True,
        "dashboard_context": True,
        "dbt_manifest": True,
        "dbt_catalog": True,
    }


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
    ai_data_sharing_enabled = models.BooleanField(default=False)
    ai_data_sharing_consented_by = models.ForeignKey(
        OrgUser,
        on_delete=models.SET_NULL,
        related_name="ai_data_sharing_consents",
        null=True,
        blank=True,
    )
    ai_data_sharing_consented_at = models.DateTimeField(null=True, blank=True)
    ai_org_context_markdown = models.TextField(blank=True, default="")
    ai_org_context_updated_by = models.ForeignKey(
        OrgUser,
        on_delete=models.SET_NULL,
        related_name="ai_org_context_updates",
        null=True,
        blank=True,
    )
    ai_org_context_updated_at = models.DateTimeField(null=True, blank=True)
    ai_chat_source_config = models.JSONField(default=default_ai_chat_source_config)
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
            "ai_data_sharing_enabled": bool(self.ai_data_sharing_enabled),
            "ai_data_sharing_consented_by": (
                self.ai_data_sharing_consented_by.user.email
                if self.ai_data_sharing_consented_by
                else None
            ),
            "ai_data_sharing_consented_at": (
                self.ai_data_sharing_consented_at.isoformat()
                if self.ai_data_sharing_consented_at
                else None
            ),
            "ai_org_context_markdown": self.ai_org_context_markdown,
            "ai_org_context_updated_by": (
                self.ai_org_context_updated_by.user.email if self.ai_org_context_updated_by else None
            ),
            "ai_org_context_updated_at": (
                self.ai_org_context_updated_at.isoformat()
                if self.ai_org_context_updated_at
                else None
            ),
            "ai_chat_source_config": self.ai_chat_source_config,
        }
