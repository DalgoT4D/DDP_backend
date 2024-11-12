from django.db import models
from enum import Enum
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from django.utils import timezone
from django.core.exceptions import ValidationError
class OrgType(str, Enum):
    """an enum representing the type of organization"""

    DEMO = "demo"
    POC = "poc"
    CLIENT = "client"

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]
class OrgPreferences(models.Model):
    """Model to store org preferences for settings panel"""

    org = models.OneToOneField(Org, on_delete=models.CASCADE, related_name="preferences")
    trial_start_date = models.DateTimeField(null=True, blank=True, default=None)
    trial_end_date = models.DateTimeField(null=True, blank=True, default=None)
    llm_optin = models.BooleanField(default=False)
    llm_optin_approved_by = models.ForeignKey(
        OrgUser, on_delete=models.CASCADE, related_name="approvedby", null=True, blank=True
    )
    llm_optin_date = models.DateTimeField(null=True, blank=True)
    enable_discord_notifications = models.BooleanField(default=False)
    discord_webhook = models.URLField(blank=True, null=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(default=timezone.now)
