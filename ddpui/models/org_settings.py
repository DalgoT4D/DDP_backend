"""
Organization Settings model to store organization details and AI preferences
"""

from django.db import models
from django.utils import timezone
from ninja import Schema
from typing import Optional
from django.contrib.auth.models import User
from ddpui.models.org import Org


class OrgSettings(models.Model):
    """Organization settings including basic details and AI configuration"""

    org = models.OneToOneField(Org, on_delete=models.CASCADE, related_name="settings")

    # Organization details are now referenced from the related Org model
    organization_logo = models.BinaryField(
        null=True, blank=True, help_text="Organization logo image data"
    )
    organization_logo_filename = models.CharField(
        max_length=200, null=True, blank=True, help_text="Original filename of the uploaded logo"
    )
    organization_logo_content_type = models.CharField(
        max_length=100, null=True, blank=True, help_text="MIME type of the logo file"
    )

    # AI settings
    ai_data_sharing_enabled = models.BooleanField(
        default=False,
        help_text="Allow sharing organization data with AI for better contextual responses",
    )
    ai_logging_acknowledged = models.BooleanField(
        default=False,
        help_text="User has acknowledged that AI questions and answers are logged and stored for at least 3 months",
    )

    # Tracking fields for AI settings acceptance
    ai_settings_accepted_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        help_text="User who last accepted/modified AI settings",
    )
    ai_settings_accepted_at = models.DateTimeField(
        null=True, blank=True, help_text="When AI settings were last accepted/modified"
    )

    # Audit fields
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "ddpui_org_settings"
        verbose_name = "Organization Settings"
        verbose_name_plural = "Organization Settings"

    def __str__(self) -> str:
        return f"OrgSettings[{self.org.slug}|{self.org.name}]"


class OrgSettingsSchema(Schema):
    """Schema for organization settings API responses"""

    organization_name: Optional[str] = None  # Read-only, from org.name
    website: Optional[str] = None  # Read-only, from org.website
    ai_data_sharing_enabled: bool
    ai_logging_acknowledged: bool
    ai_settings_accepted_by_email: Optional[str] = None
    ai_settings_accepted_at: Optional[str] = None


class CreateOrgSettingsSchema(Schema):
    """Schema for creating/updating organization settings"""

    ai_data_sharing_enabled: bool = False
    ai_logging_acknowledged: bool = False


class UpdateOrgSettingsSchema(Schema):
    """Schema for partial updates to organization settings"""

    ai_data_sharing_enabled: Optional[bool] = None
    ai_logging_acknowledged: Optional[bool] = None
