from django.db import models
from django.utils import timezone
from ddpui.models.org_user import OrgUser


class UserPreferences(models.Model):
    """Model to store user preferences for notifications"""

    orguser = models.OneToOneField(OrgUser, on_delete=models.CASCADE, related_name="preferences")
    enable_discord_notifications = models.BooleanField(default=False)  # deprecated
    discord_webhook = models.URLField(blank=True, null=True)  # deprecated
    enable_email_notifications = models.BooleanField(default=False)
    disclaimer_shown = models.BooleanField(default=False)

    # Category subscription preferences
    subscribe_incident = models.BooleanField(
        default=True, help_text="Subscribe to incident notifications"
    )
    subscribe_schema_change = models.BooleanField(
        default=True, help_text="Subscribe to schema change notifications"
    )
    subscribe_job_failure = models.BooleanField(
        default=False, help_text="Subscribe to job failure notifications"
    )
    subscribe_late_run = models.BooleanField(
        default=False, help_text="Subscribe to late run notifications"
    )
    subscribe_dbt_test_failure = models.BooleanField(
        default=True, help_text="Subscribe to dbt test failure notifications"
    )
    subscribe_system = models.BooleanField(
        default=True, help_text="Subscribe to system notifications"
    )

    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(default=timezone.now)

    def to_json(self) -> dict:
        """Return a dict representation of the model"""
        return {
            "enable_email_notifications": self.enable_email_notifications,
            "disclaimer_shown": self.disclaimer_shown,
            "subscribe_incident": self.subscribe_incident,
            "subscribe_schema_change": self.subscribe_schema_change,
            "subscribe_job_failure": self.subscribe_job_failure,
            "subscribe_late_run": self.subscribe_late_run,
            "subscribe_dbt_test_failure": self.subscribe_dbt_test_failure,
            "subscribe_system": self.subscribe_system,
        }

    def is_subscribed_to_category(self, category: str) -> bool:
        """Check if user is subscribed to a specific notification category"""
        subscription_map = {
            "incident": self.subscribe_incident,
            "schema_change": self.subscribe_schema_change,
            "job_failure": self.subscribe_job_failure,
            "late_run": self.subscribe_late_run,
            "dbt_test_failure": self.subscribe_dbt_test_failure,
            "system": self.subscribe_system,
        }
        return subscription_map.get(category, True)  # Default to True for unknown categories
