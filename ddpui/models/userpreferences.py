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

    # Category-based subscription preferences
    subscribe_incident_notifications = models.BooleanField(
        default=True, help_text="Subscribe to incident notifications"
    )
    subscribe_schema_change_notifications = models.BooleanField(
        default=True, help_text="Subscribe to schema change notifications"
    )
    subscribe_job_failure_notifications = models.BooleanField(
        default=True, help_text="Subscribe to job failure notifications"
    )
    subscribe_late_runs_notifications = models.BooleanField(
        default=True, help_text="Subscribe to late runs notifications"
    )
    subscribe_dbt_test_failure_notifications = models.BooleanField(
        default=True, help_text="Subscribe to DBT test failure notifications"
    )

    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(default=timezone.now)

    def to_json(self) -> dict:
        """Return a dict representation of the model"""
        return {
            "enable_email_notifications": self.enable_email_notifications,
            "disclaimer_shown": self.disclaimer_shown,
            "subscribe_incident_notifications": self.subscribe_incident_notifications,
            "subscribe_schema_change_notifications": self.subscribe_schema_change_notifications,
            "subscribe_job_failure_notifications": self.subscribe_job_failure_notifications,
            "subscribe_late_runs_notifications": self.subscribe_late_runs_notifications,
            "subscribe_dbt_test_failure_notifications": self.subscribe_dbt_test_failure_notifications,
        }

    def is_subscribed_to_category(self, category: str) -> bool:
        """Check if user is subscribed to a specific notification category"""
        category_mapping = {
            "incident": self.subscribe_incident_notifications,
            "schema_change": self.subscribe_schema_change_notifications,
            "job_failure": self.subscribe_job_failure_notifications,
            "late_runs": self.subscribe_late_runs_notifications,
            "dbt_test_failure": self.subscribe_dbt_test_failure_notifications,
        }
        return category_mapping.get(category, True)
