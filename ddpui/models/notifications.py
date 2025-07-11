from django.db import models
from ddpui.models.org_user import OrgUser


class NotificationCategory(models.TextChoices):
    """Enum for notification categories"""

    INCIDENT = "incident", "Incident Notifications"
    SCHEMA_CHANGE = "schema_change", "Schema Changes"
    JOB_FAILURE = "job_failure", "Job Failures"
    LATE_RUN = "late_run", "Late Runs"
    DBT_TEST_FAILURE = "dbt_test_failure", "dbt Test Failures"
    SYSTEM = "system", "System Notifications"


class UserNotificationPreferences(models.Model):
    """Model to store user preferences for notification categories"""

    user = models.OneToOneField(
        OrgUser, on_delete=models.CASCADE, related_name="notification_preferences"
    )

    # Category subscription preferences (default to True for all)
    incident_notifications = models.BooleanField(default=True)
    schema_change_notifications = models.BooleanField(default=True)
    job_failure_notifications = models.BooleanField(default=True)
    late_run_notifications = models.BooleanField(default=True)
    dbt_test_failure_notifications = models.BooleanField(default=True)
    system_notifications = models.BooleanField(default=True)

    # Email preferences
    email_notifications_enabled = models.BooleanField(default=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "User Notification Preferences"
        verbose_name_plural = "User Notification Preferences"

    def is_subscribed_to_category(self, category: str) -> bool:
        """Check if user is subscribed to a specific notification category"""
        category_mapping = {
            NotificationCategory.INCIDENT: self.incident_notifications,
            NotificationCategory.SCHEMA_CHANGE: self.schema_change_notifications,
            NotificationCategory.JOB_FAILURE: self.job_failure_notifications,
            NotificationCategory.LATE_RUN: self.late_run_notifications,
            NotificationCategory.DBT_TEST_FAILURE: self.dbt_test_failure_notifications,
            NotificationCategory.SYSTEM: self.system_notifications,
        }
        return category_mapping.get(category, True)  # Default to True if category not found


class Notification(models.Model):
    """Model to store notifications for users"""

    author = models.EmailField()
    message = models.TextField()
    email_subject = models.TextField()
    timestamp = models.DateTimeField(auto_now_add=True)
    urgent = models.BooleanField(default=False)
    scheduled_time = models.DateTimeField(null=True, blank=True)
    sent_time = models.DateTimeField(null=True, blank=True)
    category = models.CharField(
        max_length=20,
        choices=NotificationCategory.choices,
        default=NotificationCategory.SYSTEM,
        help_text="Category of the notification",
    )


class NotificationRecipient(models.Model):
    """Model to store notification recipients and their read status"""

    notification = models.ForeignKey(
        Notification, on_delete=models.CASCADE, related_name="notifications_received"
    )
    recipient = models.ForeignKey(OrgUser, on_delete=models.CASCADE, related_name="recipients")
    read_status = models.BooleanField(default=False)
    task_id = models.TextField()
