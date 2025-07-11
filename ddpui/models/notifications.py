from django.db import models
from ddpui.models.org_user import OrgUser


class NotificationCategory(models.TextChoices):
    """Choices for notification categories"""

    INCIDENT = "incident", "Incident"
    SCHEMA_CHANGE = "schema_change", "Schema Change"
    JOB_FAILURE = "job_failure", "Job Failure"
    LATE_RUNS = "late_runs", "Late Runs"
    DBT_TEST_FAILURE = "dbt_test_failure", "DBT Test Failure"


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
        default=NotificationCategory.INCIDENT,
        help_text="Category of the notification",
    )
    dismissed_by = models.ManyToManyField(
        OrgUser,
        blank=True,
        related_name="dismissed_notifications",
        help_text="Users who have dismissed this urgent notification",
    )


class NotificationRecipient(models.Model):
    """Model to store notification recipients and their read status"""

    notification = models.ForeignKey(
        Notification, on_delete=models.CASCADE, related_name="notifications_received"
    )
    recipient = models.ForeignKey(OrgUser, on_delete=models.CASCADE, related_name="recipients")
    read_status = models.BooleanField(default=False)
    task_id = models.TextField()
