"""Comment models for Dalgo platform"""

from enum import Enum

from django.db import models
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.report import ReportSnapshot


class CommentTargetType(str, Enum):
    """Application-level enum for comment target types"""

    CHART = "chart"
    SUMMARY = "summary"
    KPI = "kpi"


class Comment(models.Model):
    """Comment on a chart, KPI, or executive summary within a report snapshot"""

    id = models.BigAutoField(primary_key=True)

    target_type = models.CharField(max_length=20)
    snapshot = models.ForeignKey(
        ReportSnapshot,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="comments",
    )
    # ID of the target entity within frozen_chart_configs (chart ID, KPI ID, etc.)
    # Null for summary-level comments.
    target_id = models.IntegerField(
        null=True,
        blank=True,
        help_text="Entity ID within frozen_chart_configs (not a FK)",
    )

    content = models.TextField(help_text="Comment text, max 5000 chars enforced at schema level")

    mentioned_emails = models.JSONField(
        default=list,
        blank=True,
        help_text="List of email addresses mentioned in this comment",
    )

    # Author + multi-tenancy
    author = models.ForeignKey(OrgUser, on_delete=models.CASCADE, related_name="comments_authored")
    org = models.ForeignKey(Org, on_delete=models.CASCADE)

    is_deleted = models.BooleanField(default=False)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Comment {self.id} ({self.target_type})"

    class Meta:
        db_table = "comment"
        ordering = ["created_at"]
        indexes = [
            models.Index(fields=["org", "created_at"]),
            models.Index(fields=["snapshot"]),
        ]


class CommentReadStatus(models.Model):
    """Tracks per-user read state for each comment target on a report.

    last_read_at is updated when the user closes the comment popover.
    Comments with created_at > last_read_at are considered "new".
    """

    id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(
        OrgUser, on_delete=models.CASCADE, related_name="comment_read_statuses"
    )
    snapshot = models.ForeignKey(
        ReportSnapshot, on_delete=models.CASCADE, related_name="comment_read_statuses"
    )
    target_type = models.CharField(max_length=20)
    target_id = models.IntegerField(
        null=True,
        blank=True,
        help_text="Entity ID for target-level read status (chart ID, KPI ID, etc.)",
    )
    last_read_at = models.DateTimeField()

    def __str__(self):
        return f"ReadStatus user={self.user_id} snapshot={self.snapshot_id} {self.target_type}"

    class Meta:
        db_table = "comment_read_status"
        unique_together = [("user", "snapshot", "target_type", "target_id")]
        indexes = [
            models.Index(fields=["user", "snapshot"]),
        ]
