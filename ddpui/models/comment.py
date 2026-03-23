"""Comment models for Dalgo platform"""

from django.db import models
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.report import ReportSnapshot


class CommentTargetType:
    """Application-level enum for comment target types"""

    CHART = "chart"
    SUMMARY = "summary"

    ALL = [CHART, SUMMARY]


class Comment(models.Model):
    """Comment on a chart or executive summary within a report snapshot"""

    id = models.BigAutoField(primary_key=True)

    target_type = models.CharField(max_length=20)
    snapshot = models.ForeignKey(
        ReportSnapshot,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="comments",
    )
    # For comments targeting a specific chart within a snapshot.
    # This is an integer referencing a key in frozen_chart_configs, NOT a FK.
    snapshot_chart_id = models.IntegerField(
        null=True,
        blank=True,
        help_text="Chart ID within frozen_chart_configs (not a FK)",
    )

    content = models.TextField(help_text="Comment text, max 5000 chars enforced at schema level")

    mentioned_emails = models.JSONField(
        default=list,
        blank=True,
        help_text="List of email addresses mentioned in this comment",
    )

    # Author + multi-tenancy
    author = models.ForeignKey(
        OrgUser, on_delete=models.CASCADE, related_name="comments_authored"
    )
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
    chart_id = models.IntegerField(
        null=True,
        blank=True,
        help_text="Chart ID within frozen_chart_configs (for chart-level read status)",
    )
    last_read_at = models.DateTimeField()

    def __str__(self):
        return f"ReadStatus user={self.user_id} snapshot={self.snapshot_id} {self.target_type}"

    class Meta:
        db_table = "comment_read_status"
        unique_together = [("user", "snapshot", "target_type", "chart_id")]
        indexes = [
            models.Index(fields=["user", "snapshot"]),
        ]
