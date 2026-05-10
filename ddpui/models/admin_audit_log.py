from django.db import models
from django.utils import timezone

from ddpui.models.org import Org


class AdminAuditLog(models.Model):
    """Stores admin audit logs for organization actions."""

    ACTION_CHOICES = (
        ("organization_updated", "organization_updated"),
    )

    org = models.ForeignKey(
        Org,
        on_delete=models.CASCADE,
    )

    action = models.CharField(
        max_length=100,
        choices=ACTION_CHOICES,
    )

    old_data = models.JSONField()
    new_data = models.JSONField()

    created_at = models.DateTimeField(
        auto_created=True,
        default=timezone.now,
    )

    def __str__(self) -> str:
        return (
            f"AdminAuditLog["
            f"{self.org.slug}|{self.action}"
            f"]"
        )
