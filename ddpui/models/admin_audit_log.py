from django.db import models

from ddpui.models.org import Org


class AdminAuditLog(models.Model):
    """Stores admin audit logs for sensitive admin actions."""

    ACTION_CHOICES = [
        ("organization_updated", "organization_updated"),
    ]

    created_at = models.DateTimeField(auto_now_add=True)

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

    def __str__(self):
        return f"{self.action} | Org: {self.org.name}"
