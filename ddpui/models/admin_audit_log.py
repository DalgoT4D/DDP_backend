from django.db import models
from django.contrib.auth.models import User

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

    performed_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
    )

    action = models.CharField(
        max_length=100,
        choices=ACTION_CHOICES,
    )

    old_data = models.JSONField()
    new_data = models.JSONField()

    def __str__(self):
        performed_by = (
            self.performed_by.username
            if self.performed_by
            else "unknown"
        )

        return (
            f"{self.action} | "
            f"Org: {self.org.name} | "
            f"By: {performed_by}"
        )
