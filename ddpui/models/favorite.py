"""Shared favorite model for charts, dashboards, and other favoritable resources.

Mirrors Apache Superset's own `favstar` table design (superset/models/core.py):
one shared table keyed by (user, resource_type, resource_id) instead of a
separate table per resource type.
"""

from enum import Enum
from django.db import models
from ddpui.models.org_user import OrgUser


class FavoriteResourceType(str, Enum):
    """Resource types that can be favorited"""

    CHART = "chart"
    DASHBOARD = "dashboard"

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class Favorite(models.Model):
    """Tracks which org users have favorited which resources. Favoriting is
    personal — one user's favorite has no effect on any other user."""

    org_user = models.ForeignKey(OrgUser, on_delete=models.CASCADE, related_name="favorites")
    resource_type = models.CharField(max_length=20, choices=FavoriteResourceType.choices())
    resource_id = models.BigIntegerField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "favorite"
        constraints = [
            models.UniqueConstraint(
                fields=["org_user", "resource_type", "resource_id"], name="unique_favorite"
            )
        ]
        indexes = [
            models.Index(fields=["resource_type", "resource_id"]),
        ]

    def __str__(self):
        return f"{self.org_user.user.email} favorited {self.resource_type} {self.resource_id}"
