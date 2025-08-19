"""Dashboard models for Dalgo platform"""

from enum import Enum
from django.db import models
from django.utils import timezone
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser


class DashboardType(str, Enum):
    """Dashboard type enum"""

    NATIVE = "native"
    SUPERSET = "superset"

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class DashboardComponentType(str, Enum):
    """Dashboard component types"""

    CHART = "chart"
    TEXT = "text"
    HEADING = "heading"

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class DashboardFilterType(str, Enum):
    """Dashboard filter types"""

    VALUE = "value"
    NUMERICAL = "numerical"
    DATETIME = "datetime"

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class Dashboard(models.Model):
    """Native dashboard configuration model"""

    id = models.BigAutoField(primary_key=True)
    title = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    dashboard_type = models.CharField(
        max_length=20, choices=DashboardType.choices(), default=DashboardType.NATIVE.value
    )

    # Grid configuration
    grid_columns = models.IntegerField(default=12)
    target_screen_size = models.CharField(
        max_length=20,
        choices=[("desktop", "Desktop"), ("tablet", "Tablet"), ("mobile", "Mobile"), ("a4", "A4")],
        default="desktop",
        help_text="Target screen size for dashboard design",
    )

    # Layout configuration stored as JSON
    layout_config = models.JSONField(default=list, help_text="Grid layout positions and sizes")

    # Components configuration
    components = models.JSONField(default=dict, help_text="Dashboard components configuration")

    # Filter layout configuration
    filter_layout = models.CharField(
        max_length=20,
        choices=[("vertical", "Vertical"), ("horizontal", "Horizontal")],
        default="vertical",
        help_text="Layout for dashboard filters - vertical sidebar or horizontal top bar",
    )

    # Publishing status
    is_published = models.BooleanField(default=False)
    published_at = models.DateTimeField(null=True, blank=True)

    # Metadata
    created_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE, db_column="created_by")
    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    last_modified_by = models.ForeignKey(
        OrgUser,
        on_delete=models.CASCADE,
        db_column="last_modified_by",
        null=True,
        related_name="dashboards_modified",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.title} ({self.dashboard_type})"

    def to_json(self):
        """Return JSON representation"""
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "dashboard_type": self.dashboard_type,
            "grid_columns": self.grid_columns,
            "target_screen_size": self.target_screen_size,
            "layout_config": self.layout_config,
            "components": self.components,
            "filter_layout": self.filter_layout,
            "is_published": self.is_published,
            "published_at": self.published_at.isoformat() if self.published_at else None,
            "created_by": self.created_by.user.email if self.created_by else None,
            "org_id": self.org.id,
            "last_modified_by": self.last_modified_by.user.email if self.last_modified_by else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    class Meta:
        db_table = "dashboard"
        ordering = ["-updated_at"]


class DashboardFilter(models.Model):
    """Dashboard filter configuration"""

    id = models.BigAutoField(primary_key=True)
    dashboard = models.ForeignKey(Dashboard, on_delete=models.CASCADE, related_name="filters")

    # Filter configuration
    name = models.CharField(max_length=255, help_text="Display name for the filter", default="")
    filter_type = models.CharField(max_length=20, choices=DashboardFilterType.choices())
    schema_name = models.CharField(max_length=255)
    table_name = models.CharField(max_length=255)
    column_name = models.CharField(max_length=255)

    # Filter settings
    settings = models.JSONField(default=dict, help_text="Filter-specific settings")

    # UI positioning
    order = models.IntegerField(default=0)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.dashboard.title} - {self.name or self.column_name} ({self.filter_type})"

    def to_json(self):
        """Return JSON representation"""
        return {
            "id": self.id,
            "dashboard_id": self.dashboard_id,
            "name": self.name,
            "filter_type": self.filter_type,
            "schema_name": self.schema_name,
            "table_name": self.table_name,
            "column_name": self.column_name,
            "settings": self.settings,
            "order": self.order,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    class Meta:
        db_table = "dashboard_filter"
        ordering = ["order"]


class DashboardLock(models.Model):
    """Locking implementation for Dashboard editing"""

    dashboard = models.OneToOneField(Dashboard, on_delete=models.CASCADE, related_name="lock")
    locked_at = models.DateTimeField(auto_now_add=True)
    locked_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
    lock_token = models.CharField(max_length=36, unique=True)
    expires_at = models.DateTimeField()

    def __str__(self):
        return f"Lock for {self.dashboard.title} by {self.locked_by.user.email}"

    def is_expired(self):
        """Check if lock has expired"""
        return timezone.now() > self.expires_at

    def to_json(self):
        """Return JSON representation"""
        return {
            "dashboard_id": self.dashboard_id,
            "locked_at": self.locked_at.isoformat() if self.locked_at else None,
            "locked_by": self.locked_by.user.email if self.locked_by else None,
            "lock_token": self.lock_token,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "is_expired": self.is_expired(),
        }

    class Meta:
        db_table = "dashboard_lock"
