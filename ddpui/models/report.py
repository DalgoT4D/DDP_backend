"""Report models for Dalgo platform"""

from enum import Enum
from django.db import models
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser


class SnapshotStatus(str, Enum):
    """Snapshot status enum"""

    GENERATED = "generated"
    VIEWED = "viewed"
    ARCHIVED = "archived"

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class ReportSnapshot(models.Model):
    """
    Immutable snapshot of a dashboard at a specific date range.
    Stores frozen config — data is queried live with date filter.

    No FK to Dashboard or Chart. Once frozen, the snapshot is fully
    self-contained. Deleting dashboards or charts does NOT affect snapshots.
    """

    id = models.BigAutoField(primary_key=True)
    title = models.CharField(max_length=255, help_text="User-provided title for this snapshot")

    # Which datetime column the date range applies to
    date_column = models.JSONField(
        default=dict,
        help_text="Selected datetime column: {schema_name, table_name, column_name}",
    )

    # Date range this snapshot covers
    period_start = models.DateField(
        null=True,
        blank=True,
        help_text="Start of reporting period (inclusive). NULL = no lower bound.",
    )
    period_end = models.DateField(help_text="End of reporting period (inclusive)")

    # --- FROZEN DATA (2 layers) ---

    # Layer 1: Dashboard layout, structure & filters
    # Contains: title, description, grid_columns, target_screen_size,
    #           layout_config, components, filter_layout, filters
    frozen_dashboard = models.JSONField(
        default=dict,
        help_text="Frozen dashboard config + filters at snapshot time",
    )

    # Layer 2: Full chart configs keyed by chart_id
    # {str(chart_id): {id, title, description, chart_type, schema_name, table_name, extra_config}}
    # CRITICAL: ensures snapshots survive chart deletion or editing
    frozen_chart_configs = models.JSONField(
        default=dict,
        help_text="Frozen chart configs keyed by chart_id",
    )

    # User-editable summary (the ONLY mutable field)
    summary = models.TextField(
        blank=True,
        null=True,
        help_text="Executive summary or notes displayed above the dashboard",
    )

    # Status tracking
    status = models.CharField(
        max_length=20,
        choices=SnapshotStatus.choices(),
        default=SnapshotStatus.GENERATED.value,
    )

    # Public sharing (same pattern as Dashboard)
    is_public = models.BooleanField(default=False)
    public_share_token = models.CharField(
        max_length=64, unique=True, null=True, blank=True, db_index=True
    )
    public_shared_at = models.DateTimeField(null=True, blank=True)
    public_disabled_at = models.DateTimeField(null=True, blank=True)
    public_access_count = models.IntegerField(default=0)
    last_public_accessed = models.DateTimeField(null=True, blank=True)

    # Metadata
    created_by = models.ForeignKey(
        OrgUser,
        on_delete=models.SET_NULL,
        null=True,
        help_text="User who created this snapshot",
    )
    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        start = self.period_start or "unbounded"
        return f"{self.title} ({start} - {self.period_end})"

    class Meta:
        db_table = "report_snapshot"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["org", "created_at"]),
        ]
