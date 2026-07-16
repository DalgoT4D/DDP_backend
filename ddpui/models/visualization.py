"""Chart/Visualization models for Dalgo platform"""

from django.core.exceptions import ValidationError
from django.db import models
from django.contrib.postgres.fields import ArrayField
from ddpui.models.general_access import AccessLevel
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

CHART_TYPE_CHOICES = [
    ("bar", "Bar Chart"),
    ("pie", "Pie Chart"),
    ("line", "Line Chart"),
    ("number", "Number Chart"),
    ("map", "Map Chart"),
]

# Deprecated: computation_type is no longer used in chart logic
# Kept for backwards compatibility with existing database records
COMPUTATION_TYPE_CHOICES = [
    ("raw", "Raw Data"),
    ("aggregated", "Aggregated Data"),
]

AGGREGATE_FUNC_CHOICES = [
    ("sum", "SUM"),
    ("avg", "AVG"),
    ("count", "COUNT"),
    ("min", "MIN"),
    ("max", "MAX"),
    ("count_distinct", "COUNT DISTINCT"),
]


class Chart(models.Model):
    """Chart configuration model"""

    id = models.BigAutoField(primary_key=True)
    title = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    chart_type = models.CharField(max_length=20, choices=CHART_TYPE_CHOICES)

    # Deprecated: computation_type is no longer used in chart logic
    # All charts now use aggregated (metrics-based) behavior
    # Kept for backwards compatibility with existing database records
    computation_type = models.CharField(
        max_length=20, choices=COMPUTATION_TYPE_CHOICES, default="aggregated"
    )

    # Data source configuration
    schema_name = models.CharField(max_length=255)
    table_name = models.CharField(max_length=255)

    extra_config = models.JSONField(
        default=dict, help_text="Create chart form config including customizations"
    )

    # General access (org-wide default sharing) — Layer 1 of Resource Sharing
    # (v1.1: charts join the sharing model). Same per-role declaration as
    # Dashboard, with ONE deliberate difference: ``analyst_level`` defaults to
    # EDIT, not NONE — the behavior-preserving value (v1.1 decision #3: all
    # analysts could see/edit all charts before charts had access fields, so
    # any row created without explicit levels keeps today's behavior; the
    # service create path overrides with the org's defaults).
    # ``member_level`` is PINNED to "none" in v1.1 (decision #2: Member chart
    # sharing is deferred — Members see charts only inline inside shared
    # dashboards/reports); ``clean()`` and the sharing API both reject any
    # other value.
    analyst_level = models.CharField(
        max_length=5, choices=AccessLevel.choices, default=AccessLevel.EDIT
    )
    member_level = models.CharField(
        max_length=5, choices=AccessLevel.choices, default=AccessLevel.NONE
    )

    # Metadata
    created_by = models.ForeignKey(
        OrgUser, on_delete=models.SET_NULL, null=True, db_column="created_by"
    )
    owner = models.ForeignKey(
        OrgUser, on_delete=models.SET_NULL, null=True, related_name="owned_%(class)ss"
    )
    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    last_modified_by = models.ForeignKey(
        OrgUser,
        on_delete=models.CASCADE,
        db_column="last_modified_by",
        null=True,
        related_name="last_modified_by",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def clean(self):
        """v1.1 member-pin: Member chart sharing is deferred, so
        ``member_level`` should stay "none". The REAL enforcement lives in
        the sharing API (``shareable_types`` registry ``member_sharing=False``)
        and the resolver (which gives Member viewers nothing on charts even
        if a stray value slips in). This is only a best-effort backstop —
        Django doesn't call ``clean()`` on plain ``save()``/``update()``, so
        write paths must not rely on it."""
        super().clean()
        if self.member_level != AccessLevel.NONE:
            raise ValidationError(
                {"member_level": "charts cannot be shared with Members yet (v1.1)"}
            )

    def __str__(self):
        return f"{self.title} ({self.chart_type})"
