"""Metric and KPI models for Dalgo platform"""

from django.db import models
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser


AGGREGATION_CHOICES = [
    ("sum", "SUM"),
    ("avg", "AVG"),
    ("count", "COUNT"),
    ("min", "MIN"),
    ("max", "MAX"),
    ("count_distinct", "COUNT DISTINCT"),
]

DIRECTION_CHOICES = [
    ("increase", "Increase"),
    ("decrease", "Decrease"),
]

TIME_GRAIN_CHOICES = [
    ("daily", "Daily"),
    ("weekly", "Weekly"),
    ("monthly", "Monthly"),
    ("quarterly", "Quarterly"),
    ("yearly", "Yearly"),
]

METRIC_TYPE_TAG_CHOICES = [
    ("input", "Input"),
    ("output", "Output"),
    ("outcome", "Outcome"),
    ("impact", "Impact"),
]


class Metric(models.Model):
    """Reusable metric definition: a named aggregation over a warehouse table."""

    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)

    # Data source (same pattern as Chart)
    schema_name = models.CharField(max_length=255)
    table_name = models.CharField(max_length=255)

    # Option A: Simple — column + aggregation (dropdown picks)
    column = models.CharField(max_length=255, null=True, blank=True)
    aggregation = models.CharField(
        max_length=30, choices=AGGREGATION_CHOICES, null=True, blank=True
    )

    # Option B: Expression — free-text expression
    # e.g. "SUM(col_a - col_b) / COUNT(DISTINCT id)"
    # Validated on save by executing a test query against the warehouse
    column_expression = models.TextField(null=True, blank=True)

    # Org scoping + ownership (same pattern as Chart)
    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    created_by = models.ForeignKey(
        OrgUser, on_delete=models.CASCADE, related_name="metrics_created"
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at"]
        constraints = [
            models.UniqueConstraint(fields=["org", "name"], name="unique_metric_name_per_org")
        ]

    def __str__(self):
        if self.column_expression:
            return f"{self.name} (expression)"
        return f"{self.name} ({self.aggregation}({self.column}))"


class KPI(models.Model):
    """KPI: a Metric promoted with target, direction, RAG thresholds, and trend config."""

    id = models.BigAutoField(primary_key=True)
    metric = models.ForeignKey(Metric, on_delete=models.PROTECT, related_name="kpis")

    # Display
    name = models.CharField(max_length=255)

    # Target & direction
    target_value = models.FloatField(null=True, blank=True)
    direction = models.CharField(max_length=10, choices=DIRECTION_CHOICES)

    # RAG thresholds (percentage of target)
    green_threshold_pct = models.FloatField(default=100.0)
    amber_threshold_pct = models.FloatField(default=80.0)

    # Time configuration
    time_grain = models.CharField(max_length=20, choices=TIME_GRAIN_CHOICES)
    time_dimension_column = models.CharField(max_length=255, null=True, blank=True)
    trend_periods = models.IntegerField(default=12)

    # Tags
    metric_type_tag = models.CharField(
        max_length=20, choices=METRIC_TYPE_TAG_CHOICES, null=True, blank=True
    )
    program_tags = models.JSONField(default=list, blank=True)

    # Display order on KPI page
    display_order = models.IntegerField(default=0)

    # Org scoping + ownership
    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    created_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE, related_name="kpis_created")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["display_order", "-updated_at"]

    def __str__(self):
        return f"{self.name} (KPI)"
