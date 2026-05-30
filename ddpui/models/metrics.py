"""Metric definition and annotation models for My Metrics feature"""

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

TIME_GRAIN_CHOICES = [
    ("month", "Month"),
    ("quarter", "Quarter"),
    ("year", "Year"),
]

DIRECTION_CHOICES = [
    ("increase", "Increase"),
    ("decrease", "Decrease"),
]


class MetricDefinition(models.Model):
    """
    Stores per-metric configuration.
    Data source points to a warehouse table; value is computed live via aggregation.
    Target + RAG thresholds are stored here; trend config controls sparkline.
    """

    org = models.ForeignKey(Org, on_delete=models.CASCADE, related_name="metrics")

    # Display
    name = models.CharField(max_length=255)

    # Data source (same pattern as Chart)
    schema_name = models.CharField(max_length=255)
    table_name = models.CharField(max_length=255)
    column = models.CharField(max_length=255)  # value column
    aggregation = models.CharField(max_length=20, choices=AGGREGATION_CHOICES)

    # Time (optional — skip trend if null)
    time_column = models.CharField(max_length=255, null=True, blank=True)
    time_grain = models.CharField(max_length=20, choices=TIME_GRAIN_CHOICES, default="month")

    # Target & RAG
    direction = models.CharField(max_length=20, choices=DIRECTION_CHOICES, default="increase")
    target_value = models.FloatField(null=True, blank=True)
    amber_threshold_pct = models.FloatField(default=80)  # >= 80% of target = amber
    green_threshold_pct = models.FloatField(default=100)  # >= 100% = green
    # below amber = red

    # Tags (flexible, not enforced hierarchy)
    program_tag = models.CharField(max_length=255, blank=True, default="")
    metric_type_tag = models.CharField(max_length=100, blank=True, default="")
    # e.g. "Output", "Outcome", "Input", "Impact" — or custom

    # Trend config
    trend_periods = models.IntegerField(default=12)  # how many periods to show

    # Ordering
    display_order = models.IntegerField(default=0)

    # Metadata
    created_by = models.ForeignKey(
        OrgUser, on_delete=models.CASCADE, related_name="created_metrics"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["display_order", "name"]

    def __str__(self):
        return f"{self.name} ({self.aggregation}({self.column}))"


class MetricAnnotation(models.Model):
    """
    Stores rationale text and beneficiary quote per metric per period.
    Period key format: "2025-03" (month), "2025-Q1" (quarter), "2025" (year).
    """

    metric = models.ForeignKey(
        MetricDefinition, on_delete=models.CASCADE, related_name="annotations"
    )
    period_key = models.CharField(max_length=20)  # "2025-03", "2025-Q1", "2025"

    rationale = models.TextField(blank=True, default="")
    quote_text = models.TextField(blank=True, default="")
    quote_attribution = models.CharField(max_length=255, blank=True, default="")

    created_by = models.ForeignKey(
        OrgUser, on_delete=models.CASCADE, related_name="created_annotations"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("metric", "period_key")

    def __str__(self):
        return f"Annotation for {self.metric.name} @ {self.period_key}"
