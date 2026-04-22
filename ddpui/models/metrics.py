"""Metric, KPI, and KPIEntry models — Batch 1 rewrite.

Concepts:
  - Metric  = reusable aggregation primitive (column+agg, or Calculated SQL)
  - KPI     = tracked layer wrapping a Metric (target + RAG + trend + annotations)
  - KPIEntry = append-only timeline entry (comment / beneficiary quote + snapshot)
"""

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

CREATION_MODE_CHOICES = [
    ("simple", "Simple"),
    ("sql", "SQL"),
]

ENTRY_TYPE_CHOICES = [
    ("comment", "Comment"),
    ("quote", "Beneficiary Quote"),
]


class Metric(models.Model):
    """Reusable, saved aggregation — the primitive consumed by charts / KPIs / alerts.

    `simple_terms` shape: list of ``{"id": "t1", "agg": "avg", "column": "col_a"}``.
    `simple_formula`: expression string referencing term IDs, e.g. ``"t1 - t2"``
    or ``"t1 / t2 * 100"``. Single-term metrics use ``simple_formula = "t1"``.

    `sql_expression`: raw SQL scalar expression placed inside ``SELECT (...)``.
    May itself be an aggregate (e.g. ``SUM(CASE WHEN status='active' THEN 1 END)``).

    `filters`: list of ``{"column", "operator", "value"}``, AND-joined, baked-in.
    """

    org = models.ForeignKey(Org, on_delete=models.CASCADE, related_name="metrics")

    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, default="")
    tags = models.JSONField(default=list)

    schema_name = models.CharField(max_length=255)
    table_name = models.CharField(max_length=255)
    time_column = models.CharField(max_length=255, null=True, blank=True)
    default_time_grain = models.CharField(
        max_length=20, choices=TIME_GRAIN_CHOICES, default="month"
    )

    creation_mode = models.CharField(
        max_length=10, choices=CREATION_MODE_CHOICES, default="simple"
    )
    simple_terms = models.JSONField(default=list)
    simple_formula = models.TextField(blank=True, default="")
    sql_expression = models.TextField(blank=True, default="")
    filters = models.JSONField(default=list)

    created_by = models.ForeignKey(
        OrgUser, on_delete=models.CASCADE, related_name="created_metrics"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "metric"
        ordering = ["name"]

    def __str__(self):
        return f"Metric<{self.id} {self.name}>"


class KPI(models.Model):
    """Tracked layer on top of a Metric — carries target, RAG, trend config, tags."""

    org = models.ForeignKey(Org, on_delete=models.CASCADE, related_name="kpis")
    metric = models.ForeignKey(Metric, on_delete=models.PROTECT, related_name="kpis")

    target_value = models.FloatField(null=True, blank=True)
    direction = models.CharField(
        max_length=20, choices=DIRECTION_CHOICES, default="increase"
    )
    amber_threshold_pct = models.FloatField(default=80)
    green_threshold_pct = models.FloatField(default=100)

    trend_grain = models.CharField(
        max_length=20, choices=TIME_GRAIN_CHOICES, default="month"
    )
    trend_periods = models.IntegerField(default=12)

    metric_type_tag = models.CharField(max_length=100, blank=True, default="")
    program_tag = models.CharField(max_length=255, blank=True, default="")
    tags = models.JSONField(default=list)

    display_order = models.IntegerField(default=0)

    created_by = models.ForeignKey(
        OrgUser, on_delete=models.CASCADE, related_name="created_kpis"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "kpi"
        ordering = ["display_order", "-created_at"]

    def __str__(self):
        return f"KPI<{self.id} {self.metric.name if self.metric_id else '?'}>"

    @property
    def name(self) -> str:
        """KPIs inherit their display name from the underlying Metric."""
        return self.metric.name if self.metric_id else ""


class KPIEntry(models.Model):
    """Append-only timeline entry attached to a KPI.

    Snapshots the KPI's current value + RAG at creation time so the history
    is stable even when the underlying data drifts later.
    """

    kpi = models.ForeignKey(KPI, on_delete=models.CASCADE, related_name="entries")
    entry_type = models.CharField(max_length=20, choices=ENTRY_TYPE_CHOICES)
    period_key = models.CharField(max_length=20)

    content = models.TextField()
    attribution = models.CharField(max_length=255, blank=True, default="")

    snapshot_value = models.FloatField(null=True, blank=True)
    snapshot_rag = models.CharField(max_length=10, blank=True, default="")
    snapshot_achievement_pct = models.FloatField(null=True, blank=True)

    created_by = models.ForeignKey(
        OrgUser, on_delete=models.CASCADE, related_name="created_kpi_entries"
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "kpi_entry"
        ordering = ["-period_key", "-created_at"]

    def __str__(self):
        return f"KPIEntry<{self.id} kpi={self.kpi_id} {self.period_key}>"
