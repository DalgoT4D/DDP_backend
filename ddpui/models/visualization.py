"""Chart/Visualization models for Dalgo platform"""
from django.db import models
from django.contrib.postgres.fields import ArrayField
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

CHART_TYPE_CHOICES = [
    ("bar", "Bar Chart"),
    ("pie", "Pie Chart"),
    ("line", "Line Chart"),
]

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
    computation_type = models.CharField(
        max_length=20, choices=COMPUTATION_TYPE_CHOICES, default="raw"
    )

    # Data source configuration
    schema_name = models.CharField(max_length=255)
    table_name = models.CharField(max_length=255)

    # Configuration JSON field containing all column mappings and customizations
    # Structure:
    # {
    #   "x_axis_column": "string (for raw)",
    #   "y_axis_column": "string (for raw)",
    #   "dimension_column": "string (for aggregated)",
    #   "aggregate_column": "string (for aggregated)",
    #   "aggregate_function": "sum|avg|count|min|max (for aggregated)",
    #   "extra_dimension_column": "string (optional)",
    #   "customizations": {
    #     "orientation": "horizontal|vertical",
    #     "stacked": boolean,
    #     "showDataLabels": boolean,
    #     "xAxisTitle": "string",
    #     "yAxisTitle": "string",
    #     "donut": boolean (for pie),
    #     "smooth": boolean (for line)
    #   }
    # }
    config = models.JSONField(
        default=dict, help_text="Chart configuration including columns and customizations"
    )

    # Metadata
    user = models.ForeignKey(OrgUser, on_delete=models.CASCADE, related_name="charts")
    org = models.ForeignKey(Org, on_delete=models.CASCADE, related_name="charts")
    is_favorite = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "charts"
        ordering = ["-updated_at"]
        indexes = [
            models.Index(fields=["org", "user"]),
            models.Index(fields=["chart_type"]),
            models.Index(fields=["created_at"]),
        ]

    def __str__(self):
        return f"{self.title} ({self.chart_type})"

    def get_query_config(self):
        """Return query configuration based on computation type"""
        if self.computation_type == "raw":
            return {
                "schema": self.schema_name,
                "table": self.table_name,
                "x_axis": self.config.get("x_axis_column"),
                "y_axis": self.config.get("y_axis_column"),
                "extra_dimension": self.config.get("extra_dimension_column"),
            }
        else:  # aggregated
            return {
                "schema": self.schema_name,
                "table": self.table_name,
                "dimension": self.config.get("dimension_column"),
                "aggregate_col": self.config.get("aggregate_column"),
                "aggregate_func": self.config.get("aggregate_function"),
                "extra_dimension": self.config.get("extra_dimension_column"),
            }


class ChartSnapshot(models.Model):
    """Snapshot of chart data for caching and versioning"""

    id = models.BigAutoField(primary_key=True)
    chart = models.ForeignKey(Chart, on_delete=models.CASCADE, related_name="snapshots")
    data = models.JSONField(help_text="Cached chart data")
    echarts_config = models.JSONField(help_text="Generated ECharts configuration")
    query_hash = models.CharField(
        max_length=64, help_text="Hash of the query for cache invalidation"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField(help_text="When this snapshot expires")

    class Meta:
        db_table = "chart_snapshots"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["chart", "created_at"]),
            models.Index(fields=["query_hash"]),
            models.Index(fields=["expires_at"]),
        ]

    def __str__(self):
        return f"Snapshot for {self.chart.title} at {self.created_at}"

    def is_expired(self):
        """Check if this snapshot has expired"""
        from django.utils import timezone

        return timezone.now() > self.expires_at
