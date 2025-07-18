from django.db import models
from django.utils import timezone
import json
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser


class Chart(models.Model):
    """Chart model for storing chart configurations and metadata"""

    CHART_TYPES = [
        ("bar", "Bar Chart"),
        ("line", "Line Chart"),
        ("pie", "Pie Chart"),
        ("area", "Area Chart"),
        ("scatter", "Scatter Plot"),
        ("funnel", "Funnel Chart"),
        ("heatmap", "Heatmap"),
        ("radar", "Radar Chart"),
        ("number", "Number Chart"),
        ("map", "Map Chart"),
    ]

    CHART_LIBRARIES = [
        ("echarts", "ECharts"),
        ("nivo", "Nivo"),
        ("recharts", "Recharts"),
    ]

    COMPUTATION_TYPES = [
        ("raw", "Raw Data"),
        ("aggregated", "Aggregated Data"),
    ]

    org = models.ForeignKey(Org, on_delete=models.CASCADE, related_name="charts")
    created_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE, related_name="created_charts")

    title = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)

    chart_type = models.CharField(max_length=20, choices=CHART_LIBRARIES)

    # Data source configuration
    schema_name = models.CharField(max_length=255)
    table_name = models.CharField(max_length=255)

    # Chart configuration as JSON
    config = models.JSONField(default=dict)

    # Metadata
    is_public = models.BooleanField(default=False)
    is_favorite = models.BooleanField(default=False)

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "charts"
        ordering = ["-updated_at"]
        indexes = [
            models.Index(fields=["org", "created_by"]),
            models.Index(fields=["schema_name", "table_name"]),
            models.Index(fields=["chart_type"]),
        ]

    def __str__(self):
        return f"{self.title} ({self.chart_type})"

    @property
    def chart_config_type(self):
        """Get the chart configuration type (bar, line, pie, etc.)"""
        return self.config.get("chartType", "bar")

    @property
    def computation_type(self):
        """Get the computation type (raw, aggregated)"""
        return self.config.get("computation_type", "raw")

    @property
    def x_axis(self):
        """Get the X-axis column"""
        return self.config.get("xAxis")

    @property
    def y_axis(self):
        """Get the Y-axis column"""
        return self.config.get("yAxis")

    @property
    def dimensions(self):
        """Get chart dimensions"""
        return self.config.get("dimensions", [])

    @property
    def aggregate_function(self):
        """Get aggregation function"""
        return self.config.get("aggregate_func")

    @property
    def aggregate_column(self):
        """Get aggregation column"""
        return self.config.get("aggregate_col")

    @property
    def aggregate_column_alias(self):
        """Get aggregation column alias"""
        return self.config.get("aggregate_col_alias")

    @property
    def dimension_column(self):
        """Get dimension column"""
        return self.config.get("dimension_col")

    def to_dict(self):
        """Convert chart to dictionary representation"""
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "chart_type": self.chart_type,
            "schema_name": self.schema_name,
            "table": self.table_name,
            "config": self.config,
            "is_public": self.is_public,
            "is_favorite": self.is_favorite,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "created_by": {
                "id": self.created_by.id,
                "user": {
                    "id": self.created_by.user.id,
                    "email": self.created_by.user.email,
                    "first_name": self.created_by.user.first_name,
                    "last_name": self.created_by.user.last_name,
                },
            }
            if self.created_by
            else None,
        }

    @classmethod
    def create_chart(
        cls,
        org,
        created_by,
        title,
        description,
        chart_type,
        schema_name,
        table_name,
        config,
        is_public=False,
    ):
        """Create a new chart with validation"""
        chart = cls.objects.create(
            org=org,
            created_by=created_by,
            title=title,
            description=description,
            chart_type=chart_type,
            schema_name=schema_name,
            table_name=table_name,
            config=config,
            is_public=is_public,
        )
        return chart

    def update_chart(
        self,
        title=None,
        description=None,
        chart_type=None,
        schema_name=None,
        table_name=None,
        config=None,
        is_public=None,
    ):
        """Update chart with new values"""
        if title is not None:
            self.title = title
        if description is not None:
            self.description = description
        if chart_type is not None:
            self.chart_type = chart_type
        if schema_name is not None:
            self.schema_name = schema_name
        if table_name is not None:
            self.table_name = table_name
        if config is not None:
            self.config = config
        if is_public is not None:
            self.is_public = is_public

        self.updated_at = timezone.now()
        self.save()
        return self

    def can_edit(self, user):
        """Check if user can edit this chart"""
        return self.created_by == user or user.role.slug == "account_manager"

    def can_view(self, user):
        """Check if user can view this chart"""
        return self.is_public or self.created_by == user or user.role.slug == "account_manager"


class ChartSnapshot(models.Model):
    """Store chart data snapshots for performance optimization"""

    chart = models.ForeignKey(
        Chart, on_delete=models.CASCADE, related_name="snapshots", null=True, blank=True
    )
    data_hash = models.CharField(max_length=64, unique=True)
    chart_config = models.JSONField()
    raw_data = models.JSONField()

    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()

    class Meta:
        db_table = "chart_snapshots"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["chart", "data_hash"]),
            models.Index(fields=["expires_at"]),
        ]

    def __str__(self):
        return f"Snapshot for {self.chart.title}"

    def is_expired(self):
        """Check if snapshot is expired"""
        return timezone.now() > self.expires_at

    @classmethod
    def clean_expired(cls):
        """Remove expired snapshots"""
        cls.objects.filter(expires_at__lt=timezone.now()).delete()
