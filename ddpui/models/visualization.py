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

    extra_config = models.JSONField(
        default=dict, help_text="Create chart form config including customizations"
    )

    # Metadata
    created_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE, db_column="created_by")
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

    def __str__(self):
        return f"{self.title} ({self.chart_type})"
