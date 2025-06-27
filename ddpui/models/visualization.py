import uuid
from django.db import models
from django.utils import timezone
from enum import Enum
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser


class AggregationFunction(models.TextChoices):
    COUNT = "count", "Count"
    COUNT_DISTINCT = "count_distinct", "Count Distinct"
    SUM = "sum", "Sum"
    MIN = "min", "Min"
    MAX = "max", "Max"
    AVERAGE = "average", "Average"


class Chart(models.Model):
    """Docstring"""

    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    title = models.TextField(max_length=256)
    description = models.TextField(null=True)
    chart_type = models.TextField(max_length=256)
    schema = models.TextField(max_length=256)
    table = models.TextField(max_length=256)
    config = models.JSONField()
    metric = models.ForeignKey("Metric", on_delete=models.SET_NULL, null=True)
    metadata = models.JSONField(null=True)
    created_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return f"Chart[{self.title}|{self.chart_type}]"


class Metric(models.Model):
    """Model for storing metric definitions with aggregation functions and dimensions"""

    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    name = models.CharField(max_length=256)
    description = models.TextField(null=True, blank=True)

    # Database location
    schema = models.CharField(max_length=256)
    table = models.CharField(max_length=256)

    # Temporal column (optional, at most one)
    temporal_column = models.CharField(max_length=256, null=True)

    # Dimension columns (stored as JSON array)
    dimension_columns = models.JSONField(default=list)

    # Measure configuration
    aggregation_function = models.CharField(
        max_length=20, null=True, choices=AggregationFunction.choices
    )
    aggregation_column = models.CharField(max_length=256, null=True)

    metadata = models.JSONField(null=True)

    # Metadata
    created_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def clean(self):
        from django.core.exceptions import ValidationError

        # Validate that aggregation_column is provided for non-count functions
        if self.aggregation_function != AggregationFunction.COUNT and not self.aggregation_column:
            raise ValidationError(
                "Aggregation column is required for aggregation functions other than count"
            )

        # Validate that aggregation_column is not provided for count function
        if self.aggregation_function == AggregationFunction.COUNT and self.aggregation_column:
            raise ValidationError("Aggregation column should not be provided for count function")

    def save(self, *args, **kwargs):
        self.clean()
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"Metric[{self.name}|{self.aggregation_function}]"
