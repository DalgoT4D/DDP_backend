"""Migration for MetricDefinition and MetricAnnotation models"""

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("ddpui", "0152_reportsnapshot"),
    ]

    operations = [
        migrations.CreateModel(
            name="MetricDefinition",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.CharField(max_length=255)),
                ("schema_name", models.CharField(max_length=255)),
                ("table_name", models.CharField(max_length=255)),
                ("column", models.CharField(max_length=255)),
                (
                    "aggregation",
                    models.CharField(
                        max_length=20,
                        choices=[
                            ("sum", "SUM"),
                            ("avg", "AVG"),
                            ("count", "COUNT"),
                            ("min", "MIN"),
                            ("max", "MAX"),
                            ("count_distinct", "COUNT DISTINCT"),
                        ],
                    ),
                ),
                (
                    "time_column",
                    models.CharField(max_length=255, null=True, blank=True),
                ),
                (
                    "time_grain",
                    models.CharField(
                        max_length=20,
                        choices=[
                            ("month", "Month"),
                            ("quarter", "Quarter"),
                            ("year", "Year"),
                        ],
                        default="month",
                    ),
                ),
                ("target_value", models.FloatField(null=True, blank=True)),
                ("amber_threshold_pct", models.FloatField(default=80)),
                ("green_threshold_pct", models.FloatField(default=100)),
                (
                    "program_tag",
                    models.CharField(max_length=255, blank=True, default=""),
                ),
                (
                    "metric_type_tag",
                    models.CharField(max_length=100, blank=True, default=""),
                ),
                ("trend_periods", models.IntegerField(default=12)),
                ("display_order", models.IntegerField(default=0)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "org",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="metrics",
                        to="ddpui.org",
                    ),
                ),
                (
                    "created_by",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="created_metrics",
                        to="ddpui.orguser",
                    ),
                ),
            ],
            options={
                "ordering": ["display_order", "name"],
            },
        ),
        migrations.CreateModel(
            name="MetricAnnotation",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("period_key", models.CharField(max_length=20)),
                ("rationale", models.TextField(blank=True, default="")),
                ("quote_text", models.TextField(blank=True, default="")),
                (
                    "quote_attribution",
                    models.CharField(max_length=255, blank=True, default=""),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "metric",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="annotations",
                        to="ddpui.metricdefinition",
                    ),
                ),
                (
                    "created_by",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="created_annotations",
                        to="ddpui.orguser",
                    ),
                ),
            ],
            options={
                "unique_together": {("metric", "period_key")},
            },
        ),
    ]
