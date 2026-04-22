"""
Batch 1 — Metrics & KPIs overhaul.

Splits today's conflated `MetricDefinition` into two concepts:
  - `Metric` — the reusable, saved aggregation (primitive)
  - `KPI`    — the tracked layer (Metric + target + RAG + trend + annotations)

Replaces `MetricAnnotation` (single-per-period) with `KPIEntry`
(append-only timeline with snapshots).

Rewires `Alert`:
  - `Alert.metric` (FK MetricDefinition) is removed
  - adds `Alert.kpi` (FK KPI, nullable) — where `metric_rag_level` is used
  - adds `Alert.metric` (FK Metric, nullable) — for threshold alerts (Batch 3 wires them)

Clean cut: branch is not released; existing rows in the old tables are dropped.
"""

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("ddpui", "0162_metric_rag_alerts_remove_placeholders"),
    ]

    operations = [
        # 1. Break Alert's FK to MetricDefinition first so MetricDefinition can be dropped.
        migrations.RemoveField(model_name="alert", name="metric"),
        # 2. Drop annotations (FK into MetricDefinition) and the old MetricDefinition.
        migrations.DeleteModel(name="MetricAnnotation"),
        migrations.DeleteModel(name="MetricDefinition"),
        # 3. Create the new Metric primitive.
        migrations.CreateModel(
            name="Metric",
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
                ("description", models.TextField(blank=True, default="")),
                ("tags", models.JSONField(default=list)),
                ("schema_name", models.CharField(max_length=255)),
                ("table_name", models.CharField(max_length=255)),
                ("time_column", models.CharField(max_length=255, null=True, blank=True)),
                (
                    "default_time_grain",
                    models.CharField(
                        max_length=20,
                        default="month",
                        choices=[
                            ("month", "Month"),
                            ("quarter", "Quarter"),
                            ("year", "Year"),
                        ],
                    ),
                ),
                (
                    "creation_mode",
                    models.CharField(
                        max_length=10,
                        default="simple",
                        choices=[("simple", "Simple"), ("sql", "SQL")],
                    ),
                ),
                # Simple mode: list of {id, agg, column}
                ("simple_terms", models.JSONField(default=list)),
                # Simple mode: expression referencing term IDs, e.g. "t1 - t2"
                ("simple_formula", models.TextField(blank=True, default="")),
                # SQL mode: raw scalar-returning expression
                ("sql_expression", models.TextField(blank=True, default="")),
                # Filters (all baked-in in v1)
                ("filters", models.JSONField(default=list)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "org",
                    models.ForeignKey(
                        on_delete=models.deletion.CASCADE,
                        related_name="metrics",
                        to="ddpui.org",
                    ),
                ),
                (
                    "created_by",
                    models.ForeignKey(
                        on_delete=models.deletion.CASCADE,
                        related_name="created_metrics",
                        to="ddpui.orguser",
                    ),
                ),
            ],
            options={"db_table": "metric", "ordering": ["name"]},
        ),
        # 4. Create the KPI tracked layer.
        migrations.CreateModel(
            name="KPI",
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
                ("target_value", models.FloatField(null=True, blank=True)),
                (
                    "direction",
                    models.CharField(
                        max_length=20,
                        default="increase",
                        choices=[("increase", "Increase"), ("decrease", "Decrease")],
                    ),
                ),
                ("amber_threshold_pct", models.FloatField(default=80)),
                ("green_threshold_pct", models.FloatField(default=100)),
                (
                    "trend_grain",
                    models.CharField(
                        max_length=20,
                        default="month",
                        choices=[
                            ("month", "Month"),
                            ("quarter", "Quarter"),
                            ("year", "Year"),
                        ],
                    ),
                ),
                ("trend_periods", models.IntegerField(default=12)),
                ("metric_type_tag", models.CharField(max_length=100, blank=True, default="")),
                ("program_tag", models.CharField(max_length=255, blank=True, default="")),
                ("tags", models.JSONField(default=list)),
                ("display_order", models.IntegerField(default=0)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "org",
                    models.ForeignKey(
                        on_delete=models.deletion.CASCADE,
                        related_name="kpis",
                        to="ddpui.org",
                    ),
                ),
                (
                    "metric",
                    models.ForeignKey(
                        on_delete=models.deletion.PROTECT,
                        related_name="kpis",
                        to="ddpui.metric",
                    ),
                ),
                (
                    "created_by",
                    models.ForeignKey(
                        on_delete=models.deletion.CASCADE,
                        related_name="created_kpis",
                        to="ddpui.orguser",
                    ),
                ),
            ],
            options={"db_table": "kpi", "ordering": ["display_order", "-created_at"]},
        ),
        # 5. Create the KPI annotations timeline.
        migrations.CreateModel(
            name="KPIEntry",
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
                (
                    "entry_type",
                    models.CharField(
                        max_length=20,
                        choices=[("comment", "Comment"), ("quote", "Beneficiary Quote")],
                    ),
                ),
                ("period_key", models.CharField(max_length=20)),
                ("content", models.TextField()),
                ("attribution", models.CharField(max_length=255, blank=True, default="")),
                ("snapshot_value", models.FloatField(null=True, blank=True)),
                ("snapshot_rag", models.CharField(max_length=10, blank=True, default="")),
                ("snapshot_achievement_pct", models.FloatField(null=True, blank=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "kpi",
                    models.ForeignKey(
                        on_delete=models.deletion.CASCADE,
                        related_name="entries",
                        to="ddpui.kpi",
                    ),
                ),
                (
                    "created_by",
                    models.ForeignKey(
                        on_delete=models.deletion.CASCADE,
                        related_name="created_kpi_entries",
                        to="ddpui.orguser",
                    ),
                ),
            ],
            options={"db_table": "kpi_entry", "ordering": ["-period_key", "-created_at"]},
        ),
        # 6. Rewire Alert — RAG alerts now point at a KPI; threshold alerts will
        #    point at a Metric (Batch 3 wires the threshold path).
        migrations.AddField(
            model_name="alert",
            name="kpi",
            field=models.ForeignKey(
                null=True,
                blank=True,
                on_delete=models.deletion.CASCADE,
                related_name="alerts",
                to="ddpui.kpi",
            ),
        ),
        migrations.AddField(
            model_name="alert",
            name="metric",
            field=models.ForeignKey(
                null=True,
                blank=True,
                on_delete=models.deletion.CASCADE,
                related_name="alerts",
                to="ddpui.metric",
            ),
        ),
    ]
