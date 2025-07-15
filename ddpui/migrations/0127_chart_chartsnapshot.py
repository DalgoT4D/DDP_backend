# Generated migration for chart models

from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0126_merge_20250703_0642"),
    ]

    operations = [
        migrations.CreateModel(
            name="Chart",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("title", models.CharField(max_length=255)),
                ("description", models.TextField(blank=True, null=True)),
                (
                    "chart_type",
                    models.CharField(
                        choices=[
                            ("echarts", "ECharts"),
                            ("nivo", "Nivo"),
                            ("recharts", "Recharts"),
                        ],
                        max_length=20,
                    ),
                ),
                ("schema_name", models.CharField(max_length=255)),
                ("table_name", models.CharField(max_length=255)),
                ("config", models.JSONField(default=dict)),
                ("is_public", models.BooleanField(default=False)),
                ("is_favorite", models.BooleanField(default=False)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "created_by",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="created_charts",
                        to="ddpui.orguser",
                    ),
                ),
                (
                    "org",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="charts",
                        to="ddpui.org",
                    ),
                ),
            ],
            options={
                "db_table": "charts",
                "ordering": ["-updated_at"],
            },
        ),
        migrations.CreateModel(
            name="ChartSnapshot",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("data_hash", models.CharField(max_length=64, unique=True)),
                ("chart_config", models.JSONField()),
                ("raw_data", models.JSONField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("expires_at", models.DateTimeField()),
                (
                    "chart",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="snapshots",
                        to="ddpui.chart",
                    ),
                ),
            ],
            options={
                "db_table": "chart_snapshots",
                "ordering": ["-created_at"],
            },
        ),
        migrations.AddIndex(
            model_name="chart",
            index=models.Index(fields=["org", "created_by"], name="charts_org_id_10f8b4_idx"),
        ),
        migrations.AddIndex(
            model_name="chart",
            index=models.Index(
                fields=["schema_name", "table_name"], name="charts_schema_name_9f8b4e_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="chart",
            index=models.Index(fields=["chart_type"], name="charts_chart_type_7f8b4d_idx"),
        ),
        migrations.AddIndex(
            model_name="chartsnapshot",
            index=models.Index(
                fields=["chart", "data_hash"], name="chart_snapshots_chart_id_8f8b4c_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="chartsnapshot",
            index=models.Index(fields=["expires_at"], name="chart_snapshots_expires_at_6f8b4b_idx"),
        ),
    ]
