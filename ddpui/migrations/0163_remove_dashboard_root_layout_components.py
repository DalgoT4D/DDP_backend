"""
Schema migration: drop layout_config and components columns from the dashboard table.

All dashboard data now lives inside the tabs JSON array. The root-level columns
were kept for rollback safety after the initial tabs migration; this PR removes them.
"""

from django.db import migrations


class Migration(migrations.Migration):
    """Drop deprecated root-level layout_config and components columns from dashboard table."""

    dependencies = [
        ("ddpui", "0162_kpi_last_modified_by_metric_last_modified_by"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="dashboard",
            name="layout_config",
        ),
        migrations.RemoveField(
            model_name="dashboard",
            name="components",
        ),
    ]
