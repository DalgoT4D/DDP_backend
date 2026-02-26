# Custom migration: merge frozen_config + frozen_filters into frozen_dashboard

from django.db import migrations, models


def merge_frozen_columns(apps, schema_editor):
    """Merge frozen_config + frozen_filters into frozen_dashboard for existing rows."""
    ReportSnapshot = apps.get_model("ddpui", "ReportSnapshot")
    for snapshot in ReportSnapshot.objects.all():
        frozen_dashboard = dict(snapshot.frozen_config or {})
        frozen_dashboard["filters"] = snapshot.frozen_filters or []
        snapshot.frozen_dashboard = frozen_dashboard
        snapshot.save(update_fields=["frozen_dashboard"])


def reverse_merge(apps, schema_editor):
    """Split frozen_dashboard back into frozen_config + frozen_filters."""
    ReportSnapshot = apps.get_model("ddpui", "ReportSnapshot")
    for snapshot in ReportSnapshot.objects.all():
        dashboard = dict(snapshot.frozen_dashboard or {})
        filters = dashboard.pop("filters", [])
        snapshot.frozen_config = dashboard
        snapshot.frozen_filters = filters
        snapshot.save(update_fields=["frozen_config", "frozen_filters"])


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0151_reportsnapshot_and_more"),
    ]

    operations = [
        # Step 1: Add frozen_dashboard column
        migrations.AddField(
            model_name="reportsnapshot",
            name="frozen_dashboard",
            field=models.JSONField(
                default=dict,
                help_text="Frozen dashboard config + filters at snapshot time",
            ),
        ),
        # Step 2: Migrate data from frozen_config + frozen_filters → frozen_dashboard
        migrations.RunPython(merge_frozen_columns, reverse_merge),
        # Step 3: Remove old columns
        migrations.RemoveField(
            model_name="reportsnapshot",
            name="frozen_config",
        ),
        migrations.RemoveField(
            model_name="reportsnapshot",
            name="frozen_filters",
        ),
    ]
