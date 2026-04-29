# Rename snapshot_chart_id -> target_id on Comment, chart_id -> target_id on CommentReadStatus

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0158_metric_kpi"),
    ]

    operations = [
        # Drop unique_together before renaming
        migrations.AlterUniqueTogether(
            name="commentreadstatus",
            unique_together=set(),
        ),
        # Rename the columns (preserves data)
        migrations.RenameField(
            model_name="comment",
            old_name="snapshot_chart_id",
            new_name="target_id",
        ),
        migrations.RenameField(
            model_name="commentreadstatus",
            old_name="chart_id",
            new_name="target_id",
        ),
        # Restore unique_together with new field name
        migrations.AlterUniqueTogether(
            name="commentreadstatus",
            unique_together={("user", "snapshot", "target_type", "target_id")},
        ),
    ]
