# Generated for KPI v1.1 — Number formatting & prefix/suffix
# Adds `extra_config` JSONField to KPI. Backfills existing rows with {} via the
# default=dict; no separate data-migration step needed.

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0165_org_logo"),
    ]

    operations = [
        migrations.AddField(
            model_name="kpi",
            name="extra_config",
            field=models.JSONField(default=dict),
        ),
    ]
