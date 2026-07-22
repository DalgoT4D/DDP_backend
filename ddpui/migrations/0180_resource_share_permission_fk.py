"""v1.2 pilot: give each grant a real Permission FK (column ``permission_id``)
alongside the varchar level. No backfill: prod first sees this table at the
same deploy (created empty), and rows written since the pilot get the FK from
``ResourceShare.save()``; any older null-FK row is covered by the pool
builder's varchar fallback.
"""

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0179_chart_general_access_levels"),
    ]

    operations = [
        migrations.AddField(
            model_name="resourceshare",
            name="granted_permission",
            field=models.ForeignKey(
                db_column="permission_id",
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="+",
                to="ddpui.permission",
            ),
        ),
    ]
