"""v1.2 pilot: give each grant a real Permission FK (column ``permission_id``)
alongside the varchar level, and backfill existing rows from the
rtype+level -> slug map. The varchar stays until every rtype reads the FK.
"""

from django.db import migrations, models
import django.db.models.deletion

# Frozen copy of permission_map.RTYPE_LEVEL_SLUG at migration time —
# migrations must not drift with later edits to the live map.
RTYPE_LEVEL_SLUG = {
    ("dashboard", "view"): "can_view_dashboards",
    ("dashboard", "edit"): "can_edit_dashboards",
    ("chart", "view"): "can_view_charts",
    ("chart", "edit"): "can_edit_charts",
    ("alert", "view"): "can_view_alerts",
    ("alert", "edit"): "can_edit_alerts",
    ("metric", "view"): "can_view_metrics",
    ("metric", "edit"): "can_edit_metrics",
    ("kpi", "view"): "can_view_kpis",
    ("kpi", "edit"): "can_edit_kpis",
}


def backfill_permission_fk(apps, schema_editor):
    Permission = apps.get_model("ddpui", "Permission")
    ResourceShare = apps.get_model("ddpui", "ResourceShare")
    slug_to_id = dict(Permission.objects.values_list("slug", "id"))
    for (rtype, level), slug in RTYPE_LEVEL_SLUG.items():
        permission_id = slug_to_id.get(slug)
        if permission_id is not None:
            ResourceShare.objects.filter(resource_type=rtype, permission=level).update(
                granted_permission_id=permission_id
            )


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
        migrations.RunPython(backfill_permission_fk, migrations.RunPython.noop),
    ]
