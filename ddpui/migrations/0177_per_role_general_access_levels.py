# Replace the single (audience, level) general-access pair with independent
# per-role levels (analyst_level, member_level) on the 5 shareable models and
# the OrgPreferences defaults.
#
# Operation order matters: AddField runs BEFORE the RunPython data migration
# (which reads the OLD columns) BEFORE RemoveField. The reverse mapping is
# best-effort and lossy — mixed cases the old model can't express collapse
# to analysts_plus/edit (see REVERSE_PAIR).

from django.db import migrations, models

SHAREABLE_MODELS = ["Dashboard", "ReportSnapshot", "Metric", "KPI", "Alert"]

# (old_audience, old_level) -> (new_analyst_level, new_member_level)
FORWARD_PAIR = {
    ("private", "view"): ("none", "none"),
    ("private", "edit"): ("none", "none"),
    ("admins", "view"): ("none", "none"),
    ("admins", "edit"): ("none", "none"),
    ("analysts_plus", "view"): ("view", "none"),
    ("analysts_plus", "edit"): ("edit", "none"),
    ("all_users", "view"): ("view", "view"),
    ("all_users", "edit"): ("edit", "edit"),
}

# (new_analyst_level, new_member_level) -> (old_audience, old_level)
REVERSE_PAIR = {
    ("none", "none"): ("private", "view"),
    ("view", "none"): ("analysts_plus", "view"),
    ("edit", "none"): ("analysts_plus", "edit"),
    ("view", "view"): ("all_users", "view"),
    ("edit", "edit"): ("all_users", "edit"),
    # Lossy mixed cases (documented above): collapse to analysts_plus/edit.
    ("none", "view"): ("analysts_plus", "edit"),
    ("none", "edit"): ("analysts_plus", "edit"),
    ("view", "edit"): ("analysts_plus", "edit"),
    ("edit", "view"): ("analysts_plus", "edit"),
}


def _forward_pair(audience, level):
    """(analyst_level, member_level) for one old (audience, level) pair.
    An unrecognized/legacy audience value default-denies both roles rather
    than raising -- mirrors the resolver's "unknown -> None" philosophy."""
    return FORWARD_PAIR.get((audience, level), ("none", "none"))


def _reverse_pair(analyst_level, member_level):
    """(audience, level) best-effort inverse for one (analyst_level,
    member_level) pair. Falls back to the documented lossy default for any
    combination not in the table (defensive; the table covers every
    reachable 3x3 combination already)."""
    return REVERSE_PAIR.get((analyst_level, member_level), ("analysts_plus", "edit"))


def _migrate_shareable_models(apps, forward: bool) -> None:
    """Bulk-update every shareable resource's general-access columns via a
    handful of `.filter(...).update(...)` calls per (old, new) combination
    -- no per-row Python loop needed since the value space is tiny (8
    combinations forward, 9 reverse)."""
    pairs = FORWARD_PAIR.items() if forward else REVERSE_PAIR.items()
    for model_name in SHAREABLE_MODELS:
        model = apps.get_model("ddpui", model_name)
        for src_pair, dst_pair in pairs:
            if forward:
                audience, level = src_pair
                analyst_level, member_level = dst_pair
                model.objects.filter(general_audience=audience, general_level=level).update(
                    analyst_level=analyst_level, member_level=member_level
                )
            else:
                analyst_level, member_level = src_pair
                audience, level = dst_pair
                model.objects.filter(analyst_level=analyst_level, member_level=member_level).update(
                    general_audience=audience, general_level=level
                )


def _migrate_org_preferences(apps, forward: bool) -> None:
    model = apps.get_model("ddpui", "OrgPreferences")
    pairs = FORWARD_PAIR.items() if forward else REVERSE_PAIR.items()
    for src_pair, dst_pair in pairs:
        if forward:
            audience, level = src_pair
            analyst_level, member_level = dst_pair
            model.objects.filter(
                default_general_audience=audience, default_general_level=level
            ).update(default_analyst_level=analyst_level, default_member_level=member_level)
        else:
            analyst_level, member_level = src_pair
            audience, level = dst_pair
            model.objects.filter(
                default_analyst_level=analyst_level, default_member_level=member_level
            ).update(default_general_audience=audience, default_general_level=level)


def migrate_to_per_role_levels(apps, schema_editor):  # pylint: disable=unused-argument
    _migrate_shareable_models(apps, forward=True)
    _migrate_org_preferences(apps, forward=True)


def migrate_back_to_audience_level(apps, schema_editor):  # pylint: disable=unused-argument
    _migrate_shareable_models(apps, forward=False)
    _migrate_org_preferences(apps, forward=False)


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0176_notification_metadata"),
    ]

    operations = [
        # 1. Add the new columns (default "none") -- old columns still
        # present, so the data migration below can read from them.
        migrations.AddField(
            model_name="alert",
            name="analyst_level",
            field=models.CharField(
                choices=[("none", "None"), ("view", "View"), ("edit", "Edit")],
                default="none",
                max_length=5,
            ),
        ),
        migrations.AddField(
            model_name="alert",
            name="member_level",
            field=models.CharField(
                choices=[("none", "None"), ("view", "View"), ("edit", "Edit")],
                default="none",
                max_length=5,
            ),
        ),
        migrations.AddField(
            model_name="dashboard",
            name="analyst_level",
            field=models.CharField(
                choices=[("none", "None"), ("view", "View"), ("edit", "Edit")],
                default="none",
                max_length=5,
            ),
        ),
        migrations.AddField(
            model_name="dashboard",
            name="member_level",
            field=models.CharField(
                choices=[("none", "None"), ("view", "View"), ("edit", "Edit")],
                default="none",
                max_length=5,
            ),
        ),
        migrations.AddField(
            model_name="kpi",
            name="analyst_level",
            field=models.CharField(
                choices=[("none", "None"), ("view", "View"), ("edit", "Edit")],
                default="none",
                max_length=5,
            ),
        ),
        migrations.AddField(
            model_name="kpi",
            name="member_level",
            field=models.CharField(
                choices=[("none", "None"), ("view", "View"), ("edit", "Edit")],
                default="none",
                max_length=5,
            ),
        ),
        migrations.AddField(
            model_name="metric",
            name="analyst_level",
            field=models.CharField(
                choices=[("none", "None"), ("view", "View"), ("edit", "Edit")],
                default="none",
                max_length=5,
            ),
        ),
        migrations.AddField(
            model_name="metric",
            name="member_level",
            field=models.CharField(
                choices=[("none", "None"), ("view", "View"), ("edit", "Edit")],
                default="none",
                max_length=5,
            ),
        ),
        migrations.AddField(
            model_name="orgpreferences",
            name="default_analyst_level",
            field=models.CharField(
                choices=[("none", "None"), ("view", "View"), ("edit", "Edit")],
                default="none",
                max_length=5,
            ),
        ),
        migrations.AddField(
            model_name="orgpreferences",
            name="default_member_level",
            field=models.CharField(
                choices=[("none", "None"), ("view", "View"), ("edit", "Edit")],
                default="none",
                max_length=5,
            ),
        ),
        migrations.AddField(
            model_name="reportsnapshot",
            name="analyst_level",
            field=models.CharField(
                choices=[("none", "None"), ("view", "View"), ("edit", "Edit")],
                default="none",
                max_length=5,
            ),
        ),
        migrations.AddField(
            model_name="reportsnapshot",
            name="member_level",
            field=models.CharField(
                choices=[("none", "None"), ("view", "View"), ("edit", "Edit")],
                default="none",
                max_length=5,
            ),
        ),
        # 2. Populate the new columns from the old ones (both directions).
        migrations.RunPython(migrate_to_per_role_levels, migrate_back_to_audience_level),
        # 3. Drop the old columns.
        migrations.RemoveField(
            model_name="alert",
            name="general_audience",
        ),
        migrations.RemoveField(
            model_name="alert",
            name="general_level",
        ),
        migrations.RemoveField(
            model_name="dashboard",
            name="general_audience",
        ),
        migrations.RemoveField(
            model_name="dashboard",
            name="general_level",
        ),
        migrations.RemoveField(
            model_name="kpi",
            name="general_audience",
        ),
        migrations.RemoveField(
            model_name="kpi",
            name="general_level",
        ),
        migrations.RemoveField(
            model_name="metric",
            name="general_audience",
        ),
        migrations.RemoveField(
            model_name="metric",
            name="general_level",
        ),
        migrations.RemoveField(
            model_name="orgpreferences",
            name="default_general_audience",
        ),
        migrations.RemoveField(
            model_name="orgpreferences",
            name="default_general_level",
        ),
        migrations.RemoveField(
            model_name="reportsnapshot",
            name="general_audience",
        ),
        migrations.RemoveField(
            model_name="reportsnapshot",
            name="general_level",
        ),
    ]
