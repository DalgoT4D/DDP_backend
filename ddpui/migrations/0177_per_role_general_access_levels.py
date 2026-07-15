# D1 (permission-model rework): replace the single (audience, level) general
# access pair with two independent per-role levels -- analyst_level and
# member_level -- at both layers: the 5 shareable resource models (Dashboard,
# ReportSnapshot, Metric, KPI, Alert) and the OrgPreferences org-wide default.
#
# The old audience+level pair could never express "Analyst=Edit, Member=View"
# -- an "analysts_plus" audience gave everyone at or above that tier the SAME
# single level. Two independent per-role fields make that storable.
#
# Operation order matters: AddField (new columns, default "none") runs BEFORE
# the RunPython data migration (which needs the OLD columns still present to
# read from) BEFORE RemoveField (old columns). Reversing re-adds the old
# columns, re-populates them from the new ones (which are still present at
# that point), then drops the new columns.
#
# Forward mapping (old -> new), applied identically to both layers:
#   private            -> (none, none)
#   admins              -> (none, none)
#   analysts_plus(X)    -> (X, none)
#   all_users(X)        -> (X, X)
#
# Reverse mapping (best-effort, LOSSY -- documented per D1 brief):
#   (none, none)  -> private
#   (X, none)     -> analysts_plus X            [X != none]
#   (X, X)        -> all_users X                [X != none]
#   anything else (member has a level the analyst doesn't share, e.g.
#   (edit, view), (view, edit), or a level on member_level with
#   analyst_level="none") -> analysts_plus edit -- the old model has no way
#   to express "Member sees something Analyst doesn't" or "Analyst and
#   Member both have access but at different levels", so this collapses
#   every such case to the same lossy fallback.

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
