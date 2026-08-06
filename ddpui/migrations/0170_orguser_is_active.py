# Migration B (admin portal M4): per-org active flag on OrgUser.
# Adds OrgUser.is_active and backfills it from the user's CURRENT User.is_active,
# per features/admin-portal/v1/plan.md §4.1 / open-question #2: existing
# globally-disabled users start disabled in every org; per-org divergence
# happens going forward. Reverse is a no-op (dropping the column loses the flag).

from django.db import migrations, models


def backfill_is_active(apps, schema_editor):
    """set OrgUser.is_active from the linked User.is_active for existing rows"""
    OrgUser = apps.get_model("ddpui", "OrgUser")
    for orguser in OrgUser.objects.select_related("user").iterator():
        # a globally-disabled user starts disabled in all their orgs (safe default)
        orguser.is_active = orguser.user.is_active
        orguser.save(update_fields=["is_active"])


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0169_org_is_active"),
    ]

    operations = [
        migrations.AddField(
            model_name="orguser",
            name="is_active",
            field=models.BooleanField(
                default=True,
                help_text=(
                    "per-(user, org) active flag; distinct from User.is_active. "
                    "False deactivates this user in THIS org only (blocked at "
                    "permission-load); their membership of other orgs is unaffected. "
                    "See features/admin-portal/v1/plan.md §4.1."
                ),
            ),
        ),
        migrations.RunPython(backfill_is_active, migrations.RunPython.noop),
    ]
