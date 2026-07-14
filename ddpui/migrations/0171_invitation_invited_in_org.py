# Migration C (admin portal M4): explicit target-org on Invitation.
# Adds Invitation.invited_in_org (nullable FK -> Org) and backfills it from
# invited_by.org for every existing row, so pre-migration pending invites keep
# resolving to exactly the same org they did before. New admin-portal invites
# set this explicitly to the target org (which may differ from invited_by.org
# when a platform admin invites cross-org). accept/cancel read this field and
# fall back to invited_by.org when null. See features/admin-portal/v1/plan.md §4.4.

from django.db import migrations, models
import django.db.models.deletion


def backfill_invited_in_org(apps, schema_editor):
    """copy invited_by.org onto invited_in_org for existing invitations"""
    Invitation = apps.get_model("ddpui", "Invitation")
    for invitation in Invitation.objects.select_related("invited_by").iterator():
        # invited_by is non-null (CASCADE FK); its org is where the invite pointed
        invitation.invited_in_org_id = invitation.invited_by.org_id
        invitation.save(update_fields=["invited_in_org"])


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0170_orguser_is_active"),
    ]

    operations = [
        migrations.AddField(
            model_name="invitation",
            name="invited_in_org",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="invitations",
                to="ddpui.org",
                help_text=(
                    "the org this invite grants membership of. Explicit because a "
                    "platform admin inviting cross-org is not a member of the target "
                    "org, so invited_by.org is NOT the target org. Nullable for "
                    "backfill; old rows fall back to invited_by.org. See "
                    "features/admin-portal/v1/plan.md §4.4."
                ),
            ),
        ),
        migrations.RunPython(backfill_invited_in_org, migrations.RunPython.noop),
    ]
