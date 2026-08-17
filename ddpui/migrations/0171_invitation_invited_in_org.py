# Adds Invitation.invited_in_org: a platform admin inviting cross-org is not a member
# of the target org, so invited_by.org is not the target org. Backfilled from
# invited_by.org for existing rows.

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
                    "the org this invite grants membership of, when it differs from "
                    "invited_by.org (e.g. a platform admin inviting cross-org)."
                ),
            ),
        ),
        migrations.RunPython(backfill_invited_in_org, migrations.RunPython.noop),
    ]
