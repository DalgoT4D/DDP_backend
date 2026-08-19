# Adds Invitation.invited_in_org: a platform admin inviting cross-org is not a member
# of the target org, so invited_by.org is not the target org. Backfilled from
# invited_by.org for existing rows.

from django.db import migrations, models
import django.db.models.deletion


# Invitation is a pre-existing, live table (since migration 0001) — real pending
# invites already exist independent of this branch, and without this backfill
# they'd all get invited_in_org=NULL. accept_invitation_v1 has a fallback for that
# (invited_in_org or invited_by.org), but this sets the real value permanently
# instead of relying on the fallback indefinitely. invited_by.org is provably
# correct for every existing row: the cross-org case this field exists for is only
# reachable through the new admin-portal invite path this branch introduces.
def backfill_invited_in_org(apps, schema_editor):
    """copy invited_by.org onto invited_in_org for existing invitations"""
    Invitation = apps.get_model("ddpui", "Invitation")
    for invitation in Invitation.objects.select_related("invited_by").iterator():
        # invited_by is non-null (CASCADE FK); its org is where the invite pointed
        invitation.invited_in_org_id = invitation.invited_by.org_id
        invitation.save(update_fields=["invited_in_org"])


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0168_alter_alert_created_by_alter_chart_created_by_and_more"),
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
