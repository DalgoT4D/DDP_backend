# Reverses 0169_org_is_active / 0170_orguser_is_active: the admin-portal
# activate/deactivate capability (Option B) is being removed entirely, not just
# hidden. The admin API/UI toggle was already removed (see the "remove org/user
# activate-deactivate" commits); this drops the columns themselves plus the
# auth.py permission-load enforcement that read them. There is no successor
# field — per-org/per-user suspension is gone, not renamed.
#
# Irreversible in practice: reversing this migration re-adds the columns but
# not the deactivated/reactivated history they used to hold.

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0171_invitation_invited_in_org"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="org",
            name="is_active",
        ),
        migrations.RemoveField(
            model_name="orguser",
            name="is_active",
        ),
    ]
