# Generated migration to remove organization_name and website fields from ddpui_org_settings

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0155_populate_org_settings"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="orgsettings",
            name="organization_name",
        ),
        migrations.RemoveField(
            model_name="orgsettings",
            name="website",
        ),
    ]
