# Generated migration to rename org_settings table to ddpui_org_settings

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0153_remove_aichatlog_ai_chat_log_org_id_466f09_idx_and_more"),
    ]

    operations = [
        migrations.AlterModelTable(
            name="orgsettings",
            table="ddpui_org_settings",
        ),
    ]
