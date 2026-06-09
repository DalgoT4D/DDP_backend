from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0163_dashboard_chat_metadata_schema_version_2"),
    ]

    operations = [
        migrations.AlterField(
            model_name="dashboardchatmetadataartifact",
            name="schema_version",
            field=models.PositiveIntegerField(default=3),
        ),
    ]
