from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0162_remove_dashboard_chat_session_vector_collection_name"),
    ]

    operations = [
        migrations.AlterField(
            model_name="dashboardchatmetadataartifact",
            name="schema_version",
            field=models.PositiveIntegerField(default=2),
        ),
    ]
