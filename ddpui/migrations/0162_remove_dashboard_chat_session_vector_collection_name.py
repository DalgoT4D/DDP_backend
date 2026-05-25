from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0161_dashboard_chat_metadata_artifacts"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="dashboardchatsession",
            name="vector_collection_name",
        ),
    ]
