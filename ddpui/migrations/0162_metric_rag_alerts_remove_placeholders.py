from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("ddpui", "0161_drop_legacy_alert_cron_columns"),
    ]

    operations = [
        migrations.AddField(
            model_name="alert",
            name="metric_rag_level",
            field=models.CharField(blank=True, max_length=10, null=True),
        ),
        migrations.RemoveField(
            model_name="alert",
            name="message_placeholders",
        ),
    ]
