# Generated by Django 4.1.7 on 2024-01-30 10:44

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0040_tasklock_locking_dataflow"),
    ]

    operations = [
        migrations.AddField(
            model_name="orgtask",
            name="parameters",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
