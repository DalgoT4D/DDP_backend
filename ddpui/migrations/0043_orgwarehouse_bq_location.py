# Generated by Django 4.1.7 on 2024-02-12 08:21

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0042_orgtask_generated_by_task_is_system"),
    ]

    operations = [
        migrations.AddField(
            model_name="orgwarehouse",
            name="bq_location",
            field=models.CharField(max_length=100, null=True),
        ),
    ]
