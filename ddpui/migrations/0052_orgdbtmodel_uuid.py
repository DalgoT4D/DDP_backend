# Generated by Django 4.1.7 on 2024-02-26 05:43

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0051_orgdbtmodel_type"),
    ]

    operations = [
        migrations.AddField(
            model_name="orgdbtmodel",
            name="uuid",
            field=models.UUIDField(editable=False, null=True, unique=True),
        ),
    ]
