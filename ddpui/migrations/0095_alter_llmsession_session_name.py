# Generated by Django 4.2 on 2024-09-03 12:22

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0094_merge_20240903_0610"),
    ]

    operations = [
        migrations.AlterField(
            model_name="llmsession",
            name="session_name",
            field=models.CharField(max_length=500, null=True),
        ),
    ]