# Generated by Django 4.1.7 on 2023-12-05 08:28

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0031_task_orgtask_datafloworgtask"),
    ]

    operations = [
        migrations.CreateModel(
            name="OrgPrefectBlockv1",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("block_type", models.CharField(max_length=25)),
                ("block_id", models.CharField(max_length=36, unique=True)),
                ("block_name", models.CharField(max_length=100, unique=True)),
                (
                    "org",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE, to="ddpui.org"
                    ),
                ),
            ],
        ),
    ]
