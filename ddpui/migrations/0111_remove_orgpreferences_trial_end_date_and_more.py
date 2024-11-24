# Generated by Django 4.2 on 2024-11-15 10:52

import ddpui.models.org
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0110_userpreferences_discord_webhook_and_more"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="orgpreferences",
            name="trial_end_date",
        ),
        migrations.RemoveField(
            model_name="orgpreferences",
            name="trial_start_date",
        ),
        migrations.AlterField(
            model_name="org",
            name="type",
            field=models.CharField(
                choices=[("subscription", "SUBSCRIPTION"), ("trial", "TRIAL"), ("demo", "DEMO")],
                default=ddpui.models.org.OrgType["SUBSCRIPTION"],
                max_length=50,
            ),
        ),
        migrations.CreateModel(
            name="OrgPlans",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("base_plan", models.CharField(default=None, max_length=255, null=True)),
                ("superset_included", models.BooleanField(default=False, null=True)),
                (
                    "subscription_duration",
                    models.CharField(default=None, max_length=255, null=True),
                ),
                ("features", models.JSONField(default=None, null=True)),
                ("start_date", models.DateTimeField(blank=True, default=None, null=True)),
                ("end_date", models.DateTimeField(blank=True, default=None, null=True)),
                ("can_upgrade_plan", models.BooleanField(default=False)),
                ("created_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("updated_at", models.DateTimeField(default=django.utils.timezone.now)),
                (
                    "org",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="org_plans",
                        to="ddpui.org",
                    ),
                ),
            ],
        ),
    ]