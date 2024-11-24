# Generated by Django 4.2 on 2024-11-06 15:41

from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0104_remove_userpreferences_discord_webhook_and_more"),
    ]

    operations = [
        migrations.CreateModel(
            name="OrgPreferences",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("trial_start_date", models.DateTimeField(blank=True, default=None, null=True)),
                ("trial_end_date", models.DateTimeField(blank=True, default=None, null=True)),
                ("llm_optin", models.BooleanField(default=False)),
                ("llm_optin_date", models.DateTimeField(blank=True, null=True)),
                ("enable_discord_notifications", models.BooleanField(default=False)),
                ("discord_webhook", models.URLField(blank=True, null=True)),
                ("created_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("updated_at", models.DateTimeField(default=django.utils.timezone.now)),
                (
                    "llm_optin_approved_by",
                    models.OneToOneField(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="approvedby",
                        to="ddpui.orguser",
                    ),
                ),
                (
                    "org",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="preferences",
                        to="ddpui.org",
                    ),
                ),
            ],
        ),
    ]