# Generated migration for OrgSettings model

from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0147_add_org_settings_permission"),
    ]

    operations = [
        migrations.CreateModel(
            name="OrgSettings",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                (
                    "organization_name",
                    models.CharField(
                        blank=True,
                        help_text="Display name for the organization",
                        max_length=200,
                        null=True,
                    ),
                ),
                (
                    "website",
                    models.URLField(
                        blank=True, help_text="Organization website URL", max_length=500, null=True
                    ),
                ),
                (
                    "organization_logo",
                    models.URLField(
                        blank=True,
                        help_text="URL to organization logo image",
                        max_length=500,
                        null=True,
                    ),
                ),
                (
                    "ai_data_sharing_enabled",
                    models.BooleanField(
                        default=False,
                        help_text="Allow sharing organization data with AI for better contextual responses",
                    ),
                ),
                (
                    "ai_logging_acknowledged",
                    models.BooleanField(
                        default=False,
                        help_text="User has acknowledged that AI questions and answers are logged and stored for at least 3 months",
                    ),
                ),
                (
                    "created_at",
                    models.DateTimeField(auto_created=True, default=django.utils.timezone.now),
                ),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "org",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="settings",
                        to="ddpui.org",
                    ),
                ),
            ],
            options={
                "verbose_name": "Organization Settings",
                "verbose_name_plural": "Organization Settings",
                "db_table": "org_settings",
            },
        ),
    ]
