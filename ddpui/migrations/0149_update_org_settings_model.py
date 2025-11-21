# Generated migration for updated OrgSettings model

from django.db import migrations, models
import django.db.models.deletion
from django.conf import settings


class Migration(migrations.Migration):
    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("ddpui", "0148_create_org_settings_model"),
    ]

    operations = [
        # Remove the old organization_logo URL field
        migrations.RemoveField(
            model_name="orgsettings",
            name="organization_logo",
        ),
        # Add new logo fields for binary file storage
        migrations.AddField(
            model_name="orgsettings",
            name="organization_logo",
            field=models.BinaryField(
                blank=True, help_text="Organization logo image data", null=True
            ),
        ),
        migrations.AddField(
            model_name="orgsettings",
            name="organization_logo_filename",
            field=models.CharField(
                blank=True,
                help_text="Original filename of the uploaded logo",
                max_length=200,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="orgsettings",
            name="organization_logo_content_type",
            field=models.CharField(
                blank=True, help_text="MIME type of the logo file", max_length=100, null=True
            ),
        ),
        # Add tracking fields for AI settings acceptance
        migrations.AddField(
            model_name="orgsettings",
            name="ai_settings_accepted_by",
            field=models.ForeignKey(
                blank=True,
                help_text="User who last accepted/modified AI settings",
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                to=settings.AUTH_USER_MODEL,
            ),
        ),
        migrations.AddField(
            model_name="orgsettings",
            name="ai_settings_accepted_at",
            field=models.DateTimeField(
                blank=True, help_text="When AI settings were last accepted/modified", null=True
            ),
        ),
    ]
