from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0163_remove_dashboard_root_layout_components"),
    ]

    operations = [
        migrations.AddField(
            model_name="org",
            name="logo_url",
            field=models.URLField(blank=True, max_length=500, null=True),
        ),
        migrations.AddField(
            model_name="org",
            name="logo_s3_key",
            field=models.CharField(blank=True, max_length=500, null=True),
        ),
        migrations.AddField(
            model_name="org",
            name="logo_filename",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]
