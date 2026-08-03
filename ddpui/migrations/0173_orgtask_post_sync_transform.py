from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0172_orgwarehouse_dbt_profile_secret_block"),
    ]

    operations = [
        migrations.AddField(
            model_name="orgtask",
            name="post_sync_transform",
            field=models.JSONField(blank=True, default=None, null=True),
        ),
    ]
