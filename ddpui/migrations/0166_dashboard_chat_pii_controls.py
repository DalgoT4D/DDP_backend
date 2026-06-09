from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0165_dashboard_chat_metadata_schema_version_4"),
    ]

    operations = [
        migrations.AddField(
            model_name="orgpreferences",
            name="dashboard_chat_share_pii_with_llms",
            field=models.BooleanField(default=True),
        ),
        migrations.CreateModel(
            name="DashboardChatPIIColumnOverride",
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
                ("schema_name", models.CharField(blank=True, default="", max_length=255)),
                ("table_name", models.CharField(max_length=255)),
                ("column_name", models.CharField(max_length=255)),
                ("pii", models.BooleanField()),
                ("created_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "org",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="dashboard_chat_pii_column_overrides",
                        to="ddpui.org",
                    ),
                ),
                (
                    "updated_by",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="dashboard_chat_pii_column_overrides",
                        to="ddpui.orguser",
                    ),
                ),
            ],
            options={
                "db_table": "dashboard_chat_pii_column_override",
            },
        ),
        migrations.AddConstraint(
            model_name="dashboardchatpiicolumnoverride",
            constraint=models.UniqueConstraint(
                fields=("org", "schema_name", "table_name", "column_name"),
                name="dchat_pii_override_unique",
            ),
        ),
        migrations.AddIndex(
            model_name="dashboardchatpiicolumnoverride",
            index=models.Index(
                fields=["org", "schema_name", "table_name"],
                name="dchat_pii_override_table_idx",
            ),
        ),
    ]
