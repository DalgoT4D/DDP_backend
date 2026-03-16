import ddpui.models.llm
import ddpui.models.org_preferences
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone
import uuid


class Migration(migrations.Migration):

    dependencies = [
        ("ddpui", "0151_alter_org_queue_config"),
    ]

    operations = [
        migrations.AddField(
            model_name="dashboard",
            name="ai_context_markdown",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="dashboard",
            name="ai_context_updated_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="dashboard",
            name="ai_context_updated_by",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="ai_context_dashboards_updated",
                to="ddpui.orguser",
            ),
        ),
        migrations.CreateModel(
            name="DashboardChatSession",
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
                (
                    "session_id",
                    models.UUIDField(default=uuid.uuid4, editable=False, unique=True),
                ),
                (
                    "status",
                    models.CharField(
                        choices=ddpui.models.llm.DashboardChatSessionStatus.choices(),
                        default=ddpui.models.llm.DashboardChatSessionStatus["QUEUED"],
                        max_length=50,
                    ),
                ),
                ("messages", models.JSONField(default=list)),
                ("latest_response", models.JSONField(blank=True, null=True)),
                ("request_meta", models.JSONField(blank=True, null=True)),
                ("response_meta", models.JSONField(blank=True, null=True)),
                ("feedback", models.TextField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_created=True, default=django.utils.timezone.now)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "dashboard",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to="ddpui.dashboard",
                    ),
                ),
                (
                    "org",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        to="ddpui.org",
                    ),
                ),
                (
                    "orguser",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to="ddpui.orguser",
                    ),
                ),
                (
                    "selected_chart",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to="ddpui.chart",
                    ),
                ),
            ],
            options={
                "db_table": "dashboard_chat_session",
                "ordering": ["-updated_at"],
            },
        ),
        migrations.AddIndex(
            model_name="dashboardchatsession",
            index=models.Index(
                fields=["org", "dashboard", "created_at"],
                name="dchat_sess_org_dash_idx",
            ),
        ),
        migrations.AddField(
            model_name="orgdbt",
            name="ai_catalog_sha256",
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AddField(
            model_name="orgdbt",
            name="ai_docs_generated_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="orgdbt",
            name="ai_manifest_sha256",
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AddField(
            model_name="orgdbt",
            name="ai_vector_last_ingested_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="orgpreferences",
            name="ai_chat_source_config",
            field=models.JSONField(default=ddpui.models.org_preferences.default_ai_chat_source_config),
        ),
        migrations.AddField(
            model_name="orgpreferences",
            name="ai_data_sharing_consented_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="orgpreferences",
            name="ai_data_sharing_consented_by",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="ai_data_sharing_consents",
                to="ddpui.orguser",
            ),
        ),
        migrations.AddField(
            model_name="orgpreferences",
            name="ai_data_sharing_enabled",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="orgpreferences",
            name="ai_org_context_markdown",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="orgpreferences",
            name="ai_org_context_updated_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="orgpreferences",
            name="ai_org_context_updated_by",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="ai_org_context_updates",
                to="ddpui.orguser",
            ),
        ),
    ]
