from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0160_merge_20260413_1732"),
    ]

    operations = [
        migrations.CreateModel(
            name="DashboardChatMetadataArtifact",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("schema_version", models.PositiveIntegerField(default=1)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("ready", "Ready"),
                            ("building", "Building"),
                            ("failed", "Failed"),
                            ("stale", "Stale"),
                        ],
                        default="stale",
                        max_length=16,
                    ),
                ),
                ("artifact_json", models.JSONField(blank=True, default=dict)),
                ("source_fingerprint", models.CharField(blank=True, default="", max_length=255)),
                ("builder_model", models.CharField(blank=True, default="", max_length=128)),
                ("built_at", models.DateTimeField(blank=True, null=True)),
                ("error_payload", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "built_by",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="dashboard_chat_metadata_builds",
                        to="ddpui.orguser",
                    ),
                ),
                (
                    "dashboard",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="chat_metadata_artifact",
                        to="ddpui.dashboard",
                    ),
                ),
            ],
            options={
                "db_table": "dashboard_chat_metadata_artifact",
            },
        ),
        migrations.CreateModel(
            name="DashboardChatMetadataBuildRun",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("ready", "Ready"),
                            ("building", "Building"),
                            ("failed", "Failed"),
                            ("stale", "Stale"),
                        ],
                        default="building",
                        max_length=16,
                    ),
                ),
                ("builder_model", models.CharField(blank=True, default="", max_length=128)),
                ("started_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("finished_at", models.DateTimeField(blank=True, null=True)),
                ("log_payload", models.JSONField(blank=True, null=True)),
                ("error_payload", models.JSONField(blank=True, null=True)),
                (
                    "dashboard",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="chat_metadata_build_runs",
                        to="ddpui.dashboard",
                    ),
                ),
            ],
            options={
                "db_table": "dashboard_chat_metadata_build_run",
            },
        ),
        migrations.AddIndex(
            model_name="dashboardchatmetadataartifact",
            index=models.Index(fields=["status"], name="dchat_meta_artifact_status_idx"),
        ),
        migrations.AddIndex(
            model_name="dashboardchatmetadataartifact",
            index=models.Index(fields=["built_at"], name="dchat_meta_artifact_built_idx"),
        ),
        migrations.AddIndex(
            model_name="dashboardchatmetadatabuildrun",
            index=models.Index(fields=["dashboard", "started_at"], name="dchat_meta_run_dash_idx"),
        ),
        migrations.AddIndex(
            model_name="dashboardchatmetadatabuildrun",
            index=models.Index(fields=["status"], name="dchat_meta_run_status_idx"),
        ),
    ]
