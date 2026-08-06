# Migration for audit_logs feature
# This creates the audit_log table in the database

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    # This migration depends on the previous one (0171_orgwarehouse_org_unique)
    dependencies = [
        ("ddpui", "0171_orgwarehouse_org_unique"),
    ]

    # The operations to perform
    operations = [
        # Step 1: Create the AuditLog table
        migrations.CreateModel(
            name="AuditLog",
            fields=[
                # Primary key - BigAutoField for handling many records
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                # Backup email - readable even after user deletion
                ("orguser_email", models.EmailField(blank=True, max_length=255)),
                # What type of resource (dashboard, pipeline, etc.)
                (
                    "resource_type",
                    models.CharField(
                        max_length=50,
                        choices=[
                            ("auth", "Auth"),
                            ("user", "User"),
                            ("org", "Org"),
                            ("org_user", "Org User"),
                            ("invitation", "Invitation"),
                            ("warehouse", "Warehouse"),
                            ("data_source", "Data Source"),
                            ("connection", "Connection"),
                            ("pipeline", "Pipeline"),
                            ("dbt", "dbt"),
                            ("dashboard", "Dashboard"),
                            ("chart", "Chart"),
                            ("metric", "Metric"),
                            ("kpi", "KPI"),
                            ("report", "Report"),
                            ("comment", "Comment"),
                            ("alert", "Alert"),
                        ],
                    ),
                ),
                # ID of the specific resource
                ("resource_id", models.CharField(blank=True, max_length=255)),
                # What action was performed
                (
                    "action",
                    models.CharField(
                        max_length=50,
                        choices=[
                            ("create", "Create"),
                            ("update", "Update"),
                            ("delete", "Delete"),
                            ("execute", "Execute"),
                            ("share", "Share"),
                            ("export", "Export"),
                            ("login", "Login"),
                            ("logout", "Logout"),
                            ("password_reset_requested", "Password Reset Requested"),
                            ("password_reset_completed", "Password Reset Completed"),
                            ("password_changed", "Password Changed"),
                            ("email_verified", "Email Verified"),
                        ],
                    ),
                ),
                # JSON field for tracking what changed
                ("resource_fields", models.JSONField(blank=True, default=dict)),
                # Timestamp - auto-set when record is created
                ("timestamp", models.DateTimeField(auto_now_add=True)),
                # Foreign key to Organization
                (
                    "org",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="audit_logs",
                        to="ddpui.org",
                    ),
                ),
                # Foreign key to OrgUser (nullable)
                (
                    "orguser",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="audit_logs",
                        to="ddpui.orguser",
                    ),
                ),
            ],
            options={
                "db_table": "audit_log",
                "ordering": ["-timestamp"],
            },
        ),
        # Step 2: Add indexes for fast queries
        migrations.AddIndex(
            model_name="auditlog",
            index=models.Index(fields=["org", "timestamp"], name="auditlog_org_ts_idx"),
        ),
        migrations.AddIndex(
            model_name="auditlog",
            index=models.Index(
                fields=["org", "orguser", "timestamp"], name="auditlog_org_orguser_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="auditlog",
            index=models.Index(
                fields=["org", "resource_type", "timestamp"],
                name="auditlog_org_restype_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="auditlog",
            index=models.Index(
                fields=["org", "action", "timestamp"], name="auditlog_org_action_idx"
            ),
        ),
    ]
