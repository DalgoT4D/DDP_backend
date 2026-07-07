"""
Audit log model for tracking user-initiated actions across the platform.
"""

from django.db import models


class AuditLogResourceType(models.TextChoices):
    """
    Resource types that can be logged in the audit trail.
    These are all the "things" in Dalgo that users can act on.
    """

    AUTH = "auth", "Auth"
    USER = "user", "User"
    ORG = "org", "Org"
    ORG_USER = "org_user", "Org User"
    INVITATION = "invitation", "Invitation"
    WAREHOUSE = "warehouse", "Warehouse"
    DATA_SOURCE = "data_source", "Data Source"
    CONNECTION = "connection", "Connection"
    PIPELINE = "pipeline", "Pipeline"
    DBT = "dbt", "dbt"
    DASHBOARD = "dashboard", "Dashboard"
    CHART = "chart", "Chart"
    METRIC = "metric", "Metric"
    KPI = "kpi", "KPI"
    REPORT = "report", "Report"
    COMMENT = "comment", "Comment"


class AuditLogAction(models.TextChoices):
    """
    Action types that can be logged.
    These are the "verbs" - what the user did.
    """

    CREATE = "create", "Create"
    UPDATE = "update", "Update"
    DELETE = "delete", "Delete"
    EXECUTE = "execute", "Execute"
    SHARE = "share", "Share"
    LOGIN = "login", "Login"
    LOGOUT = "logout", "Logout"


class AuditLog(models.Model):
    """
    Immutable record of a user-initiated action.

    Each entry captures:
    - WHO performed the action (orguser + orguser_email)
    - WHICH organization it belongs to (org)
    - WHAT resource was affected (resource_type, resource_id, resource_name)
    - WHAT action was taken (action)
    - WHAT changed (field_changes)
    - WHEN it happened (timestamp)
    """

    # Primary key - using BigAutoField for large number of logs over time
    id = models.BigAutoField(primary_key=True)

    # Which organization this log belongs to
    # CASCADE means: if org is deleted, delete all its audit logs too
    org = models.ForeignKey(
        "ddpui.Org",
        on_delete=models.CASCADE,
        related_name="audit_logs",
    )

    # Who performed the action
    # SET_NULL means: if user is deleted, keep the log but set this to NULL
    orguser = models.ForeignKey(
        "ddpui.OrgUser",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="audit_logs",
    )

    # Backup copy of email - so we can still see who did it even if user is deleted
    orguser_email = models.EmailField(max_length=255, blank=True)

    # What type of thing was acted on (dashboard, pipeline, etc.)
    resource_type = models.CharField(
        max_length=50,
        choices=AuditLogResourceType.choices,
    )

    # The ID of the specific resource (e.g., dashboard ID "123")
    resource_id = models.CharField(max_length=255, blank=True)

    # Human-readable name (e.g., "Sales Dashboard")
    resource_name = models.CharField(max_length=500, blank=True)

    # What action was performed (create, update, delete, etc.)
    action = models.CharField(max_length=50, choices=AuditLogAction.choices)

    # What fields changed - stored as JSON like:
    # {"name": {"old": "Old Name", "new": "New Name"}}
    # Never contains secrets (passwords, tokens, etc.)
    field_changes = models.JSONField(default=dict, blank=True)

    # When the action happened - automatically set when record is created
    timestamp = models.DateTimeField(auto_now_add=True)

    class Meta:
        # Table name in the database
        db_table = "audit_log"

        # Default ordering: newest first
        ordering = ["-timestamp"]

        # Database indexes for fast queries
        # These make searching by org+timestamp, org+user, etc. much faster
        indexes = [
            models.Index(fields=["org", "timestamp"], name="auditlog_org_ts_idx"),
            models.Index(
                fields=["org", "orguser", "timestamp"], name="auditlog_org_orguser_idx"
            ),
            models.Index(
                fields=["org", "resource_type", "timestamp"],
                name="auditlog_org_restype_idx",
            ),
            models.Index(
                fields=["org", "action", "timestamp"], name="auditlog_org_action_idx"
            ),
        ]

    def __str__(self):
        return f"AuditLog({self.action} {self.resource_type} by {self.orguser_email})"
