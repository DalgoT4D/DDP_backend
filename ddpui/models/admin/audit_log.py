import uuid
from django.db import models
from django.contrib.auth import get_user_model

User = get_user_model()

class AuditLog(models.Model):
    class ActionChoices(models.TextChoices):
        CREATE = 'CREATE', 'Create'
        UPDATE = 'UPDATE', 'Update'
        DELETE = 'DELETE', 'Delete'
        LOGIN = 'LOGIN', 'Login'

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    actor = models.ForeignKey(
        User, 
        on_delete=models.SET_NULL, 
        null=True, 
        related_name='audit_logs',
        help_text="The admin user who performed the action"
    )
    action = models.CharField(max_length=10, choices=ActionChoices.choices)
    resource_type = models.CharField(max_length=100, help_text="e.g., Organization, Warehouse, FeatureFlag")
    resource_id = models.CharField(max_length=255, help_text="ID of the modified resource")
    payload = models.JSONField(default=dict, help_text="The data payload or diff (MUST be scrubbed of passwords)")
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'admin_audit_log' 
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.actor} performed {self.action} on {self.resource_type} ({self.resource_id})"
