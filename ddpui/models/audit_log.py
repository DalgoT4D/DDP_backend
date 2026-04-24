from django.db import models
from django.contrib.auth.models import User
from ddpui.models.org import Org

class AuditLog(models.Model):
    """
    Model to track administrative and critical actions within the platform.
    """
    user = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, related_name="audit_logs")
    org = models.ForeignKey(Org, on_delete=models.SET_NULL, null=True, related_name="audit_logs")
    
    # Action details
    action = models.CharField(max_length=255, help_text="e.g. org_create, org_delete, user_invite")
    resource_type = models.CharField(max_length=100, help_text="e.g. Org, OrgUser, Warehouse")
    resource_id = models.CharField(max_length=255, null=True, blank=True)
    
    # Metadata
    payload = models.JSONField(null=True, blank=True, help_text="Store request data or context")
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    user_agent = models.TextField(null=True, blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["resource_type", "resource_id"]),
            models.Index(fields=["action"]),
            models.Index(fields=["created_at"]),
        ]

    def __str__(self):
        return f"{self.user} performed {self.action} on {self.resource_type} ({self.created_at})"
