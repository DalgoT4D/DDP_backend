""" Lock class for UI4T Workflow Canvas """

from django.db import models
from django.utils import timezone
from ddpui.models.org_user import OrgUser


class CanvasLock(models.Model):
    """Lock object, one per dbt project"""

    dbt = models.OneToOneField("OrgDbt", on_delete=models.CASCADE, related_name="canvas_lock")
    locked_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
    locked_at = models.DateTimeField(auto_now_add=True)
    lock_token = models.CharField(max_length=36, unique=True)
    expires_at = models.DateTimeField()
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __repr__(self) -> str:
        return f"CanvasLock[{self.dbt.org.slug} | {self.locked_by.user.email}]"

    def __str__(self):
        return f"Canvas lock for {self.dbt.org.slug} dbt project by {self.locked_by.user.email}"

    def is_expired(self) -> bool:
        """Check if the lock has expired"""
        return timezone.now() > self.expires_at
