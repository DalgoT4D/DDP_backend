""" Lock class for UI4T Workflow Canvas """

from django.db import models
from django.utils import timezone
from ddpui.models.org_user import OrgUser


class CanvasLock(models.Model):
    """Lock object, one per org"""

    locked_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
    locked_at = models.DateTimeField(auto_now_add=True)
    lock_id = models.UUIDField(editable=False, unique=True, null=True)
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __repr__(self) -> str:
        return f"CanvasLock[{self.locked_by.org.slug} | {self.locked_by.user.email}]"
