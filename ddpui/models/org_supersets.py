from django.db import models
from ddpui.models.org import Org
from django.utils import timezone


class OrgSupersets(models.Model):
    """Model to store org supereset details for settings panel"""

    org = models.OneToOneField(Org, on_delete=models.CASCADE, related_name="orgInfo")
    container_name = models.CharField(max_length=255, blank=True, null=True)
    superset_version = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(default=timezone.now)
