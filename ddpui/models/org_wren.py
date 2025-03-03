from django.db import models
from django.utils import timezone
from ddpui.models.org import Org


class OrgWren(models.Model):
    """Model to store org's wrenai details"""

    org = models.OneToOneField(Org, on_delete=models.CASCADE, related_name="org_wren_info")
    hostname = models.CharField(max_length=255, blank=True, null=True)
    port = models.IntegerField(blank=True, null=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(default=timezone.now)
