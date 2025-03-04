from django.db import models
from django.utils import timezone
from ddpui.models.org import Org


class OrgWren(models.Model):
    """Model to store org's wrenai details"""

    org = models.OneToOneField(Org, on_delete=models.CASCADE, related_name="org_wren_info")
    wren_url = models.CharField(max_length=255, blank=False, null=False)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(default=timezone.now)
