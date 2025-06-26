import uuid
from django.db import models
from django.utils import timezone
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser


class Chart(models.Model):
    """Docstring"""

    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    title = models.TextField(max_length=256)
    description = models.TextField(null=True)
    chart_type = models.TextField(max_length=256)
    schema = models.TextField(max_length=256)
    table = models.TextField(max_length=256)
    config = models.JSONField()
    created_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return f"Chart[{self.title}|{self.chart_type}]"
