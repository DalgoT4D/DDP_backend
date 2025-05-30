"""store results of prefect's flow-runs so we don't need to query prefect's api every time we want to display the dashboard"""

from django.db import models
from django.utils import timezone
from ddpui.models.org_user import OrgUser


class PrefectFlowRun(models.Model):
    """Result of a prefect flow run"""

    deployment_id = models.CharField(max_length=36, null=True)
    flow_run_id = models.CharField(max_length=36, null=False, blank=False)
    name = models.CharField(max_length=255, null=False, blank=False)
    start_time = models.DateTimeField(null=True, blank=False)
    expected_start_time = models.DateTimeField(null=False, blank=False)
    total_run_time = models.FloatField(null=False, blank=False)
    status = models.CharField(max_length=20, null=False, blank=False)
    state_name = models.CharField(max_length=20, null=False, blank=False)
    retries = models.SmallIntegerField(default=0)
    orguser = models.ForeignKey(OrgUser, on_delete=models.SET_NULL, null=True, blank=True)
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        """string representation"""
        return f"PrefectFlowRun[{self.deployment_id}|{self.flow_run_id}|{self.status}]"

    def to_json(self) -> dict:
        """JSON representation"""
        return {
            "deployment_id": self.deployment_id,
            "id": self.flow_run_id,
            "name": self.name,
            "startTime": (
                self.start_time.isoformat() if self.start_time else None  # pylint:disable=no-member
            ),
            "expectedStartTime": self.expected_start_time.isoformat(),  # pylint:disable=no-member
            "totalRunTime": self.total_run_time,
            "status": self.status,
            "state_name": self.state_name,
            "orguser": self.orguser.user.email if self.orguser else None,
        }
