"""
master table to store all operations/commands that a prefect deployment can run
args to a command are specific to a deployment/flow & will be store as deployment/flow parameters
"""

from django.db import models
from ddpui.models.org import Org
from ddpui.models.org import OrgDataFlowv1
from ddpui.models.org_user import OrgUser


class Task(models.Model):
    """Dalgo task containing dbt & airbyte & git operations"""

    type = models.CharField(max_length=100, null=False, blank=False)
    slug = models.CharField(max_length=100, null=False, blank=False)
    label = models.CharField(max_length=100, null=False, blank=False)
    command = models.CharField(max_length=100, null=True)

    def __str__(self) -> str:
        """string representation"""
        return f"DalgoTask[{self.type}|{self.label}]"

    def to_json(self) -> dict:
        """JSON representation"""
        return {
            "type": self.type,
            "label": self.label,
            "command": self.command,
        }


class OrgTask(models.Model):
    """Docstring"""

    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    task = models.ForeignKey(Task, on_delete=models.CASCADE)
    connection_id = models.CharField(max_length=36, unique=True, null=True)

    def __str__(self) -> str:
        return f"OrgTask[{self.org.name}|{self.task.type}|{self.task.label}]"


class DataflowOrgTask(models.Model):
    """Association of OrgPrefectBlocks to their deployments"""

    dataflow = models.ForeignKey(OrgDataFlowv1, on_delete=models.CASCADE)
    orgtask = models.ForeignKey(OrgTask, on_delete=models.CASCADE)


class TaskLock(models.Model):
    """A locking implementation for OrgTask"""

    orgtask = models.OneToOneField(OrgTask, on_delete=models.CASCADE)
    flow_run_id = models.TextField(max_length=36, blank=True, default="")
    locked_at = models.DateTimeField(auto_now_add=True)
    locked_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
    locking_dataflow = models.ForeignKey(
        OrgDataFlowv1, on_delete=models.CASCADE, null=True
    )
