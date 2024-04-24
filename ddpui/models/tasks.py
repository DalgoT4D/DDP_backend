"""
master table to store all operations/commands that a prefect deployment can run
args to a command are specific to a deployment/flow & will be store as deployment/flow parameters
"""

from enum import Enum
from django.db import models
from ddpui.models.org import Org
from ddpui.models.org import OrgDataFlowv1
from ddpui.models.org_user import OrgUser


class OrgTaskGeneratedBy(str, Enum):
    """an enum for roles assignable to org-users"""

    SYSTEM = "system"
    CLIENT = "client"

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class Task(models.Model):
    """Dalgo task containing dbt & airbyte & git operations"""

    type = models.CharField(max_length=100, null=False, blank=False)
    slug = models.CharField(max_length=100, null=False, blank=False)
    label = models.CharField(max_length=100, null=False, blank=False)
    command = models.CharField(max_length=100, null=True)
    is_system = models.BooleanField(
        default=True
    )  # to mark the tasks created by platform by default

    def __str__(self) -> str:
        """string representation"""
        return f"DalgoTask[{self.type}|{self.label}]"

    def to_json(self) -> dict:
        """JSON representation"""
        return {
            "type": self.type,
            "label": self.label,
            "command": self.command,
            "is_system": self.is_system,
        }


class OrgTask(models.Model):
    """Docstring"""

    uuid = models.UUIDField(editable=False, unique=True, null=True)
    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    task = models.ForeignKey(Task, on_delete=models.CASCADE)
    connection_id = models.CharField(max_length=36, unique=True, null=True)
    parameters = models.JSONField(default=dict, blank=True)
    generated_by = models.CharField(
        choices=OrgTaskGeneratedBy.choices(), max_length=50, default="system"
    )

    def __str__(self) -> str:
        return f"OrgTask[{self.org.name}|{self.task.type}|{self.task.label}]"

    def flags(self):
        """parameters = {"flags": ['f1', 'f2'], "options": {"o1": "v1", "o2": "v2"}"""
        if self.parameters:
            return self.parameters.get("flags")

    def options(self):
        """parameters = {"flags": ['f1', 'f2'], "options": {"o1": "v1", "o2": "v2"}"""
        if self.parameters:
            return self.parameters.get("options")

    def get_task_parameters(self):
        """
        returns the command line parameters for this task
        """
        retval = self.task.command
        if self.flags():
            for flag in self.flags():
                retval += " --" + flag
        if self.options():
            for optname, optval in self.options().items():
                retval += f" --{optname} {optval}"
        return retval


class DataflowOrgTask(models.Model):
    """Association of OrgPrefectBlocks to their deployments"""

    dataflow = models.ForeignKey(OrgDataFlowv1, on_delete=models.CASCADE)
    orgtask = models.ForeignKey(OrgTask, on_delete=models.CASCADE)
    seq = models.IntegerField(default=1)


class TaskLockStatus(str, Enum):
    """all possible statuses of a task lock"""

    QUEUED = "queued"
    RUNNING = "running"
    LOCKED = "locked"
    COMPLETED = "complete"


class TaskLock(models.Model):
    """A locking implementation for OrgTask"""

    orgtask = models.OneToOneField(OrgTask, on_delete=models.CASCADE)
    flow_run_id = models.TextField(max_length=36, blank=True, default="")
    locked_at = models.DateTimeField(auto_now_add=True)
    locked_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
    locking_dataflow = models.ForeignKey(
        OrgDataFlowv1, on_delete=models.CASCADE, null=True
    )
