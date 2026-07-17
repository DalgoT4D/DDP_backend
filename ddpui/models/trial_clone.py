from enum import Enum

from django.db import models
from django.utils import timezone

from ddpui.models.org import Org


class TrialCloneStatus(str, Enum):
    """lifecycle of a single template→trial clone run"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

    @classmethod
    def choices(cls):
        return [(member.value, member.name) for member in cls]


class TrialClone(models.Model):
    """state machine + manifest for one template-org → trial-org clone run"""

    template_org = models.ForeignKey(Org, on_delete=models.SET_NULL, null=True, related_name="+")
    trial_org = models.ForeignKey(
        Org, on_delete=models.SET_NULL, null=True, blank=True, related_name="+"
    )
    trial_email = models.EmailField()
    status = models.CharField(
        max_length=50,
        default=TrialCloneStatus.PENDING.value,
        choices=TrialCloneStatus.choices(),
    )
    current_step = models.CharField(max_length=100, null=True, blank=True)
    error = models.TextField(null=True, blank=True)
    timings = models.JSONField(default=dict)  # {step_name: seconds}
    manifest = models.JSONField(default=dict)  # old→new id maps + external resource ids
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"TrialClone#{self.id} {self.trial_email} [{self.status}]"
