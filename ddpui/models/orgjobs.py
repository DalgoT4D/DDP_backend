from django.db import models

from ddpui.models.org import Org, OrgPrefectBlock, OrgDataFlow
from ddpui.models.org_user import OrgUser


class BlockLock(models.Model):
    """A locking implementation for OrgPrefectBlocks"""

    opb = models.OneToOneField(OrgPrefectBlock, on_delete=models.CASCADE)
    locked_at = models.DateTimeField(auto_now_add=True)
    locked_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
    flow_run_id = models.TextField(max_length=36, blank=True, default="")


class DataflowBlock(models.Model):
    """Association of OrgPrefectBlocks to their deployments"""

    dataflow = models.ForeignKey(OrgDataFlow, on_delete=models.CASCADE)
    opb = models.ForeignKey(OrgPrefectBlock, on_delete=models.CASCADE)
