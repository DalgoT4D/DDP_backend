from django.db import models

from ddpui.models.org import Org, OrgPrefectBlock, OrgDataFlow
from ddpui.models.org_user import OrgUser


class OrgJobs(models.Model):
    """table to track currently running jobs, for the UI"""

    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    flow_run_id = models.TextField(max_length=36)
    completed = models.BooleanField(default=False)
    # can add a prefect status field later

    started_from = models.TextField(max_length=50, null=True)
    started_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE, null=True)

    org_prefect_block_id = models.ForeignKey(
        OrgPrefectBlock, null=True, on_delete=models.CASCADE
    )
    org_dataflow_id = models.ForeignKey(
        OrgDataFlow, null=True, on_delete=models.CASCADE
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now_add=True)


class BlockLock(models.Model):
    """A locking implementation for OrgPrefectBlocks"""

    block = models.ForeignKey(OrgPrefectBlock, on_delete=models.CASCADE, unique=True)
    locked_at = models.DateTimeField(auto_now_add=True)
    locked_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
    flow_run_id = models.TextField(max_length=36, blank=True, default="")


# submitting a deployment run
# get blocknames from prefect-proxy/proxy/deployments/{deployment_id}
# atomic { create BlockLock for each OrgPrefectBlock(block_name={block_name}) with empty flow_run_id }
# if fail: return who locked which block when
# if success: submit deployment for run, get flow-run-id
# update the BlockLocks with the flow-run-id

# when an OrgUser accesses a page

# submitting a deployment run
# - have orgdataflow entry already
# - get flow run id
# -

# submitting a manual sync:
# - get the deployment for the manual sync
# - ask prefect for the blockname in the deployment
# - get the orgprefectblock for this blockname
# - create a entry for (orgprefectblock, org_dataflow_id)
