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


# submitting a deployment run
# - have orgdataflow entry already
# - get flow run id
# -

# submitting a manual sync:
# - get the deployment for the manual sync
# - ask prefect for the blockname in the deployment
# - get the orgprefectblock for this blockname
# - create a entry for (orgprefectblock, org_dataflow_id)
