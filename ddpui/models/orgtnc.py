from django.db import models

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser


class OrgTnC(models.Model):
    """Docstring"""

    org = models.ForeignKey(Org, on_delete=models.CASCADE, related_name="orgtncs")
    tnc_accepted_on = models.DateField(null=False)
    tnc_accepted_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE, null=False)

    def __str__(self) -> str:
        return (
            f"OrgTnC[{self.org.slug}|{self.tnc_accepted_on}|{self.orguser.user.email}]"
        )
