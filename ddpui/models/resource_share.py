"""Resource Sharing grants: the ``ResourceShare`` model (Layer 2 of Resource
Sharing — explicit per-principal grants, on top of Layer 1's general access).

One row = one grant: a principal (user, group, or in future a value like an
audience tier) has some permission (view/edit) on a specific resource.

The resource pointer is deliberately soft: ``resource_type`` + ``resource_id``
(a string), not a FK to Dashboard/ReportSnapshot/etc. This is a Layer 2/3
contract — ``resource_id`` needs to hold UUID pks and warehouse
"schema.table" identifiers later, not just the integer pks used today. Do
not "improve" this into a FK, and do not add a ``via_container`` field —
both are out of scope for this task.

The principal is similarly an open enum (``principal_type``): "user" and
"group" are matched by the access resolver in v1; "audience" is reserved
for a future deferred behavior and is never matched today.
"""

from django.db import models

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser


class ResourceShare(models.Model):
    """A single access grant on a shareable resource."""

    org = models.ForeignKey(Org, on_delete=models.CASCADE)

    # Soft pointer to the shared resource (validated against the
    # shareable_types registry at the action layer, not here).
    resource_type = models.CharField(max_length=20)
    resource_id = models.CharField(max_length=255)

    # Who the grant is for. Only "user" and "group" are matched by the
    # resolver in v1; "audience" rows are inserted but never matched
    # (deferred behavior).
    principal_type = models.CharField(max_length=10)
    principal_id = models.BigIntegerField(null=True)
    principal_value = models.CharField(max_length=50, null=True)

    permission = models.CharField(max_length=5)
    status = models.CharField(max_length=10, default="active")
    pending_email = models.CharField(max_length=255, null=True)

    created_by = models.ForeignKey(
        OrgUser,
        on_delete=models.SET_NULL,
        null=True,
        related_name="resource_shares_created",
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "resource_share"
        indexes = [
            models.Index(fields=["resource_type", "resource_id"]),
            models.Index(fields=["principal_type", "principal_id"]),
            models.Index(fields=["org", "status"]),
        ]

    def __str__(self):
        principal = self.principal_id if self.principal_id is not None else self.principal_value
        return (
            f"{self.resource_type}:{self.resource_id} -> "
            f"{self.principal_type}:{principal} ({self.permission})"
        )
