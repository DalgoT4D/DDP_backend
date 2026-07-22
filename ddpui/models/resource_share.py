"""The ``ResourceShare`` model: one row = one per-principal grant (view/edit)
on a specific resource.

The resource pointer is deliberately soft — ``resource_type`` + a string
``resource_id``, not a FK — because it must later hold UUID pks and
warehouse "schema.table" identifiers. Do not "improve" this into a FK.
"""

from django.db import models

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser


class AccessLevel(models.TextChoices):
    """Access level for a shareable resource. Used on both:
    - ``ResourceShare.access_level`` — the grant itself
    - ``OrgPreferences.default_{analyst,member}_level`` — org-level defaults
    """

    VIEW = "view", "View"
    EDIT = "edit", "Edit"


class ResourceSharePrincipalType(models.TextChoices):
    """Who a grant is for. Only ``user`` and ``group`` are matched by the resolver."""

    USER = "user", "User"
    GROUP = "group", "Group"


class ResourceShare(models.Model):
    """A single access grant on a shareable resource."""

    org = models.ForeignKey(Org, on_delete=models.CASCADE)

    resource_type = models.CharField(max_length=20)
    resource_id = models.CharField(max_length=255)

    principal_type = models.CharField(max_length=5, choices=ResourceSharePrincipalType.choices)
    principal_id = models.BigIntegerField()

    access_level = models.CharField(max_length=5, choices=AccessLevel.choices)
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
            # "who has access to this resource?" — tenant-scoped resource lookup
            models.Index(fields=["org", "resource_type", "resource_id"]),
            # "what does this principal have access to?" — tenant-scoped principal lookup
            models.Index(fields=["org", "principal_type", "principal_id"]),
            # invitation-acceptance path: find pending grants when a new user is created
            models.Index(fields=["pending_email"]),
        ]

    def __str__(self):
        return (
            f"{self.resource_type}:{self.resource_id} -> "
            f"{self.principal_type}:{self.principal_id} ({self.access_level})"
        )
