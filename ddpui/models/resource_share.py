"""The ``ResourceShare`` model: one row = one per-principal grant (view/edit)
on a specific resource.

The resource pointer is deliberately soft — ``resource_type`` + a string
``resource_id``, not a FK — because it must later hold UUID pks and
warehouse "schema.table" identifiers. Do not "improve" this into a FK.
"""

from typing import Optional

from django.db import models

from ddpui.models.org import Org
from ddpui.models.org_user import Invitation, OrgUser


class AccessLevel(models.TextChoices):
    """Access level for a shareable resource. Used on both:
    - ``ResourceShare.access_level`` — the grant itself
    - ``OrgPreferences.default_{analyst,member}_level`` — org-level defaults
    """

    VIEW = "view", "View"
    EDIT = "edit", "Edit"
    NO_ACCESS = "no_access", "No Access"


LEVEL_RANK = {AccessLevel.NO_ACCESS: 0, AccessLevel.VIEW: 1, AccessLevel.EDIT: 2}


def max_access_level(*levels: Optional[str]) -> Optional[str]:
    """Return the highest AccessLevel from args, skipping None. Returns None if all None."""
    valid = [l for l in levels if l is not None]
    return max(valid, key=lambda l: LEVEL_RANK[l]) if valid else None


class ResourceType(models.TextChoices):
    DASHBOARD = "dashboard", "Dashboard"
    CHART = "chart", "Chart"
    REPORT = "report", "Report"
    KPI = "kpi", "KPI"


class ResourceSharePrincipalType(models.TextChoices):
    """Who a grant is for. Only ``user`` and ``group`` are matched by the resolver."""

    USER = "user", "User"
    GROUP = "group", "Group"


class ResourceShare(models.Model):
    """A single access grant on a shareable resource.

    A grant points at either a concrete principal (``principal_type`` +
    ``principal_id``) or a pending ``invitation``. When the invitation is
    accepted, the row is promoted to point at the new ``OrgUser`` and
    ``invitation`` becomes NULL via ``on_delete=SET_NULL``.
    """

    org = models.ForeignKey(Org, on_delete=models.CASCADE)

    resource_type = models.CharField(max_length=20)
    resource_id = models.CharField(max_length=255)

    principal_type = models.CharField(
        max_length=5, choices=ResourceSharePrincipalType.choices, null=True
    )
    principal_id = models.BigIntegerField(null=True)

    access_level = models.CharField(max_length=10, choices=AccessLevel.choices)
    invitation = models.ForeignKey(Invitation, on_delete=models.SET_NULL, null=True)

    parent = models.ForeignKey(
        "self",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="children",
    )
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
            # cascade child lookup — filter(parent=share) when propagating level updates
            models.Index(fields=["parent"], name="resource_sh_parent_idx"),
        ]
        constraints = [
            # prevent duplicate direct grants for the same principal on the same resource
            models.UniqueConstraint(
                fields=["org", "resource_type", "resource_id", "principal_type", "principal_id"],
                condition=models.Q(parent__isnull=True),
                name="uq_resource_share_direct_grant",
            ),
            # prevent duplicate cascade rows for the same principal+resource from the same parent share
            models.UniqueConstraint(
                fields=["org", "resource_type", "resource_id", "principal_type", "principal_id", "parent"],
                condition=models.Q(parent__isnull=False),
                name="uq_resource_share_cascade_grant",
            ),
        ]

    def __str__(self):
        principal = (
            f"{self.principal_type}:{self.principal_id}"
            if self.principal_id is not None
            else f"inv:{self.invitation_id}"
        )
        return f"{self.resource_type}:{self.resource_id} -> {principal} ({self.access_level})"


class AccessRequestStatus(models.TextChoices):
    PENDING = "pending", "Pending"
    APPROVED = "approved", "Approved"
    DECLINED = "declined", "Declined"


class AccessRequest(models.Model):
    """A request from a user who lacks access to a resource.

    Persists between submission and owner response. On approval,
    a ResourceShare grant is created and this row's status is updated.
    Requests only come from authenticated users (always a user principal,
    never a group or invitation).
    """

    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    resource_type = models.CharField(max_length=20)
    resource_id = models.CharField(max_length=255)
    requester = models.ForeignKey(OrgUser, on_delete=models.CASCADE, related_name="access_requests")
    requested_level = models.CharField(
        max_length=10,
        choices=[(AccessLevel.VIEW, "View"), (AccessLevel.EDIT, "Edit")],
    )
    status = models.CharField(
        max_length=10, choices=AccessRequestStatus.choices, default=AccessRequestStatus.PENDING
    )
    note = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "access_request"
        indexes = [
            models.Index(fields=["org", "resource_type", "resource_id"], name="access_req_org_resource_idx"),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["org", "resource_type", "resource_id", "requester"],
                condition=models.Q(status="pending"),
                name="uq_access_request_pending_per_user",
            )
        ]

    def __str__(self):
        return f"{self.resource_type}:{self.resource_id} <- {self.requester} ({self.requested_level}, {self.status})"
