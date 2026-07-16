"""The ``AccessRequest`` model. One row = one ask for view/edit on a
resource. Approving inserts a ``ResourceShare`` grant; this model never
grants anything by itself — it is only the request/decision record.
"""

from datetime import timedelta

from django.db import models
from django.utils import timezone

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser


def default_access_request_expiry():
    """30 days from now. Requests undecided this long are swept to ``expired``
    by the daily cleanup task."""
    return timezone.now() + timedelta(days=30)


class AccessRequest(models.Model):
    """One request for access to a shareable resource."""

    STATUS_PENDING = "pending"
    STATUS_APPROVED = "approved"
    STATUS_DECLINED = "declined"
    STATUS_EXPIRED = "expired"

    STATUS_CHOICES = [
        (STATUS_PENDING, "Pending"),
        (STATUS_APPROVED, "Approved"),
        (STATUS_DECLINED, "Declined"),
        (STATUS_EXPIRED, "Expired"),
    ]

    org = models.ForeignKey(Org, on_delete=models.CASCADE)

    # Soft pointer to the requested resource — same shape as ResourceShare's,
    # validated against the shareable_types registry at the action layer.
    resource_type = models.CharField(max_length=20)
    resource_id = models.CharField(max_length=255)

    # CASCADE (unlike decided_by's SET_NULL): if the requester is deleted
    # there is nobody left to grant to, so the row goes with them.
    requester = models.ForeignKey(
        OrgUser,
        on_delete=models.CASCADE,
        related_name="access_requests_made",
    )
    requested_permission = models.CharField(max_length=5)  # view | edit
    note = models.CharField(max_length=500, null=True, blank=True)

    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default=STATUS_PENDING)
    decided_by = models.ForeignKey(
        OrgUser,
        on_delete=models.SET_NULL,
        null=True,
        related_name="access_requests_decided",
    )

    expires_at = models.DateTimeField(default=default_access_request_expiry)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "access_request"
        indexes = [
            models.Index(fields=["resource_type", "resource_id"]),
            models.Index(fields=["org", "status"]),
            models.Index(fields=["requester"]),
        ]

    def __str__(self):
        return (
            f"{self.resource_type}:{self.resource_id} request by "
            f"{self.requester_id} ({self.status})"
        )
