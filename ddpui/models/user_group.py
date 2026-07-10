"""Groups: the ``UserGroup``/``UserGroupMember`` models (Task 7 — Milestone 3).

Groups live in the org app, NOT ``core/sharing`` — they are a general
org-membership concept (Layers 2-3 of Resource Sharing import them, but
Groups themselves know nothing about grants). A ``UserGroup`` is just a
named set of ``OrgUser``s within one org; ``ResourceShare`` rows reference a
group by id via the existing soft ``principal_type="group"`` /
``principal_id`` pointer (see ``ddpui/models/resource_share.py``) — there is
no FK from ``ResourceShare`` to ``UserGroup``.

``UserGroupMember.pending_email`` rows are schema-only in this task: the
invite flow that creates them lands in M4. A membership row always has
exactly one of ``orguser``/``pending_email`` set (enforced by a DB
``CheckConstraint``), so a half-invited row can never exist.
"""

from django.db import models
from django.utils import timezone

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser


class UserGroupMemberStatus(models.TextChoices):
    """Whether a membership row is a real (accepted) member or a pending
    invite. Only ACTIVE rows are created by this task's endpoints — PENDING
    is reserved for the M4 invite flow."""

    ACTIVE = "active"
    PENDING = "pending"


class UserGroup(models.Model):
    """A named set of ``OrgUser``s within one org (e.g. "Funders")."""

    org = models.ForeignKey(Org, on_delete=models.CASCADE, related_name="user_groups")
    name = models.CharField(max_length=255)
    created_by = models.ForeignKey(
        OrgUser,
        on_delete=models.SET_NULL,
        null=True,
        related_name="user_groups_created",
    )
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "user_group"
        constraints = [
            models.UniqueConstraint(fields=["org", "name"], name="uq_user_group_org_name"),
        ]

    def __str__(self):
        return f"{self.name} (org={self.org_id})"


class UserGroupMember(models.Model):
    """One membership row: a group has this ``OrgUser`` (active) or this
    ``pending_email`` (invited, not yet a user) as a member."""

    group = models.ForeignKey(UserGroup, on_delete=models.CASCADE, related_name="members")
    orguser = models.ForeignKey(
        OrgUser,
        on_delete=models.CASCADE,
        null=True,
        related_name="user_group_memberships",
    )
    pending_email = models.CharField(max_length=255, null=True)
    status = models.CharField(
        max_length=10,
        choices=UserGroupMemberStatus.choices,
        default=UserGroupMemberStatus.ACTIVE,
    )
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "user_group_member"
        constraints = [
            models.UniqueConstraint(
                fields=["group", "orguser"], name="uq_user_group_member_orguser"
            ),
            models.UniqueConstraint(
                fields=["group", "pending_email"], name="uq_user_group_member_pending_email"
            ),
            models.CheckConstraint(
                check=(
                    models.Q(orguser__isnull=False, pending_email__isnull=True)
                    | models.Q(orguser__isnull=True, pending_email__isnull=False)
                ),
                name="ck_user_group_member_exactly_one_principal",
            ),
        ]

    def __str__(self):
        principal = self.orguser_id if self.orguser_id is not None else self.pending_email
        return f"group={self.group_id} member={principal} ({self.status})"
