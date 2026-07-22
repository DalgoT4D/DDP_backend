"""The ``ResourceShare`` model: one row = one per-principal grant (view/edit)
on a specific resource.

The resource pointer is deliberately soft — ``resource_type`` + a string
``resource_id``, not a FK — because it must later hold UUID pks and
warehouse "schema.table" identifiers. Do not "improve" this into a FK.
``principal_type`` is an open enum: "user" and "group" are matched by the
resolver; "audience" is reserved and never matched today.
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
    # FK twin of the varchar above (v1.2 pilot): the grant's meaning as a real
    # Permission row. Synced from (resource_type, permission) on every save;
    # PROTECT because grants make permission rows load-bearing. Nullable while
    # rtypes without view/edit slugs (report) and unseeded test DBs exist.
    granted_permission = models.ForeignKey(
        "ddpui.Permission",
        on_delete=models.PROTECT,
        null=True,
        db_column="permission_id",
        related_name="+",
    )
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

    def save(self, *args, **kwargs):
        # Keep the FK in lockstep with the varchar no matter who writes
        # (create, update_or_create, level upgrades). Unmappable rows
        # (report rtype, unseeded DBs) keep their current value.
        from ddpui.core.sharing.permission_map import permission_id_for

        pk = permission_id_for(self.resource_type, self.permission)
        if pk is not None and pk != self.granted_permission_id:
            self.granted_permission_id = pk
            update_fields = kwargs.get("update_fields")
            if update_fields is not None:
                kwargs["update_fields"] = list(update_fields) + ["granted_permission"]
        super().save(*args, **kwargs)

    def __str__(self):
        principal = self.principal_id if self.principal_id is not None else self.principal_value
        return (
            f"{self.resource_type}:{self.resource_id} -> "
            f"{self.principal_type}:{principal} ({self.permission})"
        )
