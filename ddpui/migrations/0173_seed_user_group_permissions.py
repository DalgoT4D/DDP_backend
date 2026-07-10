"""Groups (Task 7): seed the `can_manage_user_groups` / `can_view_user_groups`
permission slugs.

No existing slug family fits "Analyst+ create/read, Member excluded" for
group management (the closest sibling, invitations, is admin-only for
create/edit/delete and admin+analyst only for view — see plan discussion in
the Task 7 report). So this adds two new slugs, granted to the same roles
that hold `can_share_dashboards` (super-admin, admin, analyst); Member gets
neither.

Fresh installs get the same rows from seed/002_permissions.json +
seed/003_role_permissions.json; this migration mirrors them onto existing
databases (idempotent via get_or_create, safe before or after
`migrate_rbac_v2_roles`, and tolerant of roles that don't exist yet).
Follows 0171_seed_share_permission_slugs.py's pattern exactly, including the
Redis-cache-invalidation try/except.
"""

import os

from django.db import migrations

GROUP_PERMISSIONS = [
    ("can_manage_user_groups", "Can Manage User Groups"),
    ("can_view_user_groups", "Can View User Groups"),
]

# Mirrors the holders of can_share_dashboards. Legacy pre-rbac-v2 slugs are
# included so a DB migrated before running `migrate_rbac_v2_roles` still
# grants its admin-equivalent role; absent roles are skipped.
GRANTEE_ROLE_SLUGS = ["super-admin", "admin", "account-manager", "analyst"]


def _clear_role_permissions_cache():
    """Delete the Redis role-permission cache so it rebuilds lazily with the
    new slugs. Absence of Redis must never fail the migration."""
    try:
        from ddpui.utils.redis_client import RedisClient

        key = os.getenv("ROLE_PERMISSIONS_REDIS_KEY", "dalgo_permissions_key")
        RedisClient.get_instance().delete(key)
    except Exception:  # pylint: disable=broad-except
        pass


def seed_group_permissions(apps, schema_editor):  # pylint: disable=unused-argument
    """Insert the two group slugs + role grants. Idempotent."""
    Permission = apps.get_model("ddpui", "Permission")
    Role = apps.get_model("ddpui", "Role")
    RolePermission = apps.get_model("ddpui", "RolePermission")

    roles = list(Role.objects.filter(slug__in=GRANTEE_ROLE_SLUGS))
    for slug, name in GROUP_PERMISSIONS:
        permission, _ = Permission.objects.get_or_create(slug=slug, defaults={"name": name})
        for role in roles:
            RolePermission.objects.get_or_create(role=role, permission=permission)

    _clear_role_permissions_cache()


def remove_group_permissions(apps, schema_editor):  # pylint: disable=unused-argument
    """Reverse: drop the two slugs (RolePermission rows cascade)."""
    Permission = apps.get_model("ddpui", "Permission")
    Permission.objects.filter(slug__in=[slug for slug, _ in GROUP_PERMISSIONS]).delete()
    _clear_role_permissions_cache()


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0172_user_group_models"),
    ]

    operations = [
        migrations.RunPython(seed_group_permissions, remove_group_permissions),
    ]
