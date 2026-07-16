"""Seed the per-rtype share-permission slugs, granted to the same roles that
hold `can_share_dashboards`; Member deliberately gets none.

Fresh installs get the same rows from the seed JSON; this migration mirrors
them onto existing databases (idempotent, tolerant of absent roles). Only
DELETE the Redis role-permission cache so it rebuilds lazily — never call
set_roles_and_permissions_in_redis() here, Redis-less environments (CI)
must still migrate.
"""

import os

from django.db import migrations

SHARE_PERMISSIONS = [
    ("can_share_reports", "Can Share Reports"),
    ("can_share_alerts", "Can Share Alerts"),
    ("can_share_metrics", "Can Share Metrics"),
    ("can_share_kpis", "Can Share KPIs"),
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


def seed_share_permissions(apps, schema_editor):  # pylint: disable=unused-argument
    """Insert the four share slugs + role grants. Idempotent."""
    Permission = apps.get_model("ddpui", "Permission")
    Role = apps.get_model("ddpui", "Role")
    RolePermission = apps.get_model("ddpui", "RolePermission")

    roles = list(Role.objects.filter(slug__in=GRANTEE_ROLE_SLUGS))
    for slug, name in SHARE_PERMISSIONS:
        permission, _ = Permission.objects.get_or_create(slug=slug, defaults={"name": name})
        for role in roles:
            RolePermission.objects.get_or_create(role=role, permission=permission)

    _clear_role_permissions_cache()


def remove_share_permissions(apps, schema_editor):  # pylint: disable=unused-argument
    """Reverse: drop the four slugs (RolePermission rows cascade)."""
    Permission = apps.get_model("ddpui", "Permission")
    Permission.objects.filter(slug__in=[slug for slug, _ in SHARE_PERMISSIONS]).delete()
    _clear_role_permissions_cache()


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0170_resource_share"),
    ]

    operations = [
        migrations.RunPython(seed_share_permissions, remove_share_permissions),
    ]
