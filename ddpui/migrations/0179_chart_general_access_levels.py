# Resource Sharing v1.1: charts join the sharing model.
#
# 1. Chart gains the per-role general-access columns every shareable rtype
#    has (analyst_level / member_level). The AddField defaults ARE the
#    behavior-preserving backfill (v1.1 decision #3): every existing chart
#    row gets analyst_level="edit", member_level="none" — exactly today's
#    effective behavior (all analysts see/edit all charts; Members see them
#    only inside shared containers). Nothing changes for anyone on migration
#    day. member_level is pinned to "none" in v1.1 (decision #2: Member
#    chart sharing deferred). Reverse = drop the columns.
#
# 2. Seed the new `can_share_charts` permission slug for EXISTING databases,
#    granted to the same roles that hold `can_share_dashboards` (super-admin,
#    admin, analyst; legacy account-manager included) — mirrors
#    0171_seed_share_permission_slugs. Fresh installs get the same rows from
#    seed/002_permissions.json + seed/003_role_permissions.json.

import os

from django.db import migrations, models

SHARE_PERMISSION = ("can_share_charts", "Can Share Charts")

# Mirrors the holders of can_share_dashboards (same list 0171 used). Legacy
# pre-rbac-v2 slugs included so a DB migrated before `migrate_rbac_v2_roles`
# still grants its admin-equivalent role; absent roles are skipped.
GRANTEE_ROLE_SLUGS = ["super-admin", "admin", "account-manager", "analyst"]


def _clear_role_permissions_cache():
    """Delete the Redis role-permission cache so it rebuilds lazily with the
    new slug. Absence of Redis must never fail the migration."""
    try:
        from ddpui.utils.redis_client import RedisClient

        key = os.getenv("ROLE_PERMISSIONS_REDIS_KEY", "dalgo_permissions_key")
        RedisClient.get_instance().delete(key)
    except Exception:  # pylint: disable=broad-except
        pass


def seed_chart_share_permission(apps, schema_editor):  # pylint: disable=unused-argument
    """Insert the can_share_charts slug + role grants. Idempotent."""
    Permission = apps.get_model("ddpui", "Permission")
    Role = apps.get_model("ddpui", "Role")
    RolePermission = apps.get_model("ddpui", "RolePermission")

    slug, name = SHARE_PERMISSION
    permission, _ = Permission.objects.get_or_create(slug=slug, defaults={"name": name})
    for role in Role.objects.filter(slug__in=GRANTEE_ROLE_SLUGS):
        RolePermission.objects.get_or_create(role=role, permission=permission)

    _clear_role_permissions_cache()


def remove_chart_share_permission(apps, schema_editor):  # pylint: disable=unused-argument
    """Reverse: drop the slug (RolePermission rows cascade)."""
    Permission = apps.get_model("ddpui", "Permission")
    Permission.objects.filter(slug=SHARE_PERMISSION[0]).delete()
    _clear_role_permissions_cache()


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0178_org_preferences_view_view_defaults"),
    ]

    operations = [
        migrations.AddField(
            model_name="chart",
            name="analyst_level",
            field=models.CharField(
                choices=[("none", "None"), ("view", "View"), ("edit", "Edit")],
                default="edit",
                max_length=5,
            ),
        ),
        migrations.AddField(
            model_name="chart",
            name="member_level",
            field=models.CharField(
                choices=[("none", "None"), ("view", "View"), ("edit", "Edit")],
                default="none",
                max_length=5,
            ),
        ),
        migrations.RunPython(seed_chart_share_permission, remove_chart_share_permission),
    ]
