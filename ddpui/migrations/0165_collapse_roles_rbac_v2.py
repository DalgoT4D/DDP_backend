"""
Access Control v2 — collapse the 4 customer roles into 3.

  account-manager ─┐
                   ├─► admin   (slug renamed on pk2; pipeline-manager users repointed here)
  pipeline-manager ┘            then the pipeline-manager role row is deleted
  analyst  ──────────► analyst  (infra write/run slugs removed; infra view kept)
  guest    ──────────► member   (set to content-view-only)
  super-admin ───────► super-admin (untouched)

Slug strings are hardcoded on purpose — historical migrations must not import the
auth.py role constants, which change as roles evolve. Mirrors 0137 for the no-op guard
(roles are loaded from seed fixtures, not migrations, so this runs against zero Role
rows when building a fresh/test DB) and for the Redis refresh at the end.
"""

import os

from django.db import migrations

# analyst loses every infra write/run slug, keeps all infra view slugs
ANALYST_INFRA_WRITE_REMOVE = [
    "can_sync_sources",
    "can_create_dbt_docs",
    "can_create_dbt_model",
    "can_create_dbt_workspace",
    "can_edit_dbt_model",
    "can_edit_dbt_operation",
    "can_edit_dbt_workspace",
    "can_delete_dbt_model",
    "can_delete_dbt_operation",
    "can_delete_dbt_workspace",
    "can_create_orgtask",
    "can_delete_orgtask",
    "can_run_orgtask",
]

# member is content-view-only: viewing the four content resources + basics. No write of
# any kind and no infra visibility. Set EXACTLY (delete-all-then-add) so prod drift — e.g.
# can_create_dashboards granted to guests by migration 0137 — cannot leak through.
MEMBER_SLUGS = [
    "can_view_dashboards",
    "can_view_charts",
    "can_view_metrics",
    "can_view_kpis",
    "can_view_alerts",
    "can_view_orgusers",
    "can_view_flags",
    "can_request_llm_analysis_feature",
    "public",
]

# ── reverse-migration data: the pre-v2 permission sets (best-effort restore) ──
OLD_GUEST_SLUGS = [
    "can_request_llm_analysis_feature",
    "can_view_charts",
    "can_view_connection",
    "can_view_connections",
    "can_view_dashboards",
    "can_view_dbt_models",
    "can_view_dbt_operation",
    "can_view_dbt_workspace",
    "can_view_flags",
    "can_view_kpis",
    "can_view_master_task",
    "can_view_master_tasks",
    "can_view_metrics",
    "can_view_orgtasks",
    "can_view_orgusers",
    "can_view_pipeline",
    "can_view_pipeline_overview",
    "can_view_pipelines",
    "can_view_source",
    "can_view_sources",
    "can_view_task_progress",
    "can_view_usage_dashboard",
    "can_view_warehouse",
    "can_view_warehouse_data",
    "can_view_warehouses",
    "public",
]

OLD_PIPELINE_MANAGER_SLUGS = [
    "can_accept_tnc",
    "can_create_alerts",
    "can_create_charts",
    "can_create_connection",
    "can_create_dashboards",
    "can_create_dbt_docs",
    "can_create_dbt_model",
    "can_create_dbt_workspace",
    "can_create_kpis",
    "can_create_metrics",
    "can_create_orgtask",
    "can_create_pipeline",
    "can_create_source",
    "can_delete_alerts",
    "can_delete_charts",
    "can_delete_connection",
    "can_delete_dashboards",
    "can_delete_dbt_model",
    "can_delete_dbt_operation",
    "can_delete_dbt_workspace",
    "can_delete_kpis",
    "can_delete_metrics",
    "can_delete_orgtask",
    "can_delete_pipeline",
    "can_delete_source",
    "can_edit_alerts",
    "can_edit_charts",
    "can_edit_connection",
    "can_edit_dashboards",
    "can_edit_dbt_model",
    "can_edit_dbt_operation",
    "can_edit_dbt_workspace",
    "can_edit_kpis",
    "can_edit_metrics",
    "can_edit_pipeline",
    "can_edit_source",
    "can_request_llm_analysis_feature",
    "can_reset_connection",
    "can_run_orgtask",
    "can_run_pipeline",
    "can_share_dashboards",
    "can_sync_sources",
    "can_view_alerts",
    "can_view_charts",
    "can_view_connection",
    "can_view_connections",
    "can_view_dashboards",
    "can_view_dbt_models",
    "can_view_dbt_operation",
    "can_view_dbt_workspace",
    "can_view_flags",
    "can_view_invitations",
    "can_view_kpis",
    "can_view_master_task",
    "can_view_master_tasks",
    "can_view_metrics",
    "can_view_orgtasks",
    "can_view_orgusers",
    "can_view_pipeline",
    "can_view_pipeline_overview",
    "can_view_pipelines",
    "can_view_source",
    "can_view_sources",
    "can_view_task_progress",
    "can_view_usage_dashboard",
    "can_view_warehouse",
    "can_view_warehouse_data",
    "can_view_warehouses",
    "public",
]


def _refresh_redis():
    """Mirror auth.py: rewrite the role→permission cache so changes take effect without
    waiting for the next deploy. Best-effort — never block the migration on Redis."""
    try:
        from ddpui.auth import set_roles_and_permissions_in_redis
        from ddpui.utils.redis_client import RedisClient

        key = os.getenv("ROLE_PERMISSIONS_REDIS_KEY", "dalgo_permissions_key")
        set_roles_and_permissions_in_redis(RedisClient.get_instance(), key)
    except Exception as exc:  # pragma: no cover - infra dependent
        print(f"⚠️  could not refresh role permissions in Redis: {exc}")


def _set_role_permissions_exactly(RolePermission, Permission, role, slugs):
    """Replace a role's permission rows with exactly `slugs`."""
    RolePermission.objects.filter(role=role).delete()
    for slug in slugs:
        perm = Permission.objects.filter(slug=slug).first()
        if perm:
            RolePermission.objects.create(role=role, permission=perm)


def collapse_roles_forward(apps, schema_editor):
    Role = apps.get_model("ddpui", "Role")
    Permission = apps.get_model("ddpui", "Permission")
    RolePermission = apps.get_model("ddpui", "RolePermission")
    OrgUser = apps.get_model("ddpui", "OrgUser")
    Invitation = apps.get_model("ddpui", "Invitation")

    # roles come from seed fixtures, not migrations — no-op on a fresh/test DB
    account_manager = Role.objects.filter(slug="account-manager").first()
    if account_manager is None:
        print("RBAC v2: account-manager role not present; skipping (fresh DB)")
        return

    # 1. account-manager → admin (same pk/level, new slug + name)
    account_manager.slug = "admin"
    account_manager.name = "Admin"
    account_manager.level = 4
    account_manager.save()

    # 2. guest → member
    guest = Role.objects.filter(slug="guest").first()
    if guest:
        guest.slug = "member"
        guest.name = "Member"
        guest.level = 1
        guest.save()

    # 3. merge pipeline-manager into admin: repoint its users + pending invites, then drop it
    pipeline_manager = Role.objects.filter(slug="pipeline-manager").first()
    if pipeline_manager:
        OrgUser.objects.filter(new_role=pipeline_manager).update(new_role=account_manager)
        # invited_new_role is on_delete=CASCADE — repoint before delete or invites vanish
        Invitation.objects.filter(invited_new_role=pipeline_manager).update(
            invited_new_role=account_manager
        )
        RolePermission.objects.filter(role=pipeline_manager).delete()
        pipeline_manager.delete()

    # 4. analyst: strip infra write/run slugs (keep content + infra view)
    analyst = Role.objects.filter(slug="analyst").first()
    if analyst:
        RolePermission.objects.filter(
            role=analyst, permission__slug__in=ANALYST_INFRA_WRITE_REMOVE
        ).delete()

    # 5. member: content-view-only, set exactly
    member = Role.objects.filter(slug="member").first()
    if member:
        _set_role_permissions_exactly(RolePermission, Permission, member, MEMBER_SLUGS)

    _refresh_redis()


def collapse_roles_reverse(apps, schema_editor):
    """Best-effort restore of the pre-v2 four-role world. The original account-manager /
    pipeline-manager split per OrgUser is NOT recoverable — everyone merged into admin
    stays on the recreated account-manager role."""
    Role = apps.get_model("ddpui", "Role")
    Permission = apps.get_model("ddpui", "Permission")
    RolePermission = apps.get_model("ddpui", "RolePermission")

    admin = Role.objects.filter(slug="admin").first()
    if admin is None:
        print("RBAC v2 reverse: admin role not present; skipping")
        return

    # admin → account-manager
    admin.slug = "account-manager"
    admin.name = "Account Manager"
    admin.level = 4
    admin.save()

    # member → guest, restored to the old guest permission set
    member = Role.objects.filter(slug="member").first()
    if member:
        member.slug = "guest"
        member.name = "Guest"
        member.level = 1
        member.save()
        _set_role_permissions_exactly(RolePermission, Permission, member, OLD_GUEST_SLUGS)

    # analyst: re-grant the infra write/run slugs
    analyst = Role.objects.filter(slug="analyst").first()
    if analyst:
        for slug in ANALYST_INFRA_WRITE_REMOVE:
            perm = Permission.objects.filter(slug=slug).first()
            if perm and not RolePermission.objects.filter(role=analyst, permission=perm).exists():
                RolePermission.objects.create(role=analyst, permission=perm)

    # recreate pipeline-manager (fresh pk) with its old permission set
    if not Role.objects.filter(slug="pipeline-manager").exists():
        pm = Role.objects.create(slug="pipeline-manager", name="Pipeline Manager", level=3)
        _set_role_permissions_exactly(RolePermission, Permission, pm, OLD_PIPELINE_MANAGER_SLUGS)

    _refresh_redis()


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0164_alert_models"),
    ]

    operations = [
        migrations.RunPython(collapse_roles_forward, collapse_roles_reverse),
    ]
