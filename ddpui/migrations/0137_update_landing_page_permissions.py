# Generated migration for landing page permissions update

from django.db import migrations


def update_landing_page_permissions(apps, schema_editor):
    """
    Update landing page permissions:
    1. Remove can_manage_org_default_dashboard from Analyst role
    2. Add can_create_dashboards to Guest role (for personal landing pages)
    """
    RolePermission = apps.get_model("ddpui", "RolePermission")
    Role = apps.get_model("ddpui", "Role")
    Permission = apps.get_model("ddpui", "Permission")

    # Get roles
    try:
        analyst_role = Role.objects.get(slug="analyst")
        guest_role = Role.objects.get(slug="guest")
    except Role.DoesNotExist:
        print("Required roles not found")
        return

    # Get permissions
    try:
        org_default_perm = Permission.objects.get(slug="can_manage_org_default_dashboard")
        create_dashboard_perm = Permission.objects.get(slug="can_create_dashboards")
    except Permission.DoesNotExist:
        print("Required permissions not found")
        return

    # 1. Remove org default permission from Analyst
    analyst_org_perm = RolePermission.objects.filter(role=analyst_role, permission=org_default_perm)
    if analyst_org_perm.exists():
        analyst_org_perm.delete()
        print(f"✅ Removed can_manage_org_default_dashboard from {analyst_role.name}")

    # 2. Add create dashboard permission to Guest (if not exists)
    guest_create_perm, created = RolePermission.objects.get_or_create(
        role=guest_role,
        permission=create_dashboard_perm,
        defaults={"role": guest_role, "permission": create_dashboard_perm},
    )

    if created:
        print(f"✅ Added can_create_dashboards to {guest_role.name}")
    else:
        print(f"ℹ️  {guest_role.name} already has can_create_dashboards")


def reverse_landing_page_permissions(apps, schema_editor):
    """
    Reverse the permission changes
    """
    RolePermission = apps.get_model("ddpui", "RolePermission")
    Role = apps.get_model("ddpui", "Role")
    Permission = apps.get_model("ddpui", "Permission")

    # Get roles and permissions
    try:
        analyst_role = Role.objects.get(slug="analyst")
        guest_role = Role.objects.get(slug="guest")
        org_default_perm = Permission.objects.get(slug="can_manage_org_default_dashboard")
        create_dashboard_perm = Permission.objects.get(slug="can_create_dashboards")

        # Add back org default permission to Analyst
        RolePermission.objects.get_or_create(role=analyst_role, permission=org_default_perm)

        # Remove create dashboard permission from Guest
        RolePermission.objects.filter(role=guest_role, permission=create_dashboard_perm).delete()

        print("✅ Reversed permission changes")

    except (Role.DoesNotExist, Permission.DoesNotExist):
        print("Could not reverse - roles or permissions not found")


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0136_add_landing_page_fields"),
    ]

    operations = [
        migrations.RunPython(update_landing_page_permissions, reverse_landing_page_permissions),
    ]
