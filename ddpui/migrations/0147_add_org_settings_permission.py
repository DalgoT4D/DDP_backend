# Generated migration for organization settings permission

from django.db import migrations


def add_org_settings_permission(apps, schema_editor):
    """
    Add can_manage_org_settings permission for Account Manager role
    """
    RolePermission = apps.get_model("ddpui", "RolePermission")
    Role = apps.get_model("ddpui", "Role")
    Permission = apps.get_model("ddpui", "Permission")

    # Create the permission
    permission, created = Permission.objects.get_or_create(
        slug="can_manage_org_settings",
        defaults={"slug": "can_manage_org_settings", "name": "Can manage organization settings"},
    )

    if created:
        print("✅ Created can_manage_org_settings permission")
    else:
        print("ℹ️  can_manage_org_settings permission already exists")

    # Get Account Manager role
    try:
        account_manager_role = Role.objects.get(slug="account-manager")
    except Role.DoesNotExist:
        print("❌ Account Manager role not found")
        return

    # Add permission to Account Manager role
    role_permission, created = RolePermission.objects.get_or_create(
        role=account_manager_role,
        permission=permission,
        defaults={"role": account_manager_role, "permission": permission},
    )

    if created:
        print(f"✅ Added can_manage_org_settings to {account_manager_role.name}")
    else:
        print(f"ℹ️  {account_manager_role.name} already has can_manage_org_settings")


def reverse_org_settings_permission(apps, schema_editor):
    """
    Remove the organization settings permission
    """
    RolePermission = apps.get_model("ddpui", "RolePermission")
    Permission = apps.get_model("ddpui", "Permission")

    # Remove the permission (this will also remove role-permission relationships)
    try:
        permission = Permission.objects.get(slug="can_manage_org_settings")
        permission.delete()
        print("✅ Removed can_manage_org_settings permission")
    except Permission.DoesNotExist:
        print("ℹ️  can_manage_org_settings permission was not found")


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0143_merge_20251117_1640"),
    ]

    operations = [
        migrations.RunPython(add_org_settings_permission, reverse_org_settings_permission),
    ]
