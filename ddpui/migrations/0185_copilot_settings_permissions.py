# Copilot v1.1 permission changes (seed loaddata never deletes rows, so the
# role-4 revocation must ship as a data migration to reach existing databases):
# 1. New permission can_manage_chat_with_data_settings, granted to
#    super-admin and admin (the Settings -> Copilot page).
# 2. Chat becomes admin-only: revoke can_use_chat_with_data from analyst.

from django.db import migrations

SETTINGS_PERM = {
    "name": "Can Manage Chat With Data Settings",
    "slug": "can_manage_chat_with_data_settings",
}
CHAT_PERM_SLUG = "can_use_chat_with_data"
ADMIN_ROLE_SLUGS = ["super-admin", "admin"]


def apply_copilot_permissions(apps, schema_editor):
    Role = apps.get_model("ddpui", "Role")
    Permission = apps.get_model("ddpui", "Permission")
    RolePermission = apps.get_model("ddpui", "RolePermission")

    # Fresh database: roles/permissions arrive via loaddata AFTER migrate, and
    # the updated seed files already carry these changes (perm pk 94, no
    # analyst chat grant). Creating the permission here first would collide
    # with the seed's explicit pk on the unique slug.
    if not Role.objects.exists():
        return

    settings_perm, _ = Permission.objects.get_or_create(
        slug=SETTINGS_PERM["slug"], defaults={"name": SETTINGS_PERM["name"]}
    )
    for role in Role.objects.filter(slug__in=ADMIN_ROLE_SLUGS):
        RolePermission.objects.get_or_create(role=role, permission=settings_perm)

    # fresh databases have no seeds yet at migrate time — nothing to revoke
    chat_perm = Permission.objects.filter(slug=CHAT_PERM_SLUG).first()
    if chat_perm:
        RolePermission.objects.filter(role__slug="analyst", permission=chat_perm).delete()


def reverse_copilot_permissions(apps, schema_editor):
    Role = apps.get_model("ddpui", "Role")
    Permission = apps.get_model("ddpui", "Permission")
    RolePermission = apps.get_model("ddpui", "RolePermission")

    settings_perm = Permission.objects.filter(slug=SETTINGS_PERM["slug"]).first()
    if settings_perm:
        RolePermission.objects.filter(permission=settings_perm).delete()
        settings_perm.delete()

    chat_perm = Permission.objects.filter(slug=CHAT_PERM_SLUG).first()
    analyst = Role.objects.filter(slug="analyst").first()
    if chat_perm and analyst:
        RolePermission.objects.get_or_create(role=analyst, permission=chat_perm)


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0184_chatwithdataorgmemory"),
    ]

    operations = [
        migrations.RunPython(apply_copilot_permissions, reverse_copilot_permissions),
    ]
