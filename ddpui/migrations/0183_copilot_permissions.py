# Chat with Data is admin-only: can_use_chat_with_data (which also gates the
# Settings -> Copilot page) belongs to super-admin and admin alone. Seed
# loaddata never deletes rows, so revoking an earlier analyst grant must ship
# as a data migration to reach existing databases.

from django.db import migrations

CHAT_PERM_SLUG = "can_use_chat_with_data"


def revoke_analyst_chat(apps, schema_editor):
    Permission = apps.get_model("ddpui", "Permission")
    RolePermission = apps.get_model("ddpui", "RolePermission")

    # fresh databases have no seeds yet at migrate time — nothing to revoke
    chat_perm = Permission.objects.filter(slug=CHAT_PERM_SLUG).first()
    if chat_perm:
        RolePermission.objects.filter(role__slug="analyst", permission=chat_perm).delete()


def restore_analyst_chat(apps, schema_editor):
    Role = apps.get_model("ddpui", "Role")
    Permission = apps.get_model("ddpui", "Permission")
    RolePermission = apps.get_model("ddpui", "RolePermission")

    chat_perm = Permission.objects.filter(slug=CHAT_PERM_SLUG).first()
    analyst = Role.objects.filter(slug="analyst").first()
    if chat_perm and analyst:
        RolePermission.objects.get_or_create(role=analyst, permission=chat_perm)


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0182_chat_with_data"),
    ]

    operations = [
        migrations.RunPython(revoke_analyst_chat, restore_analyst_chat),
    ]
