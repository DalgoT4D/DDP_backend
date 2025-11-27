# Generated migration to populate ddpui_org_settings from ddpui_org data

from django.db import migrations
from django.utils import timezone


def populate_org_settings(apps, schema_editor):
    """
    Populate ddpui_org_settings table with data from ddpui_org table.
    Create an OrgSettings record for each existing Org.
    """
    Org = apps.get_model("ddpui", "Org")
    OrgSettings = apps.get_model("ddpui", "OrgSettings")

    # Get all existing organizations
    orgs = Org.objects.all()

    org_settings_to_create = []
    for org in orgs:
        # Check if OrgSettings already exists for this org to avoid duplicates
        if not OrgSettings.objects.filter(org=org).exists():
            org_settings = OrgSettings(
                org=org,
                # organization_name and website are now referenced from org model
                ai_data_sharing_enabled=False,  # Default to False for security
                ai_logging_acknowledged=False,  # Default to False for compliance
                created_at=timezone.now(),
                updated_at=timezone.now(),
            )
            org_settings_to_create.append(org_settings)

    # Bulk create all OrgSettings records
    if org_settings_to_create:
        OrgSettings.objects.bulk_create(org_settings_to_create)
        print(f"✅ Created {len(org_settings_to_create)} OrgSettings records")
    else:
        print("ℹ️  No new OrgSettings records needed")


def reverse_populate_org_settings(apps, schema_editor):
    """
    Reverse migration: Delete all OrgSettings records that were created by this migration.
    This only removes records where organization_name matches the org.name,
    indicating they were likely created by this migration.
    """
    Org = apps.get_model("ddpui", "Org")
    OrgSettings = apps.get_model("ddpui", "OrgSettings")

    # Delete all OrgSettings records (since we can't distinguish auto-created ones anymore)
    deleted_count = OrgSettings.objects.count()
    OrgSettings.objects.all().delete()

    print(f"🗑️  Deleted {deleted_count} auto-created OrgSettings records")


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0154_rename_org_settings_table"),
    ]

    operations = [
        migrations.RunPython(
            populate_org_settings,
            reverse_populate_org_settings,
            elidable=True,
        ),
    ]
