"""
Data migration to reset the ddpui_orgfeatureflag primary key sequence.

The auto-increment sequence fell out of sync with the table data (likely due
to rows inserted with explicit IDs), causing IntegrityError on INSERT when
the sequence generated an already-used ID.
"""

from django.db import migrations


def reset_sequence(apps, schema_editor):
    if schema_editor.connection.vendor == "postgresql":
        schema_editor.execute(
            "SELECT setval("
            "  pg_get_serial_sequence('ddpui_orgfeatureflag', 'id'),"
            "  COALESCE((SELECT MAX(id) FROM ddpui_orgfeatureflag), 1)"
            ")"
        )


class Migration(migrations.Migration):

    dependencies = [
        ("ddpui", "0163_remove_dashboard_root_layout_components"),
    ]

    operations = [
        migrations.RunPython(reset_sequence, migrations.RunPython.noop),
    ]