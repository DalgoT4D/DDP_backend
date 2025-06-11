# ddpui/migrations/0122_set_uuid_in_orgtask
# courtesy of chatgpt

import uuid
from django.db import migrations


def assign_uuids(apps, schema_editor):
    OrgTask = apps.get_model("ddpui", "OrgTask")
    for row in OrgTask.objects.filter(uuid__isnull=True):
        row.uuid = uuid.uuid4()
        row.save(update_fields=["uuid"])


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0121_merge_20250513_1951"),
    ]

    operations = [
        migrations.RunPython(assign_uuids),
    ]
