from django.db import migrations, models
from ddpui.ddpprefect import (
    default_queue_config,
    DDP_WORK_QUEUE,
    MANUL_DBT_WORK_QUEUE,
    SCHEDULED_PIPELINE_QUEUE,
    CONNECTION_SYNC_QUEUE,
    TRANSFORM_TASK_QUEUE,
)


def set_queue_config_for_existing_orgs(apps, schema_editor):
    """Set queue_config for all existing orgs to use old infra (ddp) for connection syncs"""
    Org = apps.get_model("ddpui", "Org")
    Org.objects.all().update(
        queue_config={
            SCHEDULED_PIPELINE_QUEUE: DDP_WORK_QUEUE,
            CONNECTION_SYNC_QUEUE: DDP_WORK_QUEUE,
            TRANSFORM_TASK_QUEUE: MANUL_DBT_WORK_QUEUE,
        }
    )


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0147_add_last_visited_transform_tab_to_user_preferences"),
    ]

    operations = [
        migrations.AddField(
            model_name="org",
            name="queue_config",
            field=models.JSONField(
                default=default_queue_config,
                help_text="Queue configuration for different task types (scheduled_pipeline_queue, connection_sync_queue, transform_task_queue)",
            ),
        ),
        migrations.RunPython(set_queue_config_for_existing_orgs, migrations.RunPython.noop),
    ]
