# Generated manually - Recreate CanvasLock table for dbt mapping

from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0144_orgtask_dbt"),
    ]

    operations = [
        # Drop existing table
        migrations.DeleteModel(
            name="CanvasLock",
        ),
        # Recreate with new structure
        migrations.CreateModel(
            name="CanvasLock",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("locked_at", models.DateTimeField(auto_now_add=True)),
                ("lock_token", models.CharField(max_length=36, unique=True)),
                ("expires_at", models.DateTimeField()),
                (
                    "created_at",
                    models.DateTimeField(auto_created=True, default=django.utils.timezone.now),
                ),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "dbt",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="canvas_lock",
                        to="ddpui.orgdbt",
                    ),
                ),
                (
                    "locked_by",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE, to="ddpui.orguser"
                    ),
                ),
            ],
        ),
    ]
