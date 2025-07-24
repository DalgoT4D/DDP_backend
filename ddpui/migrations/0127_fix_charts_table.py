from django.db import migrations, models
import django.db.models.deletion
from django.db import connection


def add_missing_columns(apps, schema_editor):
    """Add missing columns to existing charts table"""
    with connection.cursor() as cursor:
        # Get existing columns
        cursor.execute(
            """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'charts';
        """
        )
        existing_columns = {row[0] for row in cursor.fetchall()}

        # Add computation_type if missing
        if "computation_type" not in existing_columns:
            cursor.execute(
                """
                ALTER TABLE charts 
                ADD COLUMN computation_type VARCHAR(20) DEFAULT 'raw';
            """
            )

        # Add other potentially missing columns
        if "x_axis_column" not in existing_columns:
            cursor.execute(
                """
                ALTER TABLE charts 
                ADD COLUMN x_axis_column VARCHAR(255);
            """
            )

        if "y_axis_column" not in existing_columns:
            cursor.execute(
                """
                ALTER TABLE charts 
                ADD COLUMN y_axis_column VARCHAR(255);
            """
            )

        if "dimension_column" not in existing_columns:
            cursor.execute(
                """
                ALTER TABLE charts 
                ADD COLUMN dimension_column VARCHAR(255);
            """
            )

        if "aggregate_column" not in existing_columns:
            cursor.execute(
                """
                ALTER TABLE charts 
                ADD COLUMN aggregate_column VARCHAR(255);
            """
            )

        if "aggregate_function" not in existing_columns:
            cursor.execute(
                """
                ALTER TABLE charts 
                ADD COLUMN aggregate_function VARCHAR(50);
            """
            )

        if "extra_dimension_column" not in existing_columns:
            cursor.execute(
                """
                ALTER TABLE charts 
                ADD COLUMN extra_dimension_column VARCHAR(255);
            """
            )

        if "config" not in existing_columns:
            cursor.execute(
                """
                ALTER TABLE charts 
                ADD COLUMN config JSONB DEFAULT '{}';
            """
            )

        if "customizations" not in existing_columns:
            cursor.execute(
                """
                ALTER TABLE charts 
                ADD COLUMN customizations JSONB DEFAULT '{}';
            """
            )


def reverse_func(apps, schema_editor):
    """Reverse migration - remove added columns"""
    pass  # Keep columns for safety


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0126_merge_20250703_0642"),
    ]

    operations = [
        migrations.RunPython(add_missing_columns, reverse_func),
    ]
