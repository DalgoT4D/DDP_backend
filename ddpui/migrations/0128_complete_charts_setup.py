from django.db import migrations, models
import django.db.models.deletion
from django.db import connection


def create_charts_tables(apps, schema_editor):
    """Create or update charts and chart_snapshots tables with all required columns"""
    with connection.cursor() as cursor:
        # Check if charts table exists
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'charts'
            );
        """
        )
        charts_exists = cursor.fetchone()[0]

        if not charts_exists:
            # Create charts table
            cursor.execute(
                """
                CREATE TABLE charts (
                    id SERIAL PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    description TEXT,
                    chart_type VARCHAR(20) NOT NULL,
                    computation_type VARCHAR(20) NOT NULL DEFAULT 'raw',
                    schema_name VARCHAR(255) NOT NULL,
                    table_name VARCHAR(255) NOT NULL,
                    x_axis_column VARCHAR(255),
                    y_axis_column VARCHAR(255),
                    dimension_column VARCHAR(255),
                    aggregate_column VARCHAR(255),
                    aggregate_function VARCHAR(50),
                    extra_dimension_column VARCHAR(255),
                    config JSONB DEFAULT '{}',
                    customizations JSONB DEFAULT '{}',
                    user_id INTEGER REFERENCES ddpui_orguser(id) ON DELETE SET NULL,
                    org_id INTEGER REFERENCES ddpui_org(id) ON DELETE CASCADE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            )
        else:
            # Ensure all columns exist
            cursor.execute(
                """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'charts';
            """
            )
            existing_columns = {row[0] for row in cursor.fetchall()}

            # Add any missing columns
            columns_to_add = {
                "title": "VARCHAR(255) NOT NULL DEFAULT 'Untitled Chart'",
                "description": "TEXT",
                "user_id": "INTEGER REFERENCES ddpui_orguser(id) ON DELETE SET NULL",
                "org_id": "INTEGER REFERENCES ddpui_org(id) ON DELETE CASCADE",
            }

            for col_name, col_def in columns_to_add.items():
                if col_name not in existing_columns:
                    cursor.execute(
                        f"""
                        ALTER TABLE charts 
                        ADD COLUMN {col_name} {col_def};
                    """
                    )

        # Check if chart_snapshots table exists
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'chart_snapshots'
            );
        """
        )
        snapshots_exists = cursor.fetchone()[0]

        if not snapshots_exists:
            # Create chart_snapshots table
            cursor.execute(
                """
                CREATE TABLE chart_snapshots (
                    id SERIAL PRIMARY KEY,
                    chart_id INTEGER REFERENCES charts(id) ON DELETE CASCADE,
                    data JSONB DEFAULT '{}',
                    echarts_config JSONB DEFAULT '{}',
                    query_hash VARCHAR(64),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP
                );
            """
            )

        # Create indexes if they don't exist
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS charts_org_user_idx ON charts(org_id, user_id);
            CREATE INDEX IF NOT EXISTS charts_type_idx ON charts(chart_type);
            CREATE INDEX IF NOT EXISTS charts_created_idx ON charts(created_at);
            CREATE INDEX IF NOT EXISTS snapshots_chart_created_idx ON chart_snapshots(chart_id, created_at);
            CREATE INDEX IF NOT EXISTS snapshots_hash_idx ON chart_snapshots(query_hash);
            CREATE INDEX IF NOT EXISTS snapshots_expires_idx ON chart_snapshots(expires_at);
        """
        )


def reverse_func(apps, schema_editor):
    """Don't reverse - keep tables for safety"""
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0127_fix_charts_table"),
    ]

    operations = [
        migrations.RunPython(create_charts_tables, reverse_func),
    ]
