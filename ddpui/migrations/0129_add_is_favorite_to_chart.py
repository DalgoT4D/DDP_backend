from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0128_complete_charts_setup"),
    ]

    operations = [
        migrations.RunSQL(
            """
            ALTER TABLE charts 
            ADD COLUMN IF NOT EXISTS is_favorite BOOLEAN DEFAULT FALSE;
            """,
            reverse_sql="ALTER TABLE charts DROP COLUMN IF EXISTS is_favorite;",
        ),
    ]
