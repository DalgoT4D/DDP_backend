from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0160_metrics_alerts_consolidated"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(
                    sql="""
                    ALTER TABLE alert
                    DROP COLUMN IF EXISTS cron;
                    """,
                    reverse_sql="""
                    ALTER TABLE alert
                    ADD COLUMN IF NOT EXISTS cron character varying(100) NOT NULL DEFAULT '0 */5 * * *';
                    ALTER TABLE alert
                    ALTER COLUMN cron DROP DEFAULT;
                    """,
                ),
                migrations.RunSQL(
                    sql="""
                    ALTER TABLE alert_evaluation
                    DROP COLUMN IF EXISTS cron;
                    """,
                    reverse_sql="""
                    ALTER TABLE alert_evaluation
                    ADD COLUMN IF NOT EXISTS cron character varying(100) NOT NULL DEFAULT '0 */5 * * *';
                    ALTER TABLE alert_evaluation
                    ALTER COLUMN cron DROP DEFAULT;
                    """,
                ),
            ],
            state_operations=[],
        ),
    ]
