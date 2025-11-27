# Generated manually on 2025-11-18 for restructuring ai_chat_log

from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0151_create_ai_chat_logging_tables"),
    ]

    operations = [
        # First, drop existing AIChatLog table indexes
        migrations.RunSQL(
            "DROP INDEX IF EXISTS ai_chat_log_org_id_466f09_idx;",
            reverse_sql="CREATE INDEX ai_chat_log_org_id_466f09_idx ON ai_chat_log (org_id, timestamp);",
        ),
        migrations.RunSQL(
            "DROP INDEX IF EXISTS ai_chat_log_user_id_3ad059_idx;",
            reverse_sql="CREATE INDEX ai_chat_log_user_id_3ad059_idx ON ai_chat_log (user_id, timestamp);",
        ),
        migrations.RunSQL(
            "DROP INDEX IF EXISTS ai_chat_log_session_58a148_idx;",
            reverse_sql="CREATE INDEX ai_chat_log_session_58a148_idx ON ai_chat_log (session_id);",
        ),
        # Remove old fields
        migrations.RemoveField(
            model_name="aichatlog",
            name="message_type",
        ),
        migrations.RemoveField(
            model_name="aichatlog",
            name="content",
        ),
        migrations.RemoveField(
            model_name="aichatlog",
            name="timestamp",
        ),
        # Add new fields
        migrations.AddField(
            model_name="aichatlog",
            name="user_prompt",
            field=models.TextField(help_text="The user's original question or prompt", default=""),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="aichatlog",
            name="ai_response",
            field=models.TextField(
                help_text="The AI's complete response to the user prompt", default=""
            ),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="aichatlog",
            name="request_timestamp",
            field=models.DateTimeField(
                help_text="When the user sent the request", default=django.utils.timezone.now
            ),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="aichatlog",
            name="response_timestamp",
            field=models.DateTimeField(
                default=django.utils.timezone.now, help_text="When the AI response was completed"
            ),
        ),
        # Add new indexes
        migrations.RunSQL(
            "CREATE INDEX ai_chat_log_org_response_time_idx ON ai_chat_log (org_id, response_timestamp);",
            reverse_sql="DROP INDEX IF EXISTS ai_chat_log_org_response_time_idx;",
        ),
        migrations.RunSQL(
            "CREATE INDEX ai_chat_log_user_response_time_idx ON ai_chat_log (user_id, response_timestamp);",
            reverse_sql="DROP INDEX IF EXISTS ai_chat_log_user_response_time_idx;",
        ),
        migrations.RunSQL(
            "CREATE INDEX ai_chat_log_session_id_idx ON ai_chat_log (session_id);",
            reverse_sql="DROP INDEX IF EXISTS ai_chat_log_session_id_idx;",
        ),
    ]
