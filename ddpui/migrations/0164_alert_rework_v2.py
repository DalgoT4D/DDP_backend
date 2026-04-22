"""
Batch 3 — Alerts overhaul.

  - Adds `alert_type` enum: threshold (Metric-backed) / rag (KPI-backed) /
    standalone (ad-hoc SQL).
  - Adds `pipeline_triggers` JSON list — inferred by default for threshold /
    rag alerts, explicit for standalone. When empty, the evaluator falls back
    to "any pipeline whose output feeds the alert's source tables."
  - Adds `notification_cooldown_days` — null means "notify only on state
    change." An integer N means "while still firing, re-notify every N days."
  - Restructures `recipients` from a flat email list to a typed list of
    ``{"type": "email" | "user", "ref": str}`` — v1 still accepts plain email
    strings on read for backwards-compat, but writers always emit the typed
    shape.
  - Adds `AlertEvaluation.notification_sent` — we still log the evaluation
    even when the cooldown suppressed the outgoing notification.
  - Adds `AlertEvaluation.last_pipeline_update` — the timestamp of the
    pipeline run that fed the data the alert evaluated against.
"""

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("ddpui", "0163_kpis_alerts_v2_schema"),
    ]

    operations = [
        migrations.AddField(
            model_name="alert",
            name="alert_type",
            field=models.CharField(
                max_length=20,
                default="standalone",
                choices=[
                    ("threshold", "Metric Threshold"),
                    ("rag", "KPI RAG"),
                    ("standalone", "Standalone SQL"),
                ],
            ),
        ),
        migrations.AddField(
            model_name="alert",
            name="pipeline_triggers",
            field=models.JSONField(default=list),
        ),
        migrations.AddField(
            model_name="alert",
            name="notification_cooldown_days",
            field=models.IntegerField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name="alertevaluation",
            name="notification_sent",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="alertevaluation",
            name="last_pipeline_update",
            field=models.DateTimeField(null=True, blank=True),
        ),
    ]
