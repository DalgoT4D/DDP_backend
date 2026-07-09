# Generated for Resource Sharing Task 1 — backfill owner + general access.
#
# 0168 added `owner`, `general_audience`, `general_level` with Django field
# defaults, so existing rows already hold "all_users"/"view" for the two
# general-access columns (Django backfills the default at column-add time).
# `owner` copies from `created_by` instead of a static default, so that needs
# an explicit data migration — this file does that, plus a belt-and-braces
# pass over general_audience/general_level in case any row slipped through
# without the default applied.

from django.db import migrations
from django.db.models import F


SHAREABLE_MODELS = ["Dashboard", "ReportSnapshot", "Metric", "KPI", "Alert"]
OWNER_BACKFILL_MODELS = SHAREABLE_MODELS + ["Chart"]


def backfill_owner_and_general_access(apps, schema_editor):
    """owner = created_by on all 6 models; general_audience/general_level
    defaulted on existing rows of the 5 shareable models.

    Does NOT touch Redis or call any permission-seeding helpers — that is a
    later task's responsibility.
    """
    for model_name in OWNER_BACKFILL_MODELS:
        model = apps.get_model("ddpui", model_name)
        model.objects.filter(owner_id__isnull=True, created_by_id__isnull=False).update(
            owner_id=F("created_by_id")
        )

    for model_name in SHAREABLE_MODELS:
        model = apps.get_model("ddpui", model_name)
        model.objects.filter(general_audience__isnull=True).update(general_audience="all_users")
        model.objects.filter(general_level__isnull=True).update(general_level="view")


def noop_reverse(apps, schema_editor):
    """No-op reverse — rolling back 0168 drops the columns anyway; there is
    nothing meaningful to undo about the backfilled values themselves."""


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0168_general_access_owner_and_org_prefs"),
    ]

    operations = [
        migrations.RunPython(backfill_owner_and_general_access, noop_reverse),
    ]
