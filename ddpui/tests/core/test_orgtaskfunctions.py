import os
import json
from pathlib import Path
from django.apps import apps

import pytest
from ddpui.models.org import Org
from ddpui.core.orgtaskfunctions import get_edr_send_report_task
from ddpui.models.tasks import Task

pytestmark = pytest.mark.django_db


@pytest.fixture
def seed_master_tasks():
    """seeds the tasks table"""
    app_dir = os.path.join(Path(apps.get_app_config("ddpui").path), "..")
    seed_dir = os.path.abspath(os.path.join(app_dir, "seed"))
    with open(os.path.join(seed_dir, "tasks.json"), encoding="utf8") as f:
        tasks = json.load(f)
        for task in tasks:
            if not Task.objects.filter(slug=task["fields"]["slug"]).exists():
                Task.objects.create(**task["fields"])


def test_get_edr_send_report_task(seed_master_tasks):
    """test get_edr_send_report_task"""

    org = Org.objects.create(name="del", slug="del")
    assert get_edr_send_report_task(org) is None
    assert get_edr_send_report_task(org, create=False) is None
    orgtask = get_edr_send_report_task(org, create=True)
    assert orgtask is not None
    assert orgtask.org == org
    assert orgtask.task.slug == "generate-edr"
    assert orgtask.uuid is not None
    assert orgtask.parameters["options"]["profiles-dir"] == "elementary_profiles"
    assert (
        orgtask.parameters["options"]["bucket-file-path"]
        == "reports/del.TODAYS_DATE.html"
    )
    assert get_edr_send_report_task(org) is not None
