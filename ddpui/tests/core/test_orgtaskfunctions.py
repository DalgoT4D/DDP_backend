import os
import json
from pathlib import Path
from django.apps import apps

import pytest
from ddpui.models.org import Org
from ddpui.core.orgtaskfunctions import get_edr_send_report_task
from ddpui.models.tasks import Task
from ddpui.tests.api_tests.test_pipeline_api import seed_master_tasks_db

pytestmark = pytest.mark.django_db


def test_get_edr_send_report_task(seed_master_tasks_db):
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
