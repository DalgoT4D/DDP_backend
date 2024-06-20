import os
import django
from django.core.management import call_command
from django.apps import apps

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


import pytest
from ddpui.models.org import Org
from ddpui.core.orgtaskfunctions import get_edr_send_report_task
from ddpui.models.tasks import Task

pytestmark = pytest.mark.django_db


@pytest.fixture(scope="session")
def seed_master_tasks_db(django_db_setup, django_db_blocker):
    with django_db_blocker.unblock():
        # Run the loaddata command to load the fixture
        call_command("loaddata", "tasks.json")


# ================================================================================


def test_seed_master_tasks(seed_master_tasks_db):
    """a test to seed the database"""
    assert Task.objects.count() == 10


# ================================================================================


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
