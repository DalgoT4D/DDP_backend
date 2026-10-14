import os
import django
from django.core.management import call_command

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


import pytest
from ddpui.models.tasks import Task

pytestmark = pytest.mark.django_db


@pytest.fixture(scope="session")
def seed_master_tasks_db(django_db_setup, django_db_blocker):
    with django_db_blocker.unblock():
        call_command("loaddata", "tasks.json")


def test_seed_master_tasks(seed_master_tasks_db):
    """a test to seed the database"""
    assert Task.objects.count() == 12
