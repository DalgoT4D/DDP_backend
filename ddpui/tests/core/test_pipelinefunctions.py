import pytest
from pathlib import Path
import os, json
from django.apps import apps
from ddpui.models.org import Org, OrgPrefectBlockv1
from ddpui.models.tasks import Task, OrgTask
from ddpui.ddpprefect import AIRBYTESERVER

pytestmark = pytest.mark.django_db

# fake connections ids used in test cases
CONNECTION_IDS = ["test-conn-id-1", "test-conn-id-2"]


# ================================================================================
@pytest.fixture
def seed_master_tasks():
    app_dir = os.path.join(Path(apps.get_app_config("ddpui").path), "..")
    seed_dir = os.path.abspath(os.path.join(app_dir, "seed"))
    f = open(os.path.join(seed_dir, "tasks.json"))
    tasks = json.load(f)
    for task in tasks:
        Task.objects.create(**task["fields"])


@pytest.fixture
def org_with_server_block():
    """a pytest fixture which creates an Org having an airbyte workspace and server block"""
    print("creating org with server block")
    org = Org.objects.create(
        airbyte_workspace_id="FAKE-WORKSPACE-ID", slug="test-org-slug"
    )
    OrgPrefectBlockv1.objects.create(
        block_type=AIRBYTESERVER,
        block_id="test-server-blk-id",
        block_name="test-server-blk",
        org=org,
    )
    yield org
    print("deleting org with server block")
    org.delete()


@pytest.fixture
def generate_sync_org_tasks(seed_master_tasks, org_with_server_block):
    """creates the sync org tasks with fake connections ids for the org"""
    task = Task.objects.filter(slug="airbyte-sync").first()
    for connection_id in CONNECTION_IDS:
        OrgTask.objects.create(
            task=task, connection_id=connection_id, org=org_with_server_block
        )


@pytest.fixture()
def generate_transform_org_tasks(seed_master_tasks, org_with_server_block):
    for task in Task.objects.filter(type__in=["dbt", "git"], is_system=True):
        OrgTask.objects.create(task=task, org=org_with_server_block)


# ================================================================================
