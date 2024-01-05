from unittest.mock import patch, Mock
import pytest
from ddpui.models.org import Org, OrgPrefectBlockv1, OrgWarehouse
from ddpui.models.tasks import Task, OrgTask, OrgDataFlowv1, DataflowOrgTask
from ddpui.ddpprefect import DBTCLIPROFILE, AIRBYTESERVER
from ddpui.ddpprefect.schema import (
    PrefectFlowAirbyteConnection2,
    PrefectDbtTaskSetup,
    PrefectShellTaskSetup,
    PrefectAirbyteSyncTaskSetup,
)
from ddpui.core.pipelinefunctions import pipeline_sync_tasks

pytestmark = pytest.mark.django_db

# fake connections ids used in test cases
CONNECTION_IDS = ["test-conn-id-1", "test-conn-id-2"]


# ================================================================================
@pytest.fixture
def create_master_sync_task():
    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)


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
def generate_sync_org_tasks(create_master_sync_task, org_with_server_block):
    """creates the sync org tasks with fake connections ids for the org"""
    task = Task.objects.filter(slug="airbyte-sync").first()
    for connection_id in CONNECTION_IDS:
        OrgTask.objects.create(
            task=task, connection_id=connection_id, org=org_with_server_block
        )


# ================================================================================


def test_pipeline_sync_tasks_success(org_with_server_block, generate_sync_org_tasks):
    """tests if it successfully returns all of sync tasks config for sync org tasks"""
    connections = [
        PrefectFlowAirbyteConnection2(id=conn_id, seq=(i + 1))
        for i, conn_id in enumerate(CONNECTION_IDS)
    ]
    server_block = OrgPrefectBlockv1.objects.filter(org=org_with_server_block).first()
    (org_tasks, task_configs), error = pipeline_sync_tasks(
        org_with_server_block, connections, server_block
    )

    assert len(org_tasks) == len(CONNECTION_IDS)
    assert len(task_configs) == len(CONNECTION_IDS)
    assert task_configs[0]["connection_id"] == CONNECTION_IDS[0]
    assert task_configs[1]["connection_id"] == CONNECTION_IDS[1]
