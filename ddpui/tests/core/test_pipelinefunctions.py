from unittest.mock import patch, Mock
import pytest
from pathlib import Path
import os, json
from django.apps import apps
from ddpui.models.org import Org, OrgPrefectBlockv1, OrgWarehouse
from ddpui.models.tasks import Task, OrgTask, OrgDataFlowv1, DataflowOrgTask
from ddpui.ddpprefect import DBTCLIPROFILE, AIRBYTESERVER
from ddpui.ddpprefect.schema import (
    PrefectFlowAirbyteConnection2,
)
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.core.pipelinefunctions import pipeline_sync_tasks, pipeline_dbt_git_tasks

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
    for task in Task.objects.filter(type__in=["dbt", "git"]).all():
        OrgTask.objects.create(task=task, org=org_with_server_block)


# ================================================================================


def test_pipeline_sync_tasks_success(org_with_server_block, generate_sync_org_tasks):
    """tests if function successfully returns all of sync tasks config for sync org tasks"""
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


def test_pipeline_sync_tasks_success2(org_with_server_block, generate_sync_org_tasks):
    """tests if function returns only those of sync tasks config that match with org tasks"""
    connections = [
        PrefectFlowAirbyteConnection2(id=conn_id, seq=(i + 1))
        for i, conn_id in enumerate(CONNECTION_IDS)
    ]

    # add another connection id not present in our org tasks
    connections.append(
        PrefectFlowAirbyteConnection2(
            id="some-fake-conn-id-123", seq=len(connections) + 1
        )
    )
    server_block = OrgPrefectBlockv1.objects.filter(org=org_with_server_block).first()
    (org_tasks, task_configs), error = pipeline_sync_tasks(
        org_with_server_block, connections, server_block
    )

    assert len(org_tasks) == len(CONNECTION_IDS)
    assert len(task_configs) == len(CONNECTION_IDS)
    assert len(task_configs) != len(connections)
    assert task_configs[0]["connection_id"] == CONNECTION_IDS[0]
    assert task_configs[1]["connection_id"] == CONNECTION_IDS[1]


def test_pipeline_dbt_git_tasks_success(
    org_with_server_block, generate_transform_org_tasks
):
    """tests if function returns all configs for the org tasks related to git & dbt"""

    cli_profile_block = OrgPrefectBlockv1.objects.create(
        block_type=DBTCLIPROFILE,
        block_id="test-cli-profile-blk-id",
        block_name="test-cli-profile-blk",
        org=org_with_server_block,
    )

    dbt_project_params = DbtProjectParams(
        dbt_env_dir="test-dir",
        dbt_binary="test_dir",
        project_dir="test-dir",
        target="prod",
        dbt_repo_dir="test-dir",
    )

    (org_tasks, task_configs), error = pipeline_dbt_git_tasks(
        org_with_server_block, cli_profile_block, dbt_project_params
    )

    dbt_git_tasks = Task.objects.filter(type__in=["dbt", "git"]).all()
    assert len(task_configs) == len(dbt_git_tasks)
    assert len(org_tasks) == len(org_tasks)

    seqs = [t["seq"] for t in task_configs]
    seqs.sort()
    assert seqs == [i + 1 for i in range(len(dbt_git_tasks))]


def test_pipeline_dbt_git_tasks_success2(
    org_with_server_block, generate_transform_org_tasks
):
    """tests the sequence of tasks based on a different start offset"""
    offset = 2  # means there were 2 airbyte syncs

    cli_profile_block = OrgPrefectBlockv1.objects.create(
        block_type=DBTCLIPROFILE,
        block_id="test-cli-profile-blk-id",
        block_name="test-cli-profile-blk",
        org=org_with_server_block,
    )

    dbt_project_params = DbtProjectParams(
        dbt_env_dir="test-dir",
        dbt_binary="test_dir",
        project_dir="test-dir",
        target="prod",
        dbt_repo_dir="test-dir",
    )

    (org_tasks, task_configs), error = pipeline_dbt_git_tasks(
        org_with_server_block, cli_profile_block, dbt_project_params, offset
    )

    dbt_git_tasks = Task.objects.filter(type__in=["dbt", "git"]).all()
    assert len(task_configs) == len(dbt_git_tasks)
    assert len(org_tasks) == len(org_tasks)

    seqs = [t["seq"] for t in task_configs]
    seqs.sort()
    assert seqs == [i + 1 for i in range(offset, offset + len(dbt_git_tasks))]
