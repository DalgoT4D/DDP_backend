import pytest
from pathlib import Path
import os, json
from django.apps import apps
from django.contrib.auth.models import User
from unittest.mock import Mock, patch
from ninja.errors import HttpError

from ddpui.models.org import Org, OrgPrefectBlockv1, OrgDataFlowv1, OrgDbt
from ddpui.models.tasks import Task, OrgTask, TaskLock, TaskLockStatus, DataflowOrgTask, TaskType
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.ddpprefect import (
    AIRBYTESERVER,
    DBTCORE,
    SECRET,
    FLOW_RUN_RUNNING_STATE_TYPE,
    FLOW_RUN_COMPLETED_STATE_TYPE,
)
from ddpui.models.org_user import OrgUser, Role
from ddpui.core.pipelinefunctions import (
    fetch_pipeline_lock_v1,
    lock_tasks_for_dataflow,
    setup_dbt_core_task_config,
)
from ddpui.core.orchestrate.pipeline_service import PipelineService
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.ddpprefect.schema import PrefectDataFlowOrgTasks
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.utils.constants import TASK_AIRBYTESYNC, TASK_DBTRUN, TASK_GITCLONE, TASK_GITPULL, TASK_GENERATE_EDR

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
def authuser():
    """a django User object"""
    user = User.objects.create(
        username="tempusername", email="tempuseremail", password="tempuserpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org_with_server_block():
    """a pytest fixture which creates an Org having an airbyte workspace and server block"""
    print("creating org with server block")
    org = Org.objects.create(airbyte_workspace_id="FAKE-WORKSPACE-ID", slug="test-org-slug")
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
def orguser(authuser, org_with_server_block):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org_with_server_block,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def generate_sync_org_tasks(seed_master_tasks, org_with_server_block):
    """creates the sync org tasks with fake connections ids for the org"""
    task = Task.objects.filter(slug="airbyte-sync").first()
    for connection_id in CONNECTION_IDS:
        OrgTask.objects.create(task=task, connection_id=connection_id, org=org_with_server_block)


@pytest.fixture()
def generate_transform_org_tasks(seed_master_tasks, org_with_server_block):
    for task in Task.objects.filter(type__in=[TaskType.DBT, TaskType.GIT], is_system=True):
        OrgTask.objects.create(task=task, org=org_with_server_block)


@pytest.fixture
def test_dataflow(org_with_server_block, generate_sync_org_tasks, generate_transform_org_tasks):
    dataflow = OrgDataFlowv1.objects.create(
        org=org_with_server_block,
        name="test-dataflow-name",
        deployment_id="test-deployment-id",
        deployment_name="test-deployment-name",
        cron=None,
        dataflow_type="orchestrate",
    )
    yield dataflow
    dataflow.delete()


# ================================================================================


def test_fetch_pipeline_lock_v1_no_lock(test_dataflow):
    result = fetch_pipeline_lock_v1(test_dataflow, None)
    assert result is None


def test_fetch_pipeline_lock_v1_lock_no_flow_run_id(test_dataflow: OrgDataFlowv1, orguser: OrgUser):
    lock = TaskLock.objects.create(
        orgtask=OrgTask.objects.filter(org=test_dataflow.org).first(),
        locked_by=orguser,
        locking_dataflow=test_dataflow,
    )
    result = fetch_pipeline_lock_v1(test_dataflow, lock)
    assert result == {
        "lockedBy": lock.locked_by.user.email,
        "lockedAt": lock.locked_at,
        "flowRunId": lock.flow_run_id,
        "status": TaskLockStatus.QUEUED,
    }


def test_fetch_pipeline_lock_v1_flow_run_scheduled(test_dataflow: OrgDataFlowv1, orguser: OrgUser):
    with patch("ddpui.ddpprefect.prefect_service.get_flow_run") as mock_get_flow_run:
        lock = TaskLock.objects.create(
            orgtask=OrgTask.objects.filter(org=test_dataflow.org).first(),
            flow_run_id="some_flow_run_id",
            locked_by=orguser,
            locking_dataflow=test_dataflow,
        )
        mock_get_flow_run.return_value = {
            "state_type": "SCHEDULED",
            "id": lock.flow_run_id,
        }
        result = fetch_pipeline_lock_v1(test_dataflow, lock)
        assert result == {
            "lockedBy": lock.locked_by.user.email,
            "lockedAt": lock.locked_at,
            "flowRunId": lock.flow_run_id,
            "status": TaskLockStatus.QUEUED,
        }
        assert result["flowRunId"] == "some_flow_run_id"


def test_fetch_pipeline_lock_v1_flow_run_pending(test_dataflow: OrgDataFlowv1, orguser: OrgUser):
    with patch("ddpui.ddpprefect.prefect_service.get_flow_run") as mock_get_flow_run:
        lock = TaskLock.objects.create(
            orgtask=OrgTask.objects.filter(org=test_dataflow.org).first(),
            flow_run_id="some_flow_run_id",
            locked_by=orguser,
            locking_dataflow=test_dataflow,
        )
        mock_get_flow_run.return_value = {
            "state_type": "PENDING",
            "id": lock.flow_run_id,
        }
        result = fetch_pipeline_lock_v1(test_dataflow, lock)
        assert result == {
            "lockedBy": lock.locked_by.user.email,
            "lockedAt": lock.locked_at,
            "flowRunId": lock.flow_run_id,
            "status": TaskLockStatus.QUEUED,
        }
        assert result["flowRunId"] == "some_flow_run_id"


def test_fetch_pipeline_lock_v1_flow_run_running(test_dataflow: OrgDataFlowv1, orguser: OrgUser):
    flow_run_id = "some_flow_run_id"
    lock = TaskLock.objects.create(
        orgtask=OrgTask.objects.filter(org=test_dataflow.org).first(),
        flow_run_id=flow_run_id,
        locked_by=orguser,
        locking_dataflow=test_dataflow,
    )
    PrefectFlowRun.objects.create(
        deployment_id="fake-deployment-id",
        flow_run_id=flow_run_id,
        name="airbyte-sync-run",
        start_time="2022-01-01",
        expected_start_time="2022-01-01",
        total_run_time=12,
        status=FLOW_RUN_RUNNING_STATE_TYPE,
        state_name="Running",
    )
    result = fetch_pipeline_lock_v1(test_dataflow, lock)
    assert result == {
        "lockedBy": lock.locked_by.user.email,
        "lockedAt": lock.locked_at,
        "flowRunId": lock.flow_run_id,
        "status": TaskLockStatus.RUNNING,
    }
    assert result["flowRunId"] == "some_flow_run_id"


def test_fetch_pipeline_lock_v1_flow_run_completed(test_dataflow: OrgDataFlowv1, orguser: OrgUser):
    flow_run_id = "some_flow_run_id"
    lock = TaskLock.objects.create(
        orgtask=OrgTask.objects.filter(org=test_dataflow.org).first(),
        flow_run_id=flow_run_id,
        locked_by=orguser,
        locking_dataflow=test_dataflow,
    )
    PrefectFlowRun.objects.create(
        deployment_id="fake-deployment-id",
        flow_run_id=flow_run_id,
        name="airbyte-sync-run",
        start_time="2022-01-01",
        expected_start_time="2022-01-01",
        total_run_time=12,
        status=FLOW_RUN_COMPLETED_STATE_TYPE,
        state_name="Completed",
    )
    result = fetch_pipeline_lock_v1(test_dataflow, lock)
    assert result == {
        "lockedBy": lock.locked_by.user.email,
        "lockedAt": lock.locked_at,
        "flowRunId": lock.flow_run_id,
        "status": TaskLockStatus.COMPLETED,
    }
    assert result["flowRunId"] == "some_flow_run_id"


def test_fetch_pipeline_lock_v1_locking_dataflow_not_equal(
    test_dataflow: OrgDataFlowv1, orguser: OrgUser
):
    flow_run_id = "some_flow_run_id"
    other_dataflow = OrgDataFlowv1.objects.create(
        org=test_dataflow.org,
        name="other-dataflow-name",
        deployment_id="other-deployment-id",
        deployment_name="other-deployment-name",
        cron=None,
        dataflow_type="orchestrate",
    )
    lock = TaskLock.objects.create(
        orgtask=OrgTask.objects.filter(org=test_dataflow.org).first(),
        flow_run_id=flow_run_id,
        locked_by=orguser,
        locking_dataflow=other_dataflow,
    )
    PrefectFlowRun.objects.create(
        deployment_id="fake-deployment-id",
        flow_run_id=flow_run_id,
        name="airbyte-sync-run",
        start_time="2022-01-01",
        expected_start_time="2022-01-01",
        total_run_time=12,
        status=FLOW_RUN_COMPLETED_STATE_TYPE,
        state_name="Completed",
    )
    result = fetch_pipeline_lock_v1(test_dataflow, lock)
    assert result == {
        "lockedBy": lock.locked_by.user.email,
        "lockedAt": lock.locked_at,
        "flowRunId": lock.flow_run_id,
        "status": TaskLockStatus.LOCKED,
    }
    assert result["flowRunId"] == "some_flow_run_id"


def test_lock_tasks_for_dataflow(test_dataflow: OrgDataFlowv1, orguser: OrgUser):
    """test lock_tasks_for_dataflow function"""

    # create orgtasks for the dataflow
    orgtask1 = OrgTask.objects.create(
        task=Task.objects.filter(slug=TASK_AIRBYTESYNC).first(),
        org=test_dataflow.org,
        connection_id="some-conn-id",
    )
    orgtask2 = OrgTask.objects.create(
        task=Task.objects.filter(slug=TASK_DBTRUN).first(),
        org=test_dataflow.org,
    )

    # create mapping
    for orgtask in [orgtask1, orgtask2]:
        DataflowOrgTask.objects.create(orgtask=orgtask, dataflow=test_dataflow)

    lock_tasks_for_dataflow(orguser, test_dataflow, [orgtask1, orgtask2])

    assert TaskLock.objects.filter(orgtask__in=[orgtask1, orgtask2]).count() == 2

    with pytest.raises(HttpError) as exc:
        lock_tasks_for_dataflow(orguser, test_dataflow, [orgtask1, orgtask2])

    assert exc.value.status_code == 400
    assert str(exc.value) == f"{orguser.user.email} is running this pipeline right now"


# =============================================================================
# PipelineService._build_transform_tasks — git injection for EDR-only pipelines
# =============================================================================


def _make_edr_org(is_eks: bool):
    """Create an org + EDR OrgTask. Queue config sets is_workpool_eks per flag."""
    orgdbt = OrgDbt.objects.create(
        project_dir="test-org/dbtrepo",
        target_type="postgres",
        default_schema="public",
        gitrepo_url="https://github.com/test/repo.git",
    )
    workpool = "eks-pool" if is_eks else "default-pool"
    queue_config = {
        "scheduled_pipeline_queue": {
            "name": "ddp",
            "workpool": workpool,
            "is_workpool_eks": is_eks,
        },
        "connection_sync_queue": {"name": "ddp", "workpool": workpool, "is_workpool_eks": is_eks},
        "transform_task_queue": {"name": "ddp", "workpool": workpool, "is_workpool_eks": is_eks},
        "edr_queue": {"name": "edr", "workpool": workpool, "is_workpool_eks": is_eks},
    }
    org = Org.objects.create(slug="edr-test-org", dbt=orgdbt, queue_config=queue_config)
    edr_task, _ = Task.objects.get_or_create(
        slug=TASK_GENERATE_EDR, defaults={"type": TaskType.EDR, "label": "EDR generate"}
    )
    edr_orgtask = OrgTask.objects.create(org=org, task=edr_task)
    return org, edr_orgtask


@patch("ddpui.core.orchestrate.pipeline_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.core.orchestrate.pipeline_service.pipeline_with_orgtasks")
def test_build_transform_tasks_edr_only_non_eks_adds_git_pull(
    mock_pipeline_with_orgtasks,
    mock_gather_dbt_project_params,
    seed_master_tasks,
):
    """EDR-only pipeline on non-EKS: git-pull is prepended automatically."""
    org, edr_orgtask = _make_edr_org(is_eks=False)
    mock_gather_dbt_project_params.return_value = Mock(
        project_dir="/mnt/clientdbts/edr-test-org/dbtrepo",
        clients_base_dir="/mnt/clientdbts",
        project_dir_relative="edr-test-org/dbtrepo",
    )
    mock_pipeline_with_orgtasks.return_value = ([], None)

    payload = [PrefectDataFlowOrgTasks(uuid=str(edr_orgtask.uuid), seq=0)]
    PipelineService._build_transform_tasks(org, payload, [])

    all_orgtasks_passed = mock_pipeline_with_orgtasks.call_args[0][1]
    slugs = [ot.task.slug for ot in all_orgtasks_passed]
    assert TASK_GITPULL in slugs
    assert TASK_GITCLONE not in slugs
    assert TASK_GENERATE_EDR in slugs
    assert slugs.index(TASK_GITPULL) < slugs.index(TASK_GENERATE_EDR)


@patch("ddpui.core.orchestrate.pipeline_service.DbtProjectManager.gather_dbt_project_params")
@patch("ddpui.core.orchestrate.pipeline_service.pipeline_with_orgtasks")
def test_build_transform_tasks_edr_only_eks_adds_git_clone(
    mock_pipeline_with_orgtasks,
    mock_gather_dbt_project_params,
    seed_master_tasks,
):
    """EDR-only pipeline on EKS: git-clone is prepended automatically."""
    org, edr_orgtask = _make_edr_org(is_eks=True)
    mock_gather_dbt_project_params.return_value = Mock(
        project_dir="/mnt/clientdbts/edr-test-org/dbtrepo",
        clients_base_dir="/mnt/clientdbts",
        project_dir_relative="edr-test-org/dbtrepo",
    )
    mock_pipeline_with_orgtasks.return_value = ([], None)

    payload = [PrefectDataFlowOrgTasks(uuid=str(edr_orgtask.uuid), seq=0)]
    PipelineService._build_transform_tasks(org, payload, [])

    all_orgtasks_passed = mock_pipeline_with_orgtasks.call_args[0][1]
    slugs = [ot.task.slug for ot in all_orgtasks_passed]
    assert TASK_GITCLONE in slugs
    assert TASK_GITPULL not in slugs
    assert TASK_GENERATE_EDR in slugs
    assert slugs.index(TASK_GITCLONE) < slugs.index(TASK_GENERATE_EDR)
