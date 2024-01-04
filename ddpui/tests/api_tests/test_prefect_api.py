import os
import django
import yaml

from unittest.mock import Mock, patch
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.api.prefect_api import (
    post_prefect_transformation_tasks,
    get_prefect_transformation_tasks,
    delete_prefect_transformation_tasks,
    post_run_prefect_org_deployment_task,
    post_run_prefect_org_task,
)
from ddpui.models.org import OrgDbt, Org, OrgWarehouse, OrgPrefectBlockv1
from django.contrib.auth.models import User
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import Task, OrgTask, OrgDataFlowv1, DataflowOrgTask, TaskLock
from ddpui.ddpprefect import DBTCLIPROFILE, SECRET
from ddpui.utils.constants import TASK_DBTRUN, TASK_GITPULL, TASK_DBTDEPS


pytestmark = pytest.mark.django_db


def seed_tasks():
    # seed data
    for task in [
        {"type": "git", "slug": "git-pull", "label": "GIT pull", "command": "git pull"},
        {"type": "dbt", "slug": "dbt-clean", "label": "DBT clean", "command": "clean"},
        {"type": "dbt", "slug": "dbt-deps", "label": "DBT deps", "command": "deps"},
        {"type": "dbt", "slug": "dbt-run", "label": "DBT run", "command": "run"},
        {"type": "dbt", "slug": "dbt-test", "label": "DBT test", "command": "test"},
        {
            "type": "airbyte",
            "slug": "airbyte-sync",
            "label": "AIRBYTE sync",
            "command": None,
        },
    ]:
        Task.objects.create(**task)


# ================================================================================
@pytest.fixture()
def org_without_dbt_workspace():
    """org without dbt workspace"""
    print("creating org")
    org_slug = "test-org-slug"

    org = Org.objects.create(
        airbyte_workspace_id="FAKE-WORKSPACE-ID-1",
        slug=org_slug,
        name=org_slug,
    )
    yield org
    print("deleting org")
    org.delete()


@pytest.fixture()
def org_with_dbt_workspace(tmpdir_factory):
    """a pytest fixture which creates an Org having an dbt workspace"""
    print("creating org_with_dbt_workspace")
    org_slug = "test-org-slug"
    client_dir = tmpdir_factory.mktemp("clients")
    org_dir = client_dir.mkdir(org_slug)
    org_dir.mkdir("dbtrepo")

    os.environ["CLIENTDBT_ROOT"] = str(client_dir)

    # create dbt_project.yml file
    yml_obj = {"profile": "dummy"}
    with open(
        str(org_dir / "dbtrepo" / "dbt_project.yml"), "w", encoding="utf-8"
    ) as output:
        yaml.safe_dump(yml_obj, output)

    dbt = OrgDbt.objects.create(
        gitrepo_url="dummy-git-url.github.com",
        project_dir="tmp/",
        dbt_venv=tmpdir_factory.mktemp("venv"),
        target_type="postgres",
        default_schema="prod",
    )
    org = Org.objects.create(
        airbyte_workspace_id="FAKE-WORKSPACE-ID-1",
        slug=org_slug,
        dbt=dbt,
        name=org_slug,
    )
    yield org
    print("deleting org_with_dbt_workspace")
    org.delete()


@pytest.fixture()
def org_with_transformation_tasks(tmpdir_factory):
    """org having the transformation tasks and dbt workspace"""
    print("creating org with tasks")

    org_slug = "test-org-slug"
    client_dir = tmpdir_factory.mktemp("clients")
    org_dir = client_dir.mkdir(org_slug)
    org_dir.mkdir("dbtrepo")

    os.environ["CLIENTDBT_ROOT"] = str(client_dir)

    # create dbt_project.yml file
    yml_obj = {"profile": "dummy"}
    with open(
        str(org_dir / "dbtrepo" / "dbt_project.yml"), "w", encoding="utf-8"
    ) as output:
        yaml.safe_dump(yml_obj, output)

    dbt = OrgDbt.objects.create(
        gitrepo_url="dummy-git-url.github.com",
        project_dir="tmp/",
        dbt_venv=tmpdir_factory.mktemp("venv"),
        target_type="postgres",
        default_schema="prod",
    )
    org = Org.objects.create(
        airbyte_workspace_id="FAKE-WORKSPACE-ID-1",
        slug=org_slug,
        dbt=dbt,
        name=org_slug,
    )
    user = User.objects.create(
        is_superuser=True,
        username="test@gmail.com",
        password="password",
        is_active=True,
        is_staff=False,
    )
    OrgUser.objects.create(org=org, user=user, role=3, email_verified=True)

    # create secret block
    OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=SECRET,
        block_id="secret-blk-id",
        block_name="secret-blk-name",
    )

    # create cli block
    OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=DBTCLIPROFILE,
        block_id="cliprofile-blk-id",
        block_name="cliprofile-blk-name",
    )

    # seed data
    seed_tasks()

    for task in Task.objects.filter(type__in=["dbt", "git"]).all():
        org_task = OrgTask.objects.create(org=org, task=task)

        if task.slug == "dbt-run":
            new_dataflow = OrgDataFlowv1.objects.create(
                org=org,
                name="test-dbtrun-deployment",
                deployment_name="test-dbtrun-deployment",
                deployment_id="test-dbtrun-deployment-id",
                dataflow_type="manual",
            )

            DataflowOrgTask.objects.create(
                dataflow=new_dataflow,
                orgtask=org_task,
            )

    yield org

    org.delete()


# ================================================================================
def test_post_prefect_transformation_tasks_dbt_not_setup():
    """tests POST /tasks/transform/ without setting up dbt workspace"""
    mock_orguser = Mock()
    mock_orguser.org.dbt = None

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        post_prefect_transformation_tasks(mock_request)
    assert str(excinfo.value) == "create a dbt workspace first"


def test_post_prefect_transformation_tasks_warehouse_not_setup(org_with_dbt_workspace):
    """tests POST /tasks/transform/ with no warehouse"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_dbt_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        post_prefect_transformation_tasks(mock_request)
    assert str(excinfo.value) == "need to set up a warehouse first"


@patch.multiple(
    "ddpui.utils.secretsmanager",
    retrieve_warehouse_credentials=Mock(
        return_value={
            "host": "the-host",
            "port": 0,
            "username": "the-user",
            "password": "the-password",
            "database": "the-database",
        }
    ),
    retrieve_github_token=Mock(return_value="test-git-acccess-token"),
)
@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    create_secret_block=Mock(
        return_value={"block_id": "git-secret-blk", "block_name": "git-secret-blk"}
    ),
    create_dbt_cli_profile_block=Mock(
        return_value={"block_id": "cli-blk-id", "block_name": "cli-blk-name"}
    ),
    create_dataflow_v1=Mock(
        return_value={
            "deployment": {"id": "test-deploy-id", "name": "test-deploy-name"}
        }
    ),
)
def test_post_prefect_transformation_tasks_success_postgres_warehouse(
    org_with_dbt_workspace,
):
    """tests POST /tasks/transform/ success with postgres warehouse"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_dbt_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    seed_tasks()

    OrgWarehouse.objects.create(org=org_with_dbt_workspace, wtype="postgres")

    post_prefect_transformation_tasks(mock_request)


@patch.multiple(
    "ddpui.utils.secretsmanager",
    retrieve_warehouse_credentials=Mock(
        return_value={
            "host": "the-host",
            "port": 0,
            "username": "the-user",
            "password": "the-password",
            "database": "the-database",
        }
    ),
    retrieve_github_token=Mock(return_value="test-git-acccess-token"),
)
@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    create_secret_block=Mock(
        return_value={"block_id": "git-secret-blk", "block_name": "git-secret-blk"}
    ),
    create_dbt_cli_profile_block=Mock(
        return_value={"block_id": "cli-blk-id", "block_name": "cli-blk-name"}
    ),
    create_dataflow_v1=Mock(
        return_value={
            "deployment": {"id": "test-deploy-id", "name": "test-deploy-name"}
        }
    ),
)
@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_destination=Mock(
        return_value={"connectionConfiguration": {"dataset_location": "US"}}
    ),
)
def test_post_prefect_transformation_tasks_success_bigquery_warehouse(
    org_with_dbt_workspace,
):
    """tests POST /tasks/transform/ success with bigquery warehouse"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_dbt_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    seed_tasks()

    OrgWarehouse.objects.create(org=org_with_dbt_workspace, wtype="bigquery")

    post_prefect_transformation_tasks(mock_request)


def test_get_prefect_transformation_tasks_success(org_with_transformation_tasks):
    """tests GET /tasks/transform/ success"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_transformation_tasks

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    get_prefect_transformation_tasks(mock_request)

    assert OrgTask.objects.filter(org=mock_orguser.org).count() == 5


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    delete_secret_block=Mock(return_value=True),
    delete_dbt_cli_profile_block=Mock(return_value=True),
    delete_deployment_by_id=Mock(return_value=True),
)
def test_delete_prefect_transformation_tasks_success(org_with_transformation_tasks):
    """tests DELETE /tasks/transform/ success"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_transformation_tasks

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    delete_prefect_transformation_tasks(mock_request)

    assert OrgTask.objects.filter(org=mock_orguser.org).count() == 0
    assert (
        OrgPrefectBlockv1.objects.filter(
            org=mock_orguser.org, block_type=SECRET
        ).count()
        == 0
    )
    assert (
        OrgPrefectBlockv1.objects.filter(
            org=mock_orguser.org, block_type=DBTCLIPROFILE
        ).count()
        == 0
    )


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    create_deployment_flow_run=Mock(return_value=True),
    lock_tasks_for_deployment=Mock(return_value=[]),
)
def test_post_run_prefect_org_deployment_task_success(org_with_transformation_tasks):
    """tests POST /v1/flows/{deployment_id}/flow_run/ success"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_transformation_tasks

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    dataflow_orgtask = None
    org_task = OrgTask.objects.filter(
        org=mock_orguser.org, task__slug=TASK_DBTRUN
    ).first()
    if org_task:
        dataflow_orgtask = DataflowOrgTask.objects.filter(orgtask=org_task).first()

    if dataflow_orgtask is None:
        raise Exception("Deployment not found")

    post_run_prefect_org_deployment_task(
        mock_orguser, dataflow_orgtask.dataflow.deployment_id
    )

    assert TaskLock.objects.filter(orgtask=org_task).count() == 0


def test_post_run_prefect_org_task_invalid_task_id(org_with_transformation_tasks):
    """tests POST /tasks/{orgtask_id}/run/ failure by invalid task id"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_transformation_tasks

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        post_run_prefect_org_task(mock_request, 0)
    assert str(excinfo.value) == "task not found"


def test_post_run_prefect_org_task_invalid_task_type(org_with_transformation_tasks):
    """tests POST /tasks/{orgtask_id}/run/ failure by invalid task type"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_transformation_tasks

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    org_task = OrgTask.objects.create(task=task, org=mock_orguser.org)

    if org_task is None:
        raise Exception("Task not found")

    with pytest.raises(HttpError) as excinfo:
        post_run_prefect_org_task(mock_request, org_task.id)
    assert str(excinfo.value) == "task not supported"


def test_post_run_prefect_org_task_no_dbt_workspace(org_with_transformation_tasks):
    """tests POST /tasks/{orgtask_id}/run/ failure by not setting up dbt workspace"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_transformation_tasks
    mock_orguser.org.dbt = None

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    org_task = OrgTask.objects.filter(
        org=mock_orguser.org, task__slug=TASK_DBTDEPS
    ).first()

    if org_task is None:
        raise Exception("Task not found")

    with pytest.raises(HttpError) as excinfo:
        post_run_prefect_org_task(mock_request, org_task.id)
    assert str(excinfo.value) == "dbt is not configured for this client"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    run_shell_task_sync=Mock(return_value=True),
)
def test_post_run_prefect_org_task_git_pull_success(org_with_transformation_tasks):
    """tests POST /tasks/{orgtask_id}/run/ success"""

    mock_request = Mock()
    mock_request.org = org_with_transformation_tasks
    mock_request.orguser = OrgUser.objects.filter(
        org=org_with_transformation_tasks
    ).first()

    org_task = OrgTask.objects.filter(
        org=mock_request.org, task__slug=TASK_GITPULL
    ).first()

    if org_task is None:
        raise Exception("Task not found")

    post_run_prefect_org_task(mock_request, org_task.id)


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    run_dbt_task_sync=Mock(return_value=True),
)
def test_post_run_prefect_org_task_dbt_deps_success(org_with_transformation_tasks):
    """tests POST /tasks/{orgtask_id}/run/ success"""

    mock_request = Mock()
    mock_request.org = org_with_transformation_tasks
    mock_request.orguser = OrgUser.objects.filter(
        org=org_with_transformation_tasks
    ).first()

    org_task = OrgTask.objects.filter(
        org=org_with_transformation_tasks, task__slug=TASK_DBTDEPS
    ).first()

    if org_task is None:
        raise Exception("Task not found")

    post_run_prefect_org_task(mock_request, org_task.id)
