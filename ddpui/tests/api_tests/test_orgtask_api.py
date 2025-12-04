import os, uuid
from unittest.mock import Mock, patch

import django
from django.core.management import call_command
import pytest
import yaml
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User

from ddpui.api.orgtask_api import (
    delete_system_transformation_tasks,
    get_prefect_transformation_tasks,
    post_system_transformation_tasks,
    post_run_prefect_org_task,
)
from ddpui.ddpprefect import DBTCLIPROFILE, SECRET
from ddpui.ddpprefect.schema import PrefectShellTaskSetup
from ddpui.models.org import Org, OrgDbt, OrgPrefectBlockv1, OrgWarehouse
from ddpui.models.role_based_access import Role, RolePermission, Permission
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import DataflowOrgTask, OrgDataFlowv1, OrgTask, Task, TaskType
from ddpui.utils.constants import TASK_DBTDEPS, TASK_GITPULL, TASK_GENERATE_EDR
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


@pytest.fixture(scope="session")
def seed_master_tasks_db(django_db_setup, django_db_blocker):
    with django_db_blocker.unblock():
        # Run the loaddata command to load the fixture
        call_command("loaddata", "tasks.json")


# ================================================================================


@pytest.fixture()
def org_without_dbt_workspace():
    """org without dbt workspace"""
    org_slug = "test-org-slug"

    org = Org.objects.create(
        airbyte_workspace_id="FAKE-WORKSPACE-ID-1",
        slug=org_slug,
        name=org_slug,
    )
    yield org
    org.delete()


@pytest.fixture
def orguser(org_without_dbt_workspace):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser = OrgUser.objects.create(
        user=User.objects.create(
            username="tempusername", email="tempuseremail", password="tempuserpassword"
        ),
        org=org_without_dbt_workspace,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


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
    with open(str(org_dir / "dbtrepo" / "dbt_project.yml"), "w", encoding="utf-8") as output:
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


@pytest.fixture
def orguser_dbt_workspace(org_with_dbt_workspace):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser = OrgUser.objects.create(
        user=User.objects.create(
            username="tempusername", email="tempuseremail", password="tempuserpassword"
        ),
        org=org_with_dbt_workspace,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture()
def org_with_transformation_tasks(tmpdir_factory, seed_master_tasks_db):
    """org having the transformation tasks and dbt workspace"""
    org_slug = "test-org-slug"
    client_dir = tmpdir_factory.mktemp("clients")
    org_dir = client_dir.mkdir(org_slug)
    org_dir.mkdir("dbtrepo")

    os.environ["CLIENTDBT_ROOT"] = str(client_dir)

    # create dbt_project.yml file
    yml_obj = {"profile": "dummy"}
    with open(str(org_dir / "dbtrepo" / "dbt_project.yml"), "w", encoding="utf-8") as output:
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

    # create secret block
    OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=SECRET,
        block_id="secret-blk-id",
        block_name="secret-blk-name",
    )

    # create cli block
    cli_block = OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=DBTCLIPROFILE,
        block_id="cliprofile-blk-id",
        block_name="cliprofile-blk-name",
    )
    # Set the relationship on dbt
    dbt.cli_profile_block = cli_block
    dbt.save()

    for task in Task.objects.filter(type__in=[TaskType.DBT, TaskType.GIT]).all():
        org_task = OrgTask.objects.create(org=org, task=task, uuid=uuid.uuid4(), dbt=org.dbt)

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


@pytest.fixture
def orguser_transform_tasks(org_with_transformation_tasks):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser = OrgUser.objects.create(
        user=User.objects.create(
            username="tempusername", email="tempuseremail", password="tempuserpassword"
        ),
        org=org_with_transformation_tasks,
        email_verified=True,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


# ================================================================================


def test_seed_data(seed_db):
    """a test to seed the database"""
    assert Role.objects.count() == 5
    assert RolePermission.objects.count() > 5
    assert Permission.objects.count() > 5


def test_seed_master_tasks(seed_master_tasks_db):
    """a test to seed the database"""
    assert Task.objects.count() == 12


# ================================================================================
def test_post_system_transformation_tasks_dbt_not_setup(orguser):
    """tests POST /tasks/transform/ without setting up dbt workspace"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        post_system_transformation_tasks(request)
    assert str(excinfo.value) == "create a dbt workspace first"


def test_post_system_transformation_tasks_warehouse_not_setup(orguser_dbt_workspace):
    """tests POST /tasks/transform/ with no warehouse"""
    request = mock_request(orguser_dbt_workspace)

    with pytest.raises(HttpError) as excinfo:
        post_system_transformation_tasks(request)
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
        return_value={"deployment": {"id": "test-deploy-id", "name": "test-deploy-name"}}
    ),
)
def test_post_system_transformation_tasks_success_postgres_warehouse(
    orguser_dbt_workspace,
):
    """tests POST /tasks/transform/ success with postgres warehouse"""
    request = mock_request(orguser_dbt_workspace)

    OrgWarehouse.objects.create(org=request.orguser.org, wtype="postgres")

    post_system_transformation_tasks(request)


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
        return_value={"deployment": {"id": "test-deploy-id", "name": "test-deploy-name"}}
    ),
)
@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_destination=Mock(return_value={"connectionConfiguration": {"dataset_location": "US"}}),
)
def test_post_system_transformation_tasks_success_bigquery_warehouse(
    orguser_dbt_workspace,
):
    """tests POST /tasks/transform/ success with bigquery warehouse"""
    request = mock_request(orguser_dbt_workspace)

    OrgWarehouse.objects.create(org=request.orguser.org, wtype="bigquery")

    post_system_transformation_tasks(request)


def test_get_prefect_transformation_tasks_success(orguser_transform_tasks):
    """tests GET /tasks/transform/ success"""
    request = mock_request(orguser_transform_tasks)

    get_prefect_transformation_tasks(request)

    assert OrgTask.objects.filter(org=request.orguser.org).count() == 7  # including git, dbt


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    delete_secret_block=Mock(return_value=True),
    delete_dbt_cli_profile_block=Mock(return_value=True),
    delete_deployment_by_id=Mock(return_value=True),
)
def test_delete_system_transformation_tasks_success(orguser_transform_tasks):
    """tests DELETE /tasks/transform/ success"""
    request = mock_request(orguser_transform_tasks)

    delete_system_transformation_tasks(request)

    assert OrgTask.objects.filter(org=request.orguser.org, task__is_system=True).count() == 0
    assert OrgPrefectBlockv1.objects.filter(org=request.orguser.org, block_type=SECRET).count() == 0
    assert (
        OrgPrefectBlockv1.objects.filter(org=request.orguser.org, block_type=DBTCLIPROFILE).count()
        == 0
    )


def test_post_run_prefect_org_task_invalid_task_id(orguser_transform_tasks):
    """tests POST /tasks/{orgtask_uuid}/run/ failure by invalid task id"""
    request = mock_request(orguser_transform_tasks)

    with pytest.raises(HttpError) as excinfo:
        post_run_prefect_org_task(request, 0)
    assert str(excinfo.value) == "invalid input type"

    with pytest.raises(HttpError) as excinfo:
        post_run_prefect_org_task(request, uuid.uuid4())
    assert str(excinfo.value) == "task not found"


def test_post_run_prefect_org_task_invalid_task_type(orguser_transform_tasks):
    """tests POST /tasks/{orgtask_uuid}/run/ failure by invalid task type"""
    request = mock_request(orguser_transform_tasks)

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    org_task = OrgTask.objects.create(task=task, org=request.orguser.org, uuid=uuid.uuid4())

    if org_task is None:
        raise Exception("Task not found")

    with pytest.raises(HttpError) as excinfo:
        post_run_prefect_org_task(request, org_task.uuid)
    assert str(excinfo.value) == "task not supported"


def test_post_run_prefect_org_task_no_dbt_workspace(orguser_transform_tasks):
    """tests POST /tasks/{orgtask_uuid}/run/ failure by not setting up dbt workspace"""
    orguser_transform_tasks.org.dbt = None
    request = mock_request(orguser_transform_tasks)

    org_task = OrgTask.objects.filter(org=request.orguser.org, task__slug=TASK_DBTDEPS).first()

    if org_task is None:
        raise Exception("Task not found")

    with pytest.raises(HttpError) as excinfo:
        post_run_prefect_org_task(request, org_task.uuid)
    assert str(excinfo.value) == "dbt is not configured for this client"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    run_shell_task_sync=Mock(return_value=True),
)
def test_post_run_prefect_org_task_git_pull_success(orguser_transform_tasks):
    """tests POST /tasks/{orgtask_id}/run/ success"""

    request = mock_request(orguser_transform_tasks)

    org_task = OrgTask.objects.filter(org=request.orguser.org, task__slug=TASK_GITPULL).first()

    if org_task is None:
        raise Exception("Task not found")

    post_run_prefect_org_task(request, org_task.uuid)


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    run_dbt_task_sync=Mock(return_value=True),
)
def test_post_run_prefect_org_task_dbt_deps_success(orguser_transform_tasks):
    """tests POST /tasks/{orgtask_uuid}/run/ success"""

    request = mock_request(orguser_transform_tasks)

    org_task = OrgTask.objects.filter(org=request.orguser.org, task__slug=TASK_DBTDEPS).first()

    if org_task is None:
        raise Exception("Task not found")

    post_run_prefect_org_task(request, org_task.uuid)


def test_post_run_prefect_org_task_generate_edr(
    orguser_dbt_workspace, seed_master_tasks_db, seed_db
):
    """tests POST /tasks/{orgtask_uuid}/run/ success"""

    request = mock_request(orguser_dbt_workspace)
    task = Task.objects.filter(slug=TASK_GENERATE_EDR).first()

    org_task = OrgTask.objects.create(org=request.orguser.org, task=task, uuid=uuid.uuid4())

    with patch(
        "ddpui.api.orgtask_api.setup_edr_send_report_task_config"
    ) as setup_edr_send_report_task_config_mock:
        with patch(
            "ddpui.ddpprefect.prefect_service.run_shell_task_sync"
        ) as run_shell_task_sync_mock:
            task_config = PrefectShellTaskSetup(
                type="",
                slug="",
                commands=[],
                working_dir="",
                env={},
                orgtask_uuid=str(org_task.uuid),
            )
            setup_edr_send_report_task_config_mock.return_value = task_config
            run_shell_task_sync_mock.return_value = "retval"
            retval = post_run_prefect_org_task(request, org_task.uuid)
            assert retval == "retval"
            run_shell_task_sync_mock.assert_called_once_with(task_config)
