import os
import django
from pathlib import Path
import yaml

from unittest.mock import Mock, patch, MagicMock
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.api.client.prefect_api import (
    post_prefect_transformation_tasks,
    get_prefect_transformation_tasks,
)
from ddpui.models.org import OrgDbt, Org, OrgWarehouse
from ddpui.models.tasks import Task, OrgTask, OrgDataFlowv1, DataflowOrgTask
from ddpui.utils.constants import TASK_DBTRUN


pytestmark = pytest.mark.django_db


# ================================================================================
@pytest.fixture()
def org_with_dbt_workspace(tmpdir_factory):
    """a pytest fixture which creates an Org having an airbyte workspace"""
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
def org_with_transformation_tasks():
    print("creating org with tasks")

    org_slug = "test-org-slug"
    org = Org.objects.create(
        airbyte_workspace_id="FAKE-WORKSPACE-ID-1",
        slug=org_slug,
        name=org_slug,
    )

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

        print("fxiture", org_task)

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

    OrgWarehouse.objects.create(org=org_with_dbt_workspace, wtype="bigquery")

    post_prefect_transformation_tasks(mock_request)


def test_get_prefect_transformation_tasks_success(org_with_transformation_tasks):
    """tests GET /tasks/transform/ success with bigquery warehouse"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_transformation_tasks

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    resp = get_prefect_transformation_tasks(mock_request)

    print(resp)

    assert OrgTask.objects.filter(org=mock_orguser.org).count() == 5
