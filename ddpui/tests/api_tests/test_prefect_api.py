import os
import django
from pathlib import Path
import yaml

from unittest.mock import Mock, patch, MagicMock
import pytest
from ninja.errors import HttpError

from ddpui.api.client.prefect_api import post_prefect_transformation_tasks
from ddpui.models.org import OrgDbt, Org, OrgWarehouse

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

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


# ================================================================================
def test_post_prefect_transformation_tasks_dbt_not_setup():
    """tests /tasks/transform/ without setting up dbt workspace"""
    mock_orguser = Mock()
    mock_orguser.org.dbt = None

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        post_prefect_transformation_tasks(mock_request)
    assert str(excinfo.value) == "create a dbt workspace first"


def test_post_prefect_transformation_tasks_warehouse_not_setup(org_with_dbt_workspace):
    """tests /tasks/transform/ with no warehouse"""
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
    """tests /tasks/transform/ success with postgres warehouse"""
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
    """tests /tasks/transform/ success with bigquery warehouse"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_dbt_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    OrgWarehouse.objects.create(org=org_with_dbt_workspace, wtype="bigquery")

    post_prefect_transformation_tasks(mock_request)
