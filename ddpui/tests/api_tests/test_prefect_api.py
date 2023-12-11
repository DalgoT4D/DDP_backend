import os
import django

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
@pytest.fixture
def org_with_dbt_workspace(tmpdir_factory):
    """a pytest fixture which creates an Org having an airbyte workspace"""
    print("creating org_with_dbt_workspace")
    dbt = OrgDbt.objects.create(
        gitrepo_url="dummy-git-url.github.com",
        project_dir="tmp/",
        dbt_venv=tmpdir_factory.mktemp("venv"),
        target_type="postgres",
        default_schema="prod",
    )
    org = Org.objects.create(
        airbyte_workspace_id="FAKE-WORKSPACE-ID", slug="test-org-slug", dbt=dbt
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
def test_post_prefect_transformation_tasks_success(org_with_dbt_workspace):
    """tests /tasks/transform/ success"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_dbt_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    OrgWarehouse.objects.create(org=org_with_dbt_workspace, wtype="postgres")

    post_prefect_transformation_tasks(mock_request)
