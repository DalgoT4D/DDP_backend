import os, glob
import subprocess
from pathlib import Path
from unittest.mock import patch
import django
import pytest

from dbt_automation import assets
from ddpui.models.org import Org, OrgDbt, OrgPrefectBlockv1, OrgWarehouse
from ddpui.ddpdbt.dbt_service import delete_dbt_workspace, setup_local_dbt_workspace
from ddpui.ddpprefect import DBTCLIPROFILE, SECRET

from django.contrib.auth.models import User
from ddpui.ddpdbt import dbt_service
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.models.org import Org, OrgDbt
from ddpui.models.org_user import OrgUser, OrgUserRole
from ddpui.models.role_based_access import Role


pytestmark = pytest.mark.django_db

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


@pytest.fixture
def org_with_workspace():
    """a pytest fixture which creates an Org having an airbyte workspace"""
    print("creating org_with_workspace")
    org = Org.objects.create(
        name="org-name", airbyte_workspace_id="FAKE-WORKSPACE-ID", slug="test-org-slug"
    )
    yield org
    print("deleting org_with_workspace")
    org.delete()


@pytest.fixture
def authuser():
    """a django User object"""
    user = User.objects.create(
        username="tempusername", email="tempuseremail", password="tempuserpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def orguser(authuser, org_with_workspace):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    org_user = OrgUser.objects.create(
        user=authuser,
        org=org_with_workspace,
        role=OrgUserRole.ACCOUNT_MANAGER,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield org_user
    org_user.delete()


def test_delete_dbt_workspace():
    """tests the delete_dbt_workspace function"""
    org = Org.objects.create(name="temp", slug="temp")

    org.dbt = OrgDbt.objects.create(
        gitrepo_url="gitrepo_url",
        project_dir="project-dir",
        target_type="tgt",
        default_schema="default_schema",
    )
    org.save()

    assert OrgDbt.objects.filter(gitrepo_url="gitrepo_url").count() == 1

    OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=DBTCLIPROFILE,
        block_id="dbtcli-block-id",
        block_name="dbtcli-block-id",
    )
    OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=SECRET,
        block_id="secret-block-id",
        block_name="secret-git-pull",
    )

    assert OrgPrefectBlockv1.objects.filter(block_id="dbtcli-block-id").count() == 1
    assert OrgPrefectBlockv1.objects.filter(block_id="secret-block-id").count() == 1

    with patch("ddpui.ddpdbt.dbt_service.os.path.exists") as mock_exists, patch(
        "ddpui.ddpdbt.dbt_service.shutil.rmtree"
    ) as mock_rmtree, patch(
        "ddpui.ddpdbt.dbt_service.prefect_service.delete_dbt_cli_profile_block"
    ) as mock_delete_dbt_cli_block, patch(
        "ddpui.ddpdbt.dbt_service.prefect_service.delete_secret_block"
    ) as mock_delete_secret_block:
        delete_dbt_workspace(org)
        mock_exists.return_value = True
        mock_rmtree.assert_called_once_with("project-dir")
        mock_delete_dbt_cli_block.assert_called_once_with("dbtcli-block-id")
        mock_delete_secret_block.assert_called_once_with("secret-block-id")

    assert org.dbt is None
    assert OrgDbt.objects.filter(gitrepo_url="gitrepo_url").count() == 0
    assert OrgPrefectBlockv1.objects.filter(block_id="block-id").count() == 0


def test_setup_local_dbt_workspace_warehouse_not_created():
    """a failure test; creating local dbt workspace without org warehouse"""
    org = Org.objects.create(name="temp", slug="temp")

    result, error = setup_local_dbt_workspace(
        org, project_name="dbtrepo", default_schema="default"
    )
    assert result is None
    assert error == "Please set up your warehouse first"


def test_setup_local_dbt_workspace_project_already_exists(tmp_path):
    """a failure test; creating local dbt workspace failed if project already exists"""
    project_name = "dbtrepo"
    default_schema = "default"

    org = Org.objects.create(name="temp", slug="temp")

    OrgWarehouse.objects.create(org=org, wtype="postgres")
    project_dir: Path = Path(tmp_path) / org.slug
    dbtrepo_dir: Path = project_dir / project_name
    os.makedirs(dbtrepo_dir)

    with patch("os.getenv", return_value=tmp_path):
        result, error = setup_local_dbt_workspace(
            org, project_name=project_name, default_schema=default_schema
        )
        assert result is None
        assert error == f"Project {project_name} already exists"


def test_setup_local_dbt_workspace_dbt_init_failed(tmp_path):
    """a failure test; setup fails because dbt init failed"""
    project_name = "dbtrepo"
    default_schema = "default"

    org = Org.objects.create(name="temp", slug="temp")

    OrgWarehouse.objects.create(org=org, wtype="postgres")

    with patch("os.getenv", return_value=tmp_path), patch(
        "subprocess.check_call",
        side_effect=subprocess.CalledProcessError(returncode=1, cmd="cmd"),
    ) as mock_subprocess_call:
        result, error = setup_local_dbt_workspace(
            org, project_name=project_name, default_schema=default_schema
        )
        assert result is None
        assert error == "Something went wrong while setting up workspace"
        mock_subprocess_call.assert_called_once()


def test_setup_local_dbt_workspace_success(tmp_path):
    """a success test for creating local dbt workspace"""
    project_name = "dbtrepo"
    default_schema = "default"

    org = Org.objects.create(name="temp", slug="temp")

    OrgWarehouse.objects.create(org=org, wtype="postgres")
    project_dir: Path = Path(tmp_path) / org.slug
    dbtrepo_dir: Path = project_dir / project_name

    def run_dbt_init(*args, **kwargs):
        os.makedirs(dbtrepo_dir)
        os.makedirs(dbtrepo_dir / "macros")

    with patch("os.getenv", return_value=tmp_path), patch(
        "subprocess.check_call", side_effect=run_dbt_init
    ) as mock_subprocess_call:
        result, error = setup_local_dbt_workspace(
            org, project_name=project_name, default_schema=default_schema
        )
        assert result is None
        assert error is None
        mock_subprocess_call.assert_called_once()

    assert (Path(dbtrepo_dir) / "packages.yml").exists()
    assert (Path(dbtrepo_dir) / "macros").exists()
    assets_dir = assets.__path__[0]

    for sql_file_path in glob.glob(os.path.join(assets_dir, "*.sql")):
        assert (Path(dbtrepo_dir) / "macros" / Path(sql_file_path).name).exists()

    orgdbt = OrgDbt.objects.filter(org=org).first()
    assert orgdbt is not None
    assert org.dbt == orgdbt


class TestElementaryReportRefresh:
    """Test elementary report generation"""
    def test_edr_pipeline_not_present(self, orguser):
        """Get an error if the pipeline is not present"""
        response = dbt_service.refresh_elementary_report_via_prefect(orguser)
        assert response == {"error": "pipeline not found"}
