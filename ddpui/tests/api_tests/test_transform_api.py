import os, glob, uuid
from pathlib import Path
import django.core
import django.core.exceptions
from pydantic.error_wrappers import ValidationError as PydanticValidationError
import django
import pydantic
import pytest
from unittest.mock import Mock, patch, call
from datetime import datetime, timedelta
from django.utils import timezone
from ninja.errors import HttpError, ValidationError
from ddpui.dbt_automation import assets
from ddpui.models.role_based_access import Role, RolePermission, Permission
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgWarehouse, OrgDbt, TransformType
from ddpui.models.dbt_workflow import OrgDbtModel, OrgDbtOperation, DbtEdge, OrgDbtModelType
from ddpui.models.canvaslock import CanvasLock
from ddpui.auth import (
    GUEST_ROLE,
    SUPER_ADMIN_ROLE,
    ACCOUNT_MANAGER_ROLE,
    PIPELINE_MANAGER_ROLE,
    ANALYST_ROLE,
)
from ddpui.schemas.org_task_schema import DbtProjectSchema
from ddpui.ddpprefect.schema import DbtProfile, OrgDbtSchema
from ddpui.models.tasks import TaskProgressHashPrefix
from ddpui.tests.api_tests.test_user_org_api import (
    seed_db,
    orguser,
    nonadminorguser,
    mock_request,
    authuser,
    org_without_workspace,
)
from ddpui.api.transform_api import (
    create_dbt_project,
    delete_dbt_project,
    sync_sources,
    get_input_sources_and_models,
    get_warehouse_datatypes,
    lock_canvas,
    unlock_canvas,
)
from ddpui.schemas.dbt_workflow_schema import (
    LockCanvasResponseSchema,
)
from ddpui.utils.taskprogress import TaskProgress
from ddpui.ddpdbt.dbt_service import setup_local_dbt_workspace
from ddpui.core.dbtautomation_service import sync_sources_for_warehouse_v2
from ddpui.dbt_automation.utils.warehouseclient import PostgresClient

pytestmark = pytest.mark.django_db


WAREHOUSE_DATA = {
    "schema1": ["table1", "table2"],
    "schema2": ["table3", "table4"],
}


def mock_setup_dbt_workspace_ui_transform(orguser: OrgUser, tmp_path):
    project_name = "dbtrepo"
    default_schema = "default"

    org = orguser.org

    OrgWarehouse.objects.create(org=org, wtype="postgres")
    project_dir: Path = Path(tmp_path) / org.slug
    dbtrepo_dir: Path = project_dir / project_name

    def run_dbt_init(*args, **kwargs):
        os.makedirs(dbtrepo_dir)
        os.makedirs(dbtrepo_dir / "macros")
        os.makedirs(dbtrepo_dir / "models")

    with patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_org_dir", return_value=project_dir
    ), patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.run_dbt_command", side_effect=run_dbt_init
    ) as mock_dbt_command, patch(
        "ddpui.ddpdbt.dbt_service.create_or_update_org_cli_block", return_value=((None, None), None)
    ) as mock_create_cli_block, patch(
        "ddpui.ddpdbt.dbt_service.secretsmanager.retrieve_warehouse_credentials", return_value={}
    ) as mock_retrieve_creds:
        setup_local_dbt_workspace(org, project_name=project_name, default_schema=default_schema)
        mock_dbt_command.assert_called_once()
        mock_retrieve_creds.assert_called_once()
        mock_create_cli_block.assert_called_once()

    assert (Path(dbtrepo_dir) / "packages.yml").exists()
    assert (Path(dbtrepo_dir) / "macros").exists()
    assets_dir = assets.__path__[0]

    for sql_file_path in glob.glob(os.path.join(assets_dir, "*.sql")):
        assert (Path(dbtrepo_dir) / "macros" / Path(sql_file_path).name).exists()

    # Verify .gitignore was created with expected content
    gitignore_path = Path(dbtrepo_dir) / ".gitignore"
    assert gitignore_path.exists()
    gitignore_content = gitignore_path.read_text()
    assert "target/" in gitignore_content
    assert "dbt_packages/" in gitignore_content
    assert "profiles.yml" in gitignore_content
    assert ".env*" in gitignore_content

    orgdbt = OrgDbt.objects.filter(org=org).first()
    assert orgdbt is not None
    assert org.dbt == orgdbt


def mock_setup_sync_sources(orgdbt: OrgDbt, warehouse: OrgWarehouse):
    # warehouse schemas and tables
    SCHEMAS_TABLES = WAREHOUSE_DATA

    with patch.object(TaskProgress, "__init__", return_value=None), patch.object(
        TaskProgress, "add", return_value=Mock()
    ) as add_progress_mock, patch(
        "ddpui.core.dbtautomation_service._get_wclient",
    ) as get_wclient_mock:
        mock_instance = Mock()
        mock_instance.get_schemas.return_value = SCHEMAS_TABLES.keys()
        mock_instance.get_tables.side_effect = lambda schema: SCHEMAS_TABLES[schema]

        # Make _get_wclient return the mock instance
        get_wclient_mock.return_value = mock_instance

        assert OrgDbtModel.objects.filter(type="source", orgdbt=orgdbt).count() == 0
        sync_sources_for_warehouse_v2(orgdbt.id, warehouse.id, "task-id", "hashkey")
        for schema in SCHEMAS_TABLES:
            assert (
                list(
                    OrgDbtModel.objects.filter(
                        type="source", orgdbt=orgdbt, schema=schema
                    ).values_list("name", flat=True)
                )
                == SCHEMAS_TABLES[schema]
            )


# ================================================================================


def test_seed_data(seed_db):
    """a test to seed the database"""
    assert Role.objects.count() == 5
    assert RolePermission.objects.count() > 5
    assert Permission.objects.count() > 5


def test_lock_canvas_acquire(seed_db, orguser):
    """a test to lock the canvas"""
    # First create a dbt workspace for the org
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir="test_project_dir",
        dbt_venv="test_venv",
        target_type="postgres",
        default_schema="default_schema",
        transform_type=TransformType.UI,
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    request = mock_request(orguser)
    response = lock_canvas(request)
    assert response is not None
    assert response.locked_by == orguser.user.email
    assert response.lock_token is not None


def test_lock_canvas_locked_by_another(seed_db, orguser, nonadminorguser):
    """a test to lock the canvas when it's already locked by another user"""
    # Create dbt workspace for both users
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir="test_project_dir",
        dbt_venv="test_venv",
        target_type="postgres",
        default_schema="default_schema",
        transform_type=TransformType.UI,
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    # Create a lock for another user
    lock = CanvasLock.objects.create(
        dbt=orgdbt,
        locked_by=nonadminorguser,
        lock_token="existing-lock-token",
        expires_at=timezone.now() + timedelta(minutes=5),
    )

    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        lock_canvas(request)
    assert "already locked by" in str(excinfo.value)


def test_lock_canvas_locked_by_self(seed_db, orguser):
    """a test to lock the canvas when it's already locked by the same user"""
    # Create dbt workspace
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir="test_project_dir",
        dbt_venv="test_venv",
        target_type="postgres",
        default_schema="default_schema",
        transform_type=TransformType.UI,
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    request = mock_request(orguser)
    # First lock should succeed
    first_response = lock_canvas(request)
    assert first_response is not None
    assert first_response.locked_by == orguser.user.email
    assert first_response.lock_token is not None

    # Second call should refresh the lock
    second_response = lock_canvas(request)
    assert second_response is not None
    assert second_response.locked_by == orguser.user.email
    assert second_response.lock_token == first_response.lock_token


def test_unlock_canvas_unlocked(seed_db, orguser):
    """a test to unlock the canvas when no lock exists"""
    # Create dbt workspace
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir="test_project_dir",
        dbt_venv="test_venv",
        target_type="postgres",
        default_schema="default_schema",
        transform_type=TransformType.UI,
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    request = mock_request(orguser)
    # Should succeed even if no lock exists (already unlocked)
    response = unlock_canvas(request)
    assert response == {"success": True}


def test_unlock_canvas_locked_by_different_user(seed_db, orguser, nonadminorguser):
    """a test to unlock the canvas when locked by different user"""
    # Create dbt workspace
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir="test_project_dir",
        dbt_venv="test_venv",
        target_type="postgres",
        default_schema="default_schema",
        transform_type=TransformType.UI,
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    # Create a lock for another user
    CanvasLock.objects.create(
        dbt=orgdbt,
        locked_by=nonadminorguser,
        lock_token="existing-lock-token",
        expires_at=timezone.now() + timedelta(minutes=5),
    )

    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        unlock_canvas(request)
    assert "You can only unlock your own locks" in str(excinfo.value)


def test_unlock_canvas_success(seed_db, orguser):
    """a test to unlock the canvas successfully"""
    # Create dbt workspace
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir="test_project_dir",
        dbt_venv="test_venv",
        target_type="postgres",
        default_schema="default_schema",
        transform_type=TransformType.UI,
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    request = mock_request(orguser)
    # First lock the canvas
    lock_response = lock_canvas(request)
    assert lock_response.locked_by == orguser.user.email

    # Then unlock it
    unlock_response = unlock_canvas(request)
    assert unlock_response == {"success": True}


def test_unlock_canvas_dbt_workspace_not_setup(seed_db, orguser):
    """a test to unlock the canvas when dbt workspace is not setup"""
    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        unlock_canvas(request)
    assert "dbt workspace not setup" in str(excinfo.value)


# =================================== ui4t =======================================


def test_create_dbt_project_check_permission(orguser: OrgUser):
    """
    a failure test to check if the orguser has the correct permission
    """
    slugs_have_permission = [
        SUPER_ADMIN_ROLE,
        ACCOUNT_MANAGER_ROLE,
        ANALYST_ROLE,
        PIPELINE_MANAGER_ROLE,
    ]
    slugs_dont_have_permission = [GUEST_ROLE]

    payload = DbtProjectSchema(default_schema="default")
    for slug in slugs_dont_have_permission:
        orguser.new_role = Role.objects.filter(slug=slug).first()
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            create_dbt_project(request, payload)
        assert str(exc.value) == "unauthorized"

    for slug in slugs_have_permission:
        orguser.new_role = Role.objects.filter(slug=slug).first()
        request = mock_request(orguser)
        with pytest.raises(Exception) as exc:
            create_dbt_project(request, payload)
        assert str(exc.value) == "Please set up your warehouse first"


def test_create_dbt_project_mocked_helper(orguser: OrgUser):
    """a success test to create a dbt project"""
    request = mock_request(orguser)
    payload = DbtProjectSchema(default_schema="default")

    with patch(
        "ddpui.api.transform_api.setup_local_dbt_workspace",
        return_value=(None, None),
    ) as setup_local_dbt_workspace_mock:
        result = create_dbt_project(request, payload)
        assert isinstance(result, dict)
        assert "message" in result
        assert result["message"] == f"Project {orguser.org.slug} created successfully"
        setup_local_dbt_workspace_mock.assert_called_once_with(
            orguser.org, project_name="dbtrepo", default_schema=payload.default_schema
        )


def test_delete_dbt_project_failure_projectdir_does_not_exist(orguser: OrgUser, tmp_path):
    """a failure test for delete a dbt project api when project dir does not exist"""
    request = mock_request(orguser)
    project_name = "dummy-project"
    with patch("ddpui.api.transform_api.DbtProjectManager.get_org_dir", return_value=tmp_path):
        with pytest.raises(HttpError) as excinfo:
            delete_dbt_project(request, project_name)
    assert (
        str(excinfo.value)
        == f"Project {project_name} does not exist in organization {orguser.org.slug}"
    )


def test_delete_dbt_project_failure_dbtrepodir_does_not_exist(orguser: OrgUser, tmp_path):
    """a failure test for delete a dbt project api when dbt repo dir does not exist"""
    request = mock_request(orguser)
    project_name = "dummy-project"
    with patch("os.getenv", return_value=tmp_path):
        project_dir = Path(tmp_path) / orguser.org.slug
        project_dir.mkdir(parents=True, exist_ok=True)
        with pytest.raises(HttpError) as excinfo:
            delete_dbt_project(request, project_name)
    assert (
        str(excinfo.value)
        == f"Project {project_name} does not exist in organization {orguser.org.slug}"
    )


def test_delete_dbt_project_success(orguser: OrgUser, tmp_path):
    """a failure test for delete a dbt project api when dbt repo dir does not exist"""
    request = mock_request(orguser)
    project_name = "dummy-project"

    project_dir = Path(tmp_path) / orguser.org.slug
    project_dir.mkdir(parents=True, exist_ok=True)
    dbtrepo_dir = project_dir / project_name
    dbtrepo_dir.mkdir(parents=True, exist_ok=True)
    dbt = OrgDbt.objects.create(
        project_dir=str(project_dir),
        dbt_venv="some/venv/bin/dbt",
        target_type="postgres",
        default_schema="default",
        transform_type=TransformType.GIT,
    )
    orguser.org.dbt = dbt
    orguser.org.save()

    assert OrgDbt.objects.filter(org=orguser.org).count() == 1

    with patch("ddpui.api.transform_api.DbtProjectManager.get_org_dir", return_value=project_dir):
        delete_dbt_project(request, project_name)

    assert not dbtrepo_dir.exists()
    assert project_dir.exists()
    assert OrgDbt.objects.filter(org=orguser.org).count() == 0


def test_sync_sources_failed_warehouse_not_present(orguser: OrgUser):
    """a failure test for sync sources when warehouse is not present"""
    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        sync_sources(request)
    assert str(excinfo.value) == "Please set up your warehouse first"


def test_sync_sources_failed_dbt_workspace_not_setup(orguser: OrgUser):
    """a failure test for sync sources when dbt workspace is not setup"""
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )
    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        sync_sources(request)
    assert str(excinfo.value) == "DBT workspace not set up"


def test_sync_sources_success(orguser: OrgUser, tmp_path):
    """a success test for sync sources"""
    org_warehouse = OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir=str(Path(tmp_path) / orguser.org.slug),
        dbt_venv=tmp_path,
        target_type="postgres",
        default_schema="default_schema",
        transform_type=TransformType.UI,
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    request = mock_request(orguser)
    mocked_task = Mock()
    mocked_task.id = "task-id"
    with patch(
        "ddpui.core.dbtautomation_service.sync_sources_for_warehouse_v2.delay",
        return_value=mocked_task,
    ) as delay:
        result = sync_sources(request)

        args, kwargs = delay.call_args
        assert len(args) == 4
        assert args[0] == orgdbt.id
        assert args[1] == org_warehouse.id
        # args[2] is a random uuid
        assert args[3] == f"{TaskProgressHashPrefix.SYNCSOURCES.value}-{orguser.org.slug}"
        assert "task_id" in result
        assert "hashkey" in result
