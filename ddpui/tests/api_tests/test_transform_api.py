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
from ddpui.models.canvas_models import CanvasNode, CanvasEdge, CanvasNodeType
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
    get_input_sources_and_models_v2,
    get_warehouse_datatypes,
    lock_canvas,
    unlock_canvas,
    delete_orgdbtmodel,
    get_dbt_project_DAG_v2,
    post_create_src_model_node,
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


def create_canvas_graph(orgdbt):
    """Helper function to create a sample canvas graph with nodes and edges"""
    # Create canvas nodes
    source_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.SOURCE,
        name="source_table",
        uuid=uuid.uuid4(),
        output_cols=["id", "name"],
    )

    operation_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.OPERATION,
        name="filter",
        uuid=uuid.uuid4(),
        operation_config={"type": "where", "config": {"clauses": []}},
        output_cols=["id", "name"],
    )

    model_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.MODEL,
        name="target_model",
        uuid=uuid.uuid4(),
        output_cols=["id", "name"],
    )

    # Create edges: source -> operation -> model
    edge1 = CanvasEdge.objects.create(from_node=source_node, to_node=operation_node, seq=1)

    edge2 = CanvasEdge.objects.create(from_node=operation_node, to_node=model_node, seq=1)

    return {
        "nodes": [source_node, operation_node, model_node],
        "edges": [edge1, edge2],
        "source_node": source_node,
        "operation_node": operation_node,
        "model_node": model_node,
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


def test_get_input_sources_and_models_v2_warehouse_not_setup(seed_db, orguser: OrgUser):
    """Test failure when warehouse is not setup"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_input_sources_and_models_v2(request)

    assert str(excinfo.value) == "please setup your warehouse first"


def test_get_input_sources_and_models_v2_dbt_workspace_not_setup(seed_db, orguser: OrgUser):
    """Test failure when dbt workspace is not setup"""
    # Create warehouse but no dbt workspace
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )

    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_input_sources_and_models_v2(request)

    assert str(excinfo.value) == "dbt workspace not setup"


def test_get_input_sources_and_models_v2_empty_response(seed_db, orguser: OrgUser):
    """Test success with no models/sources"""
    # Setup warehouse and dbt workspace
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )

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

    result = get_input_sources_and_models_v2(request)

    assert result == []


def test_get_input_sources_and_models_v2_with_models_and_sources(seed_db, orguser: OrgUser):
    """Test success with models and sources, excluding under_construction models"""
    # Setup warehouse and dbt workspace
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )

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

    # Create a completed model (should be included)
    completed_model = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="completed_model",
        display_name="Completed Model",
        schema="public",
        type=OrgDbtModelType.MODEL,
        sql_path="models/completed_model.sql",
        under_construction=False,
        output_cols=["col1", "col2"],
    )

    # Create a source (should be included)
    source_model = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="source_table",
        display_name="Source Table",
        schema="raw",
        type=OrgDbtModelType.SOURCE,
        source_name="my_source",
        under_construction=False,
        output_cols=["id", "name"],
    )

    # Create an under_construction model (should be excluded)
    under_construction_model = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="under_construction_model",
        display_name="Under Construction Model",
        schema="public",
        type=OrgDbtModelType.MODEL,
        under_construction=True,
        output_cols=["col3"],
    )

    request = mock_request(orguser)

    result = get_input_sources_and_models_v2(request)

    # Should return 2 items (completed_model and source_model), excluding under_construction
    assert len(result) == 2

    # Find the completed model in results
    completed_model_result = next((r for r in result if r["name"] == "completed_model"), None)
    assert completed_model_result is not None
    assert completed_model_result["schema"] == "public"
    assert completed_model_result["sql_path"] == "models/completed_model.sql"
    assert completed_model_result["display_name"] == "Completed Model"
    assert completed_model_result["type"] == OrgDbtModelType.MODEL
    assert completed_model_result["source_name"] is None
    assert completed_model_result["output_cols"] == ["col1", "col2"]
    assert completed_model_result["uuid"] == str(completed_model.uuid)

    # Find the source in results
    source_result = next((r for r in result if r["name"] == "source_table"), None)
    assert source_result is not None
    assert source_result["schema"] == "raw"
    assert source_result["display_name"] == "Source Table"
    assert source_result["type"] == OrgDbtModelType.SOURCE
    assert source_result["source_name"] == "my_source"
    assert source_result["output_cols"] == ["id", "name"]
    assert source_result["uuid"] == str(source_model.uuid)

    # Ensure under_construction model is not included
    under_construction_result = next(
        (r for r in result if r["name"] == "under_construction_model"), None
    )
    assert under_construction_result is None


def test_get_input_sources_and_models_v2_with_schema_filter(seed_db, orguser: OrgUser):
    """Test success with schema name filter"""
    # Setup warehouse and dbt workspace
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )

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

    # Create models in different schemas
    model_public = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="model_public",
        display_name="Model Public",
        schema="public",
        type=OrgDbtModelType.MODEL,
        under_construction=False,
        output_cols=["col1"],
    )

    model_raw = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="model_raw",
        display_name="Model Raw",
        schema="raw",
        type=OrgDbtModelType.MODEL,
        under_construction=False,
        output_cols=["col2"],
    )

    request = mock_request(orguser)

    # Test with schema filter "public"
    result = get_input_sources_and_models_v2(request, schema_name="public")

    assert len(result) == 1
    assert result[0]["name"] == "model_public"
    assert result[0]["schema"] == "public"

    # Test with schema filter "raw"
    result = get_input_sources_and_models_v2(request, schema_name="raw")

    assert len(result) == 1
    assert result[0]["name"] == "model_raw"
    assert result[0]["schema"] == "raw"

    # Test with schema filter that doesn't exist
    result = get_input_sources_and_models_v2(request, schema_name="nonexistent")

    assert len(result) == 0


def test_delete_orgdbtmodel_cascade_not_implemented(seed_db, orguser: OrgUser):
    """Test that cascade=True raises NotImplementedError"""
    request = mock_request(orguser)

    with pytest.raises(NotImplementedError):
        delete_orgdbtmodel(request, "test-uuid", cascade=True)


def test_delete_orgdbtmodel_warehouse_not_setup(seed_db, orguser: OrgUser):
    """Test failure when warehouse is not setup"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        delete_orgdbtmodel(request, "test-uuid")

    assert str(excinfo.value) == "please setup your warehouse first"


def test_delete_orgdbtmodel_dbt_workspace_not_setup(seed_db, orguser: OrgUser):
    """Test failure when dbt workspace is not setup"""
    # Create warehouse but no dbt workspace
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )

    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        delete_orgdbtmodel(request, "test-uuid")

    assert str(excinfo.value) == "dbt workspace not setup"


def test_delete_orgdbtmodel_canvas_lock_validation_fails(seed_db, orguser: OrgUser):
    """Test failure when canvas lock validation fails"""
    # Setup warehouse and dbt workspace
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )

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

    # Mock validate_canvas_lock to raise an HttpError
    with patch("ddpui.api.transform_api.validate_canvas_lock") as mock_validate_lock:
        mock_validate_lock.side_effect = HttpError(423, "Canvas is not locked")

        with pytest.raises(HttpError) as excinfo:
            delete_orgdbtmodel(request, "test-uuid")

        assert str(excinfo.value) == "Canvas is not locked"
        mock_validate_lock.assert_called_once_with(orguser, orgdbt)


def test_delete_orgdbtmodel_model_not_found(seed_db, orguser: OrgUser):
    """Test failure when model is not found"""
    # Setup warehouse and dbt workspace
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )

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

    # Use a valid UUID format that doesn't exist in database
    nonexistent_uuid = str(uuid.uuid4())

    # Mock validate_canvas_lock to pass
    with patch("ddpui.api.transform_api.validate_canvas_lock"):
        with pytest.raises(HttpError) as excinfo:
            delete_orgdbtmodel(request, nonexistent_uuid)

        assert str(excinfo.value) == "model not found"


def test_delete_orgdbtmodel_cannot_delete_source(seed_db, orguser: OrgUser):
    """Test failure when trying to delete a source model"""
    # Setup warehouse and dbt workspace
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )

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

    # Create a source model
    source_model = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="source_table",
        display_name="Source Table",
        schema="raw",
        type=OrgDbtModelType.SOURCE,
        source_name="my_source",
        under_construction=False,
        uuid=uuid.uuid4(),
    )

    request = mock_request(orguser)

    # Mock validate_canvas_lock to pass
    with patch("ddpui.api.transform_api.validate_canvas_lock"):
        with pytest.raises(HttpError) as excinfo:
            delete_orgdbtmodel(request, str(source_model.uuid))

        assert str(excinfo.value) == "Cannot delete source model"


def test_delete_orgdbtmodel_success(seed_db, orguser: OrgUser):
    """Test successful deletion of a model"""
    # Setup warehouse and dbt workspace
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )

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

    # Create a model (not source)
    model = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="test_model",
        display_name="Test Model",
        schema="public",
        type=OrgDbtModelType.MODEL,
        under_construction=False,
        sql_path="models/test_model.sql",
        uuid=uuid.uuid4(),
    )

    request = mock_request(orguser)

    # Mock both validate_canvas_lock and delete_org_dbt_model_v2
    with patch("ddpui.api.transform_api.validate_canvas_lock"), patch(
        "ddpui.core.dbtautomation_service.delete_org_dbt_model_v2"
    ) as mock_delete:
        result = delete_orgdbtmodel(request, str(model.uuid))

        # Verify the service function was called with correct parameters
        mock_delete.assert_called_once_with(model, False)

        # Verify the response
        assert result == {"success": 1}


def test_get_dbt_project_DAG_v2_warehouse_not_setup(seed_db, orguser: OrgUser):
    """Test failure when warehouse is not setup"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_dbt_project_DAG_v2(request)

    assert str(excinfo.value) == "please setup your warehouse first"


def test_get_dbt_project_DAG_v2_dbt_workspace_not_setup(seed_db, orguser: OrgUser):
    """Test failure when dbt workspace is not setup"""
    # Create warehouse but no dbt workspace
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )

    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_dbt_project_DAG_v2(request)

    assert str(excinfo.value) == "dbt workspace not setup"


def test_get_dbt_project_DAG_v2_empty_canvas(seed_db, orguser: OrgUser, tmp_path):
    """Test success with empty canvas (no nodes or edges)"""
    # Setup warehouse and dbt workspace
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )

    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir=str(tmp_path),
        dbt_venv="test_venv",
        target_type="postgres",
        default_schema="default_schema",
        transform_type=TransformType.UI,
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    request = mock_request(orguser)

    # Mock DbtProjectManager.get_dbt_project_dir
    with patch("ddpui.api.transform_api.DbtProjectManager.get_dbt_project_dir") as mock_get_dir:
        mock_get_dir.return_value = str(tmp_path)

        result = get_dbt_project_DAG_v2(request)

        assert result == {"nodes": [], "edges": []}


def test_get_dbt_project_DAG_v2_with_canvas_graph(seed_db, orguser: OrgUser, tmp_path):
    """Test success with canvas nodes and edges using helper function"""
    # Setup warehouse and dbt workspace
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )

    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir=str(tmp_path),
        dbt_venv="test_venv",
        target_type="postgres",
        default_schema="default_schema",
        transform_type=TransformType.UI,
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    # Create canvas graph using helper function
    canvas_data = create_canvas_graph(orgdbt)

    request = mock_request(orguser)

    # Mock required functions - patch where the function is imported in transform_api
    with patch(
        "ddpui.api.transform_api.DbtProjectManager.get_dbt_project_dir"
    ) as mock_get_dir, patch(
        "ddpui.api.transform_api.convert_canvas_node_to_frontend_format"
    ) as mock_convert:
        mock_get_dir.return_value = str(tmp_path)

        # Mock convert function to return simple format
        def mock_convert_side_effect(node, changed_files=None):
            return {
                "id": str(node.uuid),
                "type": node.node_type,
                "name": node.name,
                "output_cols": node.output_cols,
            }

        mock_convert.side_effect = mock_convert_side_effect

        result = get_dbt_project_DAG_v2(request)

        # Verify structure
        assert "nodes" in result
        assert "edges" in result

        # Verify 3 nodes (source, operation, model)
        assert len(result["nodes"]) == 3

        # Verify 2 edges (source->operation, operation->model)
        assert len(result["edges"]) == 2

        # Verify edge format
        expected_edges = [
            {
                "id": f"{canvas_data['source_node'].uuid}_{canvas_data['operation_node'].uuid}",
                "source": str(canvas_data["source_node"].uuid),
                "target": str(canvas_data["operation_node"].uuid),
            },
            {
                "id": f"{canvas_data['operation_node'].uuid}_{canvas_data['model_node'].uuid}",
                "source": str(canvas_data["operation_node"].uuid),
                "target": str(canvas_data["model_node"].uuid),
            },
        ]

        # Check edges (order may vary)
        for expected_edge in expected_edges:
            assert expected_edge in result["edges"]

        # Verify convert function was called for each node
        assert mock_convert.call_count == 3


def test_get_dbt_project_DAG_v2_project_directory_not_exist(seed_db, orguser: OrgUser):
    """Test failure when dbt project directory does not exist"""
    # Setup warehouse and dbt workspace
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )

    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir="nonexistent_project_dir",
        dbt_venv="test_venv",
        target_type="postgres",
        default_schema="default_schema",
        transform_type=TransformType.UI,
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    request = mock_request(orguser)

    # Mock DbtProjectManager.get_dbt_project_dir to return nonexistent path
    with patch("ddpui.api.transform_api.DbtProjectManager.get_dbt_project_dir") as mock_get_dir:
        mock_get_dir.return_value = "/nonexistent/path"

        with pytest.raises(HttpError) as excinfo:
            get_dbt_project_DAG_v2(request)

        assert str(excinfo.value) == "dbt project directory does not exist"


# ==================== Tests for post_create_src_model_node ====================


def test_post_create_src_model_node_warehouse_not_setup(seed_db, orguser):
    """Test post_create_src_model_node when warehouse is not setup"""
    request = mock_request(orguser)

    # Ensure no warehouse exists
    OrgWarehouse.objects.filter(org=orguser.org).delete()

    with pytest.raises(HttpError) as excinfo:
        post_create_src_model_node(request, str(uuid.uuid4()))

    assert str(excinfo.value) == "please setup your warehouse first"


def test_post_create_src_model_node_dbt_workspace_not_setup(seed_db, orguser):
    """Test post_create_src_model_node when dbt workspace is not setup"""
    request = mock_request(orguser)

    # Create warehouse but ensure no dbt workspace exists
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="test_destination_id",
    )
    orguser.org.dbt = None
    orguser.org.save()

    with pytest.raises(HttpError) as excinfo:
        post_create_src_model_node(request, str(uuid.uuid4()))

    assert str(excinfo.value) == "dbt workspace not setup"


def test_post_create_src_model_node_model_not_found(seed_db, orguser):
    """Test post_create_src_model_node when model is not found"""
    request = mock_request(orguser)

    # Create warehouse and dbt workspace
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="test_destination_id",
    )
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

    with pytest.raises(HttpError) as excinfo:
        post_create_src_model_node(request, str(uuid.uuid4()))

    assert str(excinfo.value) == "model not found"


@patch("ddpui.api.transform_api.dbtautomation_service.ensure_source_yml_definition_in_project")
@patch("ddpui.api.transform_api.dbtautomation_service.update_output_cols_of_dbt_model")
@patch("ddpui.api.transform_api.convert_canvas_node_to_frontend_format")
def test_post_create_src_model_node_source_type_success(
    mock_convert_canvas, mock_update_cols, mock_ensure_source, seed_db, orguser
):
    """Test post_create_src_model_node for SOURCE type - successful creation"""
    request = mock_request(orguser)

    # Create warehouse and dbt workspace
    org_warehouse = OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="test_destination_id",
    )
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

    # Create SOURCE type dbt model
    org_dbt_model = OrgDbtModel.objects.create(
        uuid=uuid.uuid4(),
        orgdbt=orgdbt,
        name="test_source",
        schema="public",
        type=OrgDbtModelType.SOURCE,
        display_name="Test Source",
        sql_path="models/source.sql",
    )

    # Mock the source definition response
    mock_source_def = Mock()
    mock_source_def.sql_path = "models/schema.yml"
    mock_source_def.source_name = "test_source_name"
    mock_source_def.table = "test_table"
    mock_source_def.source_schema = "public"
    mock_ensure_source.return_value = mock_source_def

    # Mock update_output_cols to return sample columns
    mock_update_cols.return_value = ["id", "name", "email"]

    # Mock convert_canvas_node_to_frontend_format
    mock_convert_canvas.return_value = {"uuid": str(org_dbt_model.uuid), "name": "test_source"}

    # Call the function
    result = post_create_src_model_node(request, str(org_dbt_model.uuid))

    # Verify mocks were called correctly
    mock_ensure_source.assert_called_once_with(orgdbt, "public", "test_source")
    mock_update_cols.assert_called_once_with(org_warehouse, org_dbt_model)
    mock_convert_canvas.assert_called_once()

    # Verify the model was updated with source definition values
    org_dbt_model.refresh_from_db()
    assert org_dbt_model.sql_path == "models/schema.yml"
    assert org_dbt_model.source_name == "test_source_name"
    assert org_dbt_model.name == "test_table"
    assert org_dbt_model.schema == "public"
    assert org_dbt_model.output_cols == ["id", "name", "email"]

    # Verify canvas node was created
    canvas_node = CanvasNode.objects.get(dbtmodel=org_dbt_model)
    assert canvas_node.node_type == CanvasNodeType.SOURCE
    assert canvas_node.name == "public.test_table"
    assert canvas_node.output_cols == ["id", "name", "email"]

    # Verify result
    assert result == {"uuid": str(org_dbt_model.uuid), "name": "test_source"}


@patch("ddpui.api.transform_api.dbtautomation_service.update_output_cols_of_dbt_model")
@patch("ddpui.api.transform_api.convert_canvas_node_to_frontend_format")
def test_post_create_src_model_node_model_type_success(
    mock_convert_canvas, mock_update_cols, seed_db, orguser
):
    """Test post_create_src_model_node for MODEL type - successful creation"""
    request = mock_request(orguser)

    # Create warehouse and dbt workspace
    org_warehouse = OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="test_destination_id",
    )
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

    # Create MODEL type dbt model
    org_dbt_model = OrgDbtModel.objects.create(
        uuid=uuid.uuid4(),
        orgdbt=orgdbt,
        name="test_model",
        schema="analytics",
        type=OrgDbtModelType.MODEL,
        display_name="Test Model",
        sql_path="models/test_model.sql",
    )

    # Mock update_output_cols to return sample columns
    mock_update_cols.return_value = ["id", "name", "processed_at"]

    # Mock convert_canvas_node_to_frontend_format
    mock_convert_canvas.return_value = {"uuid": str(org_dbt_model.uuid), "name": "test_model"}

    # Call the function
    result = post_create_src_model_node(request, str(org_dbt_model.uuid))

    # Verify mock was called correctly
    mock_update_cols.assert_called_once_with(org_warehouse, org_dbt_model)
    mock_convert_canvas.assert_called_once()

    # Verify the model was updated with output columns
    org_dbt_model.refresh_from_db()
    assert org_dbt_model.output_cols == ["id", "name", "processed_at"]

    # Verify canvas node was created
    canvas_node = CanvasNode.objects.get(dbtmodel=org_dbt_model)
    assert canvas_node.node_type == CanvasNodeType.MODEL
    assert canvas_node.name == "analytics.test_model"
    assert canvas_node.output_cols == ["id", "name", "processed_at"]

    # Verify result
    assert result == {"uuid": str(org_dbt_model.uuid), "name": "test_model"}


@patch("ddpui.api.transform_api.convert_canvas_node_to_frontend_format")
def test_post_create_src_model_node_existing_node_returns_existing(
    mock_convert_canvas, seed_db, orguser
):
    """Test post_create_src_model_node returns existing canvas node if already exists"""
    request = mock_request(orguser)

    # Create warehouse and dbt workspace
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="test_destination_id",
    )
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

    # Create dbt model
    org_dbt_model = OrgDbtModel.objects.create(
        uuid=uuid.uuid4(),
        orgdbt=orgdbt,
        name="existing_model",
        schema="public",
        type=OrgDbtModelType.MODEL,
        display_name="Existing Model",
    )

    # Create existing canvas node
    existing_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.MODEL,
        name="public.existing_model",
        dbtmodel=org_dbt_model,
        output_cols=["id", "name"],
    )

    # Mock convert_canvas_node_to_frontend_format
    mock_convert_canvas.return_value = {"uuid": str(existing_node.uuid), "name": "existing_model"}

    # Call the function
    result = post_create_src_model_node(request, str(org_dbt_model.uuid))

    # Verify convert function was called with existing node
    mock_convert_canvas.assert_called_once_with(existing_node)

    # Verify no new canvas node was created
    assert CanvasNode.objects.filter(dbtmodel=org_dbt_model).count() == 1

    # Verify result
    assert result == {"uuid": str(existing_node.uuid), "name": "existing_model"}


@patch("ddpui.api.transform_api.dbtautomation_service.ensure_source_yml_definition_in_project")
def test_post_create_src_model_node_source_definition_error(mock_ensure_source, seed_db, orguser):
    """Test post_create_src_model_node handles error in source definition creation"""
    request = mock_request(orguser)

    # Create warehouse and dbt workspace
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="test_destination_id",
    )
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

    # Create SOURCE type dbt model
    org_dbt_model = OrgDbtModel.objects.create(
        uuid=uuid.uuid4(),
        orgdbt=orgdbt,
        name="test_source",
        schema="public",
        type=OrgDbtModelType.SOURCE,
        display_name="Test Source",
    )

    # Mock ensure_source_yml_definition_in_project to raise exception
    mock_ensure_source.side_effect = Exception("Failed to create source definition")

    # Call the function and expect HttpError
    with pytest.raises(HttpError) as excinfo:
        post_create_src_model_node(request, str(org_dbt_model.uuid))

    assert str(excinfo.value) == "Failed to create node: Failed to create source definition"

    # Verify ensure_source was called
    mock_ensure_source.assert_called_once_with(orgdbt, "public", "test_source")

    # Verify no canvas node was created
    assert not CanvasNode.objects.filter(dbtmodel=org_dbt_model).exists()


@patch("ddpui.api.transform_api.dbtautomation_service.update_output_cols_of_dbt_model")
def test_post_create_src_model_node_update_cols_error(mock_update_cols, seed_db, orguser):
    """Test post_create_src_model_node handles error in update_output_cols"""
    request = mock_request(orguser)

    # Create warehouse and dbt workspace
    org_warehouse = OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="test_destination_id",
    )
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

    # Create MODEL type dbt model
    org_dbt_model = OrgDbtModel.objects.create(
        uuid=uuid.uuid4(),
        orgdbt=orgdbt,
        name="test_model",
        schema="public",
        type=OrgDbtModelType.MODEL,
        display_name="Test Model",
    )

    # Mock update_output_cols to raise exception
    mock_update_cols.side_effect = Exception("Failed to update columns")

    # Call the function and expect HttpError
    with pytest.raises(HttpError) as excinfo:
        post_create_src_model_node(request, str(org_dbt_model.uuid))

    assert str(excinfo.value) == "Failed to create node: Failed to update columns"

    # Verify update_cols was called
    mock_update_cols.assert_called_once_with(org_warehouse, org_dbt_model)

    # Verify no canvas node was created
    assert not CanvasNode.objects.filter(dbtmodel=org_dbt_model).exists()


@patch("ddpui.api.transform_api.dbtautomation_service.ensure_source_yml_definition_in_project")
@patch("ddpui.api.transform_api.dbtautomation_service.update_output_cols_of_dbt_model")
@patch("ddpui.api.transform_api.convert_canvas_node_to_frontend_format")
def test_post_create_src_model_node_source_no_existing_node_calls_all_functions(
    mock_convert_canvas, mock_update_cols, mock_ensure_source, seed_db, orguser
):
    """Test that all required functions are called for SOURCE type when no existing node"""
    request = mock_request(orguser)

    # Create warehouse and dbt workspace
    org_warehouse = OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="test_destination_id",
    )
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

    # Create SOURCE type dbt model
    org_dbt_model = OrgDbtModel.objects.create(
        uuid=uuid.uuid4(),
        orgdbt=orgdbt,
        name="new_source",
        schema="public",
        type=OrgDbtModelType.SOURCE,
        display_name="New Source",
    )

    # Mock responses
    mock_source_def = Mock()
    mock_source_def.sql_path = "schema.yml"
    mock_source_def.source_name = "source_name"
    mock_source_def.table = "table_name"
    mock_source_def.source_schema = "public"
    mock_ensure_source.return_value = mock_source_def

    mock_update_cols.return_value = ["col1", "col2"]
    mock_convert_canvas.return_value = {"result": "success"}

    # Call the function
    result = post_create_src_model_node(request, str(org_dbt_model.uuid))

    # Verify all functions were called in correct order
    mock_ensure_source.assert_called_once_with(orgdbt, "public", "new_source")
    mock_update_cols.assert_called_once_with(org_warehouse, org_dbt_model)
    mock_convert_canvas.assert_called_once()

    # Verify result
    assert result == {"result": "success"}


@patch("ddpui.api.transform_api.dbtautomation_service.update_output_cols_of_dbt_model")
@patch("ddpui.api.transform_api.convert_canvas_node_to_frontend_format")
def test_post_create_src_model_node_model_no_existing_node_calls_required_functions(
    mock_convert_canvas, mock_update_cols, seed_db, orguser
):
    """Test that required functions are called for MODEL type when no existing node"""
    request = mock_request(orguser)

    # Create warehouse and dbt workspace
    org_warehouse = OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="test_destination_id",
    )
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

    # Create MODEL type dbt model
    org_dbt_model = OrgDbtModel.objects.create(
        uuid=uuid.uuid4(),
        orgdbt=orgdbt,
        name="new_model",
        schema="analytics",
        type=OrgDbtModelType.MODEL,
        display_name="New Model",
    )

    # Mock responses
    mock_update_cols.return_value = ["id", "value"]
    mock_convert_canvas.return_value = {"result": "model_success"}

    # Call the function
    result = post_create_src_model_node(request, str(org_dbt_model.uuid))

    # Verify required functions were called
    mock_update_cols.assert_called_once_with(org_warehouse, org_dbt_model)
    mock_convert_canvas.assert_called_once()

    # Verify result
    assert result == {"result": "model_success"}
