import os, glob
from pathlib import Path
from pydantic.error_wrappers import ValidationError as PydanticValidationError
import pydantic
import pytest
from unittest.mock import Mock, patch, call
from datetime import datetime
from ninja.errors import HttpError, ValidationError
from dbt_automation import assets
from ddpui.models.role_based_access import Role, RolePermission, Permission
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgWarehouse, OrgDbt
from ddpui.models.dbt_workflow import OrgDbtModel, OrgDbtOperation
from ddpui.auth import (
    GUEST_ROLE,
    SUPER_ADMIN_ROLE,
    ACCOUNT_MANAGER_ROLE,
    PIPELINE_MANAGER_ROLE,
    ANALYST_ROLE,
)
from ddpui.schemas.org_task_schema import DbtProjectSchema
from ddpui.ddpprefect.schema import DbtProfile, OrgDbtSchema
from ddpui.celeryworkers.tasks import setup_dbtworkspace
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
    post_construct_dbt_model_operation,
    put_operation,
    get_operation,
    post_save_model,
    get_input_sources_and_models,
    get_dbt_project_DAG,
    delete_model,
    delete_operation,
    get_warehouse_datatypes,
    post_lock_canvas,
    LockCanvasRequestSchema,
    CanvasLock,
    post_unlock_canvas,
)
from ddpui.schemas.dbt_workflow_schema import CreateDbtModelPayload
from ddpui.utils.taskprogress import TaskProgress
from ddpui.ddpdbt.dbt_service import setup_local_dbt_workspace
from ddpui.core.dbtautomation_service import sync_sources_for_warehouse
from dbt_automation.utils.warehouseclient import PostgresClient

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
        sync_sources_for_warehouse(orgdbt.id, warehouse.id, warehouse.org.slug)
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


def test_post_lock_canvas_acquire(orguser):
    """a test to lock the canvas"""
    request = mock_request(orguser)
    payload = LockCanvasRequestSchema()
    response = post_lock_canvas(request, payload)
    assert response is not None
    assert response.locked_by == orguser.user.email
    assert response.lock_id is not None


def test_post_lock_canvas_locked_by_another(orguser, nonadminorguser):
    """a test to lock the canvas"""
    CanvasLock.objects.create(locked_by=nonadminorguser, locked_at=datetime.now())
    request = mock_request(orguser)
    payload = LockCanvasRequestSchema()
    response = post_lock_canvas(request, payload)
    assert response is not None
    assert response.locked_by == nonadminorguser.user.email
    assert response.lock_id is None


def test_post_lock_canvas_locked_by_self(orguser):
    """a test to lock the canvas"""
    request = mock_request(orguser)
    payload = LockCanvasRequestSchema()
    post_lock_canvas(request, payload)
    # the second call won't acquire a lock
    response = post_lock_canvas(request, payload)
    assert response is not None
    assert response.locked_by == orguser.user.email
    assert response.lock_id is None


def test_post_unlock_canvas_unlocked(orguser):
    """a test to unlock the canvas"""
    request = mock_request(orguser)
    payload = LockCanvasRequestSchema()
    with pytest.raises(HttpError) as excinfo:
        post_unlock_canvas(request, payload)
    assert str(excinfo.value) == "no lock found"


def test_post_unlock_canvas_locked(orguser):
    """a test to unlock the canvas"""
    request = mock_request(orguser)
    payload = LockCanvasRequestSchema()
    post_lock_canvas(request, payload)

    with pytest.raises(HttpError) as excinfo:
        post_unlock_canvas(request, payload)
    assert str(excinfo.value) == "wrong lock id"


def test_post_unlock_canvas_locked_unlocked(orguser):
    """a test to unlock the canvas"""
    request = mock_request(orguser)
    payload = LockCanvasRequestSchema()
    lock = post_lock_canvas(request, payload)

    payload.lock_id = lock.lock_id
    response = post_unlock_canvas(request, payload)
    assert response == {"success": 1}


def test_post_unlock_canvas_locked_wrong_lock_id(orguser):
    """a test to unlock the canvas"""
    request = mock_request(orguser)
    payload = LockCanvasRequestSchema()
    post_lock_canvas(request, payload)

    payload.lock_id = "wrong-lock-id"
    with pytest.raises(HttpError) as excinfo:
        post_unlock_canvas(request, payload)
    assert str(excinfo.value) == "wrong lock id"


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
        with pytest.raises(HttpError) as exc:
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


def test_delete_dbt_project_failure_projectdir_does_not_exist(
    orguser: OrgUser, tmp_path
):
    """a failure test for delete a dbt project api when project dir does not exist"""
    request = mock_request(orguser)
    project_name = "dummy-project"
    with patch("os.getenv", return_value=tmp_path):
        with pytest.raises(HttpError) as excinfo:
            delete_dbt_project(request, project_name)
    assert (
        str(excinfo.value)
        == f"Organization {orguser.org.slug} does not have any projects"
    )


def test_delete_dbt_project_failure_dbtrepodir_does_not_exist(
    orguser: OrgUser, tmp_path
):
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
        transform_type="github",
    )
    orguser.org.dbt = dbt
    orguser.org.save()

    assert OrgDbt.objects.filter(org=orguser.org).count() == 1

    with patch("os.getenv", return_value=tmp_path):
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
        transform_type="ui",
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    request = mock_request(orguser)
    mocked_task = Mock()
    mocked_task.id = "task-id"
    with patch(
        "ddpui.core.dbtautomation_service.sync_sources_for_warehouse.delay",
        return_value=mocked_task,
    ) as delay:
        result = sync_sources(request)

        delay.assert_called_once_with(orgdbt.id, org_warehouse.id, orguser.org.slug)
        assert result["task_id"] == "task-id"


def test_post_construct_dbt_model_operation_failure_warehouse_not_setup(
    orguser: OrgUser,
):
    """a failure test for constructiong model/operation due to warehouse not setup"""
    os.environ["CANVAS_LOCK"] = "False"
    request = mock_request(orguser)
    payload = CreateDbtModelPayload(
        config={}, op_type="drop", target_model_uuid="", input_uuid=""
    )
    with pytest.raises(HttpError) as excinfo:
        post_construct_dbt_model_operation(request, payload)
    assert str(excinfo.value) == "please setup your warehouse first"


def test_post_construct_dbt_model_operation_failure_dbt_workspace_not_setup(
    orguser: OrgUser,
):
    """a failure test for constructiong model/operation due to warehouse not setup"""
    os.environ["CANVAS_LOCK"] = "False"
    request = mock_request(orguser)
    payload = CreateDbtModelPayload(
        config={}, op_type="drop", target_model_uuid="", input_uuid=""
    )

    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )

    with pytest.raises(HttpError) as excinfo:
        post_construct_dbt_model_operation(request, payload)
    assert str(excinfo.value) == "dbt workspace not setup"


def test_post_construct_dbt_model_operation_failure_invalid_op_type(
    orguser: OrgUser, tmp_path
):
    """a failure test for constructiong model/operation due to invalid op type"""
    os.environ["CANVAS_LOCK"] = "False"
    request = mock_request(orguser)
    payload = CreateDbtModelPayload(
        config={},
        op_type="some-random-op-type-not-supported",
        target_model_uuid="",
        input_uuid="",
    )

    OrgWarehouse.objects.create(
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
        transform_type="ui",
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    with pytest.raises(HttpError) as excinfo:
        post_construct_dbt_model_operation(request, payload)
    assert str(excinfo.value) == "Operation not supported"


def test_post_construct_dbt_model_operation_success_chain1(orguser: OrgUser, tmp_path):
    """
    a success test: | model | -> | op (cast) | -> | op (arithmetic) | -> | target_model |
    Single input operation
    Chain 2 operations
    """
    os.environ["CANVAS_LOCK"] = "False"
    warehouse = OrgWarehouse.objects.create(
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
        transform_type="ui",
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    mock_setup_dbt_workspace_ui_transform(orguser, tmp_path)
    mock_setup_sync_sources(orgdbt, warehouse)

    source = OrgDbtModel.objects.filter(orgdbt=orgdbt, type="source").first()
    # Create a model
    payload = CreateDbtModelPayload(
        target_model_uuid="",
        source_columns=["State", "District_Name", "Iron", "Arsenic"],
        op_type="castdatatypes",
        config={
            "columns": [
                {"columnname": "Iron", "columntype": "INT"},
                {"columnname": "Arsenic", "columntype": "INT"},
            ]
        },
        input_uuid=str(source.uuid),
    )

    request = mock_request(orguser)
    with patch(
        "ddpui.core.dbtautomation_service._get_wclient",
    ) as get_wclient_mock:
        mock_instance = Mock()
        mock_instance.name = "postgres"
        get_wclient_mock.return_value = mock_instance

        # push cast operation
        response1 = post_construct_dbt_model_operation(request, payload)

        assert response1 is not None
        assert "target_model_id" in response1

        target_model = OrgDbtModel.objects.filter(
            uuid=response1["target_model_id"]
        ).first()
        assert target_model is not None
        assert target_model.under_construction is True

        assert "output_cols" in response1
        assert response1["output_cols"] == payload.source_columns

        assert OrgDbtOperation.objects.filter(dbtmodel=target_model).count() == 1
        cast_op = OrgDbtOperation.objects.filter(dbtmodel=target_model).first()
        assert cast_op is not None
        assert cast_op.config["type"] == "castdatatypes"
        assert cast_op.output_cols == payload.source_columns
        assert cast_op.seq == 1
        assert cast_op.config["config"] == payload.config

        # push arithmetic operation
        payload.target_model_uuid = target_model.uuid
        payload.op_type = "arithmetic"
        payload.config = {
            "operator": "add",
            "operands": [
                {"is_col": True, "value": "Iron"},
                {"is_col": True, "value": "Arsenic"},
            ],
            "output_column_name": "IronPlusArsenic",
        }
        response2 = post_construct_dbt_model_operation(request, payload)

        assert response2 is not None
        assert "target_model_id" in response2
        assert response2["target_model_id"] == target_model.uuid

        assert "output_cols" in response2
        assert response2["output_cols"] == payload.source_columns + ["IronPlusArsenic"]

        assert OrgDbtOperation.objects.filter(dbtmodel=target_model).count() == 2
        cast_op = OrgDbtOperation.objects.filter(dbtmodel=target_model, seq=2).first()
        assert cast_op is not None
        assert cast_op.config["type"] == "arithmetic"
        assert cast_op.output_cols == payload.source_columns + ["IronPlusArsenic"]
        assert cast_op.config["config"] == payload.config
