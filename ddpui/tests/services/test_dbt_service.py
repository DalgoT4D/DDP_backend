import os, glob
import subprocess
from pathlib import Path
from unittest.mock import patch, Mock, MagicMock
import django
import pytest
import json

from ddpui.models.org import Org, OrgDbt, OrgPrefectBlockv1, OrgWarehouse
from ddpui.models.canvas_models import CanvasNode, CanvasNodeType, CanvasEdge
from ddpui.core.orgdbt_manager import DbtCommandError
from ddpui.ddpdbt.dbt_service import (
    delete_dbt_workspace,
    setup_local_dbt_workspace,
    parse_dbt_manifest_to_canvas,
    generate_manifest_json_for_dbt_project,
)
from ddpui.ddpprefect import DBTCLIPROFILE, SECRET
from ddpui.core.orgdbt_manager import DbtCommandError
from ddpui.dbt_automation import assets

pytestmark = pytest.mark.django_db

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


def test_delete_dbt_workspace():
    """tests the delete_dbt_workspace function"""
    org = Org.objects.create(name="temp", slug="temp")

    cli_block = OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=DBTCLIPROFILE,
        block_id="dbtcli-block-id",
        block_name="dbtcli-block-id",
    )

    org.dbt = OrgDbt.objects.create(
        gitrepo_url="gitrepo_url",
        project_dir="project-dir",
        target_type="tgt",
        default_schema="default_schema",
        cli_profile_block=cli_block,
    )
    org.save()

    assert OrgDbt.objects.filter(gitrepo_url="gitrepo_url").count() == 1
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

    with pytest.raises(Exception) as excinfo:
        setup_local_dbt_workspace(org, project_name="dbtrepo", default_schema="default")
    assert str(excinfo.value) == "Please set up your warehouse first"


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
        with pytest.raises(Exception) as excinfo:
            setup_local_dbt_workspace(org, project_name=project_name, default_schema=default_schema)
        assert str(excinfo.value) == f"Project {project_name} already exists"


def test_setup_local_dbt_workspace_dbt_init_failed(tmp_path):
    """a failure test; setup fails because dbt init failed"""
    project_name = "dbtrepo"
    default_schema = "default"

    org = Org.objects.create(name="temp", slug="temp")

    OrgWarehouse.objects.create(org=org, wtype="postgres")

    # Mock the DbtProjectManager methods that would be called
    with patch("os.getenv", return_value=tmp_path), patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.run_dbt_command"
    ) as mock_run_command, patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.gather_dbt_project_params"
    ) as mock_gather_params, patch(
        "ddpui.ddpdbt.dbt_service.secretsmanager.retrieve_warehouse_credentials", return_value={}
    ) as mock_retrieve_creds, patch(
        "ddpui.ddpdbt.dbt_service.create_or_update_org_cli_block", return_value=((None, None), None)
    ) as mock_create_cli_block:
        # Mock DbtCommandError for dbt init failure
        mock_run_command.side_effect = DbtCommandError("dbt init failed", "command failed")

        with pytest.raises(Exception) as excinfo:
            setup_local_dbt_workspace(org, project_name=project_name, default_schema=default_schema)
        assert "dbt init failed" in str(excinfo.value)
        mock_run_command.assert_called_once()
        # retrieve_warehouse_credentials and cli block creation are not called when dbt init fails early
        mock_retrieve_creds.assert_not_called()
        mock_create_cli_block.assert_not_called()


def test_setup_local_dbt_workspace_success(tmp_path):
    """a success test for creating local dbt workspace"""
    project_name = "dbtrepo"
    default_schema = "default"

    org = Org.objects.create(name="temp", slug="temp")

    OrgWarehouse.objects.create(org=org, wtype="postgres")
    project_dir: Path = Path(tmp_path) / org.slug
    dbtrepo_dir: Path = project_dir / project_name

    def mock_run_dbt_init(*args, **kwargs):
        # Create the directories that would be created by dbt init
        os.makedirs(dbtrepo_dir, exist_ok=True)
        os.makedirs(dbtrepo_dir / "macros", exist_ok=True)
        # Return a mock CompletedProcess
        return Mock(returncode=0, stdout="", stderr="")

    # Mock the DbtProjectManager methods and other dependencies
    with patch("os.getenv", return_value=tmp_path), patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.run_dbt_command", side_effect=mock_run_dbt_init
    ) as mock_run_command, patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.gather_dbt_project_params"
    ) as mock_gather_params, patch(
        "ddpui.ddpdbt.dbt_service.secretsmanager.retrieve_warehouse_credentials", return_value={}
    ) as mock_retrieve_creds, patch(
        "ddpui.ddpdbt.dbt_service.create_or_update_org_cli_block", return_value=((None, None), None)
    ) as mock_create_cli_block:
        # Mock gather_dbt_project_params to return valid params (called during CLI block creation)
        from ddpui.ddpdbt.schema import DbtProjectParams

        mock_gather_params.return_value = DbtProjectParams(
            dbt_binary="/mock/dbt",
            dbt_env_dir="/mock/env",
            venv_binary="/mock/bin",
            target=default_schema,
            project_dir=str(dbtrepo_dir),
            org_project_dir=str(project_dir),
        )

        setup_local_dbt_workspace(org, project_name=project_name, default_schema=default_schema)

        # Verify the dbt command was called for init
        mock_run_command.assert_called_once()
        args = mock_run_command.call_args[0][2]  # third argument is the command list
        assert "init" in args
        assert project_name in args

        mock_retrieve_creds.assert_called_once()
        mock_create_cli_block.assert_called_once()

    assert (Path(dbtrepo_dir) / "packages.yml").exists()
    assert (Path(dbtrepo_dir) / "macros").exists()
    assets_dir = assets.__path__[0]

    for sql_file_path in glob.glob(os.path.join(assets_dir, "*.sql")):
        assert (Path(dbtrepo_dir) / "macros" / Path(sql_file_path).name).exists()

    orgdbt = OrgDbt.objects.filter(org=org).first()
    assert orgdbt is not None
    assert org.dbt == orgdbt


# ============= Tests for generate_manifest_json_for_dbt_project =============


@pytest.fixture
def sample_manifest():
    """Sample manifest.json structure for testing"""
    return {
        "metadata": {"project_name": "my_project"},
        "sources": {
            "source.my_project.source1.table1": {
                "unique_id": "source.my_project.source1.table1",
                "source_name": "source1",
                "name": "table1",
                "database": "test_db",
                "schema": "raw",
                "columns": {
                    "id": {"name": "id", "data_type": "integer"},
                    "name": {"name": "name", "data_type": "text"},
                },
            },
            "source.my_project.source1.table2": {
                "unique_id": "source.my_project.source1.table2",
                "source_name": "source1",
                "name": "table2",
                "database": "test_db",
                "schema": "raw",
                "columns": {
                    "user_id": {"name": "user_id", "data_type": "integer"},
                    "email": {"name": "email", "data_type": "text"},
                },
            },
        },
        "nodes": {
            "model.my_project.model1": {
                "unique_id": "model.my_project.model1",
                "resource_type": "model",
                "package_name": "my_project",
                "name": "model1",
                "database": "test_db",
                "schema": "analytics",
                "path": "models/model1.sql",
                "original_file_path": "models/model1.sql",
                "depends_on": {"nodes": ["source.my_project.source1.table1"]},
                "columns": {
                    "id": {"name": "id", "data_type": "integer"},
                    "name": {"name": "name", "data_type": "text"},
                    "created_at": {"name": "created_at", "data_type": "timestamp"},
                },
            },
            "model.my_project.model2": {
                "unique_id": "model.my_project.model2",
                "resource_type": "model",
                "package_name": "my_project",
                "name": "model2",
                "database": "test_db",
                "schema": "analytics",
                "path": "models/model2.sql",
                "original_file_path": "models/model2.sql",
                "depends_on": {
                    "nodes": ["model.my_project.model1", "source.my_project.source1.table2"]
                },
                "columns": {
                    "user_id": {"name": "user_id", "data_type": "integer"},
                    "full_name": {"name": "full_name", "data_type": "text"},
                    "email": {"name": "email", "data_type": "text"},
                },
            },
            # Include a package model that should be filtered out
            "model.elementary.elementary_test_results": {
                "unique_id": "model.elementary.elementary_test_results",
                "resource_type": "model",
                "package_name": "elementary",
                "name": "elementary_test_results",
                "database": "test_db",
                "schema": "elementary",
                "path": "models/elementary_test_results.sql",
            },
        },
    }


def test_generate_manifest_no_cli_profile_block(tmp_path):
    """Test that exception is raised when CLI profile block is missing"""
    org = Org.objects.create(name="test-org", slug="test-org")
    orgdbt = OrgDbt.objects.create(
        project_dir=str(tmp_path / "dbtrepo"),
        dbt_venv=str(tmp_path / "venv"),
        target_type="postgres",
        default_schema="public",
        cli_profile_block=None,  # No CLI profile block
    )

    try:
        # Mock gather_dbt_project_params to pass the initial check but then hit the CLI profile block check
        with patch(
            "ddpui.ddpdbt.dbt_service.DbtProjectManager.gather_dbt_project_params"
        ) as mock_gather:
            from ddpui.ddpdbt.schema import DbtProjectParams

            mock_gather.return_value = DbtProjectParams(
                dbt_binary=str(tmp_path / "venv/bin/dbt"),
                dbt_env_dir=str(tmp_path / "venv"),
                venv_binary=str(tmp_path / "venv/bin"),
                target="public",
                project_dir=str(tmp_path / "dbtrepo"),
                org_project_dir=str(tmp_path),
            )

            with pytest.raises(Exception) as exc_info:
                generate_manifest_json_for_dbt_project(org, orgdbt)

            assert "DBT CLI profile block not found" in str(exc_info.value)
    finally:
        orgdbt.delete()
        org.delete()


def test_generate_manifest_success(tmp_path):
    """Test successful manifest generation"""
    org = Org.objects.create(name="test-org", slug="test-org")

    # Create CLI profile block
    cli_block = OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=DBTCLIPROFILE,
        block_name="test-cli-block",
        block_id="test-block-id",
    )

    orgdbt = OrgDbt.objects.create(
        project_dir=str(tmp_path / "dbtrepo"),
        dbt_venv=str(tmp_path / "venv"),
        target_type="postgres",
        default_schema="public",
        cli_profile_block=cli_block,
    )

    try:
        # Mock the subprocess call to simulate successful dbt docs generation
        mock_manifest = {"metadata": {"project_name": "test_project"}}

        # Create the manifest file in tmp_path
        manifest_path = tmp_path / "dbtrepo" / "target" / "manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(mock_manifest))

        with patch(
            "ddpui.ddpdbt.dbt_service.DbtProjectManager.gather_dbt_project_params"
        ) as mock_gather, patch(
            "ddpui.ddpdbt.dbt_service.prefect_service.get_dbt_cli_profile_block"
        ) as mock_get_profile, patch(
            "ddpui.ddpdbt.dbt_service.DbtProjectManager.run_dbt_command"
        ) as mock_run_dbt:
            # Mock gather_dbt_project_params
            from ddpui.ddpdbt.schema import DbtProjectParams

            mock_gather.return_value = DbtProjectParams(
                dbt_binary=str(tmp_path / "venv/bin/dbt"),
                dbt_env_dir=str(tmp_path / "venv"),
                venv_binary=str(tmp_path / "venv/bin"),
                target="public",
                project_dir=str(tmp_path / "dbtrepo"),
                org_project_dir=str(tmp_path),
            )

            # Mock the profile block content
            mock_get_profile.return_value = {
                "profile": {
                    "test_profile": {
                        "outputs": {
                            "public": {"type": "postgres", "host": "localhost", "port": 5432}
                        }
                    }
                }
            }

            result = generate_manifest_json_for_dbt_project(org, orgdbt)

            # Verify dbt commands were called (deps and compile)
            assert mock_run_dbt.call_count == 2
            # First call should be deps
            deps_call = mock_run_dbt.call_args_list[0]
            assert "deps" in deps_call[1]["command"]
            # Second call should be compile
            compile_call = mock_run_dbt.call_args_list[1]
            assert "compile" in compile_call[1]["command"]

            # Verify manifest was read and returned
            assert result == mock_manifest

    finally:
        orgdbt.delete()
        cli_block.delete()
        org.delete()


def test_generate_manifest_error(tmp_path):
    """Test error handling when dbt docs generate fails"""
    org = Org.objects.create(name="test-org", slug="test-org")

    cli_block = OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=DBTCLIPROFILE,
        block_name="test-cli-block",
        block_id="test-block-id",
    )

    orgdbt = OrgDbt.objects.create(
        project_dir=str(tmp_path / "dbtrepo"),
        dbt_venv=str(tmp_path / "venv"),
        target_type="postgres",
        default_schema="public",
        cli_profile_block=cli_block,
    )

    try:
        with patch(
            "ddpui.ddpdbt.dbt_service.DbtProjectManager.gather_dbt_project_params"
        ) as mock_gather, patch(
            "ddpui.ddpdbt.dbt_service.prefect_service.get_dbt_cli_profile_block"
        ) as mock_get_profile, patch(
            "ddpui.ddpdbt.dbt_service.DbtProjectManager.run_dbt_command"
        ) as mock_run_dbt:
            # Mock gather_dbt_project_params
            from ddpui.ddpdbt.schema import DbtProjectParams

            mock_gather.return_value = DbtProjectParams(
                dbt_binary=str(tmp_path / "venv/bin/dbt"),
                dbt_env_dir=str(tmp_path / "venv"),
                venv_binary=str(tmp_path / "venv/bin"),
                target="public",
                project_dir=str(tmp_path / "dbtrepo"),
                org_project_dir=str(tmp_path),
            )

            # Mock the profile block content
            mock_get_profile.return_value = {
                "profile": {
                    "test_profile": {
                        "outputs": {
                            "public": {"type": "postgres", "host": "localhost", "port": 5432}
                        }
                    }
                }
            }

            # Mock dbt command to fail
            mock_run_dbt.side_effect = DbtCommandError("dbt deps failed", "command failed")

            with pytest.raises(Exception) as exc_info:
                generate_manifest_json_for_dbt_project(org, orgdbt)

            assert "Something went wrong while generating manifest.json" in str(exc_info.value)

    finally:
        orgdbt.delete()
        cli_block.delete()
        org.delete()


# ============= Tests for parse_dbt_manifest_to_canvas =============


def test_parse_dbt_manifest_to_canvas_success(sample_manifest):
    """Test successful parsing of manifest to canvas nodes"""
    org = Org.objects.create(name="test-org", slug="test-org")

    # Create OrgWarehouse for the function
    from ddpui.models.org import OrgWarehouse

    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres")

    # Create OrgDbt that will be used by CanvasNode
    orgdbt = OrgDbt.objects.create(target_type="postgres", default_schema="public")
    org.dbt = orgdbt
    org.save()

    try:
        # Mock warehouse connection to return column info
        mock_warehouse = Mock()
        mock_warehouse.get_table_columns.return_value = [
            {"name": "id", "data_type": "integer"},
            {"name": "name", "data_type": "text"},
            {"name": "extra_col", "data_type": "text"},
        ]

        with patch(
            "ddpui.ddpdbt.dbt_service.WarehouseFactory.connect", return_value=mock_warehouse
        ) as mock_connect, patch(
            "ddpui.ddpdbt.dbt_service.secretsmanager.retrieve_warehouse_credentials",
            return_value={"host": "localhost"},
        ) as mock_creds:
            result = parse_dbt_manifest_to_canvas(org, orgdbt, warehouse, sample_manifest)

            # Check the returned statistics
            assert "sources_processed" in result
            assert "models_processed" in result
            assert "edges_created" in result

            # Verify sources were created
            assert result["sources_processed"] == 2
            source_nodes = CanvasNode.objects.filter(orgdbt=orgdbt, node_type=CanvasNodeType.SOURCE)
            assert source_nodes.count() == 2

            # Verify models were created (excluding package models)
            assert result["models_processed"] == 2
            model_nodes = CanvasNode.objects.filter(orgdbt=orgdbt, node_type=CanvasNodeType.MODEL)
            assert model_nodes.count() == 2

            # Verify edges were created
            assert (
                result["edges_created"] == 3
            )  # model1 -> table1, model2 -> model1, model2 -> table2
            edges = CanvasEdge.objects.filter(from_node__orgdbt=orgdbt)
            assert edges.count() == 3

    finally:
        # Cleanup
        CanvasNode.objects.filter(orgdbt=orgdbt).delete()
        CanvasEdge.objects.filter(from_node__orgdbt=orgdbt).delete()
        orgdbt.delete()
        warehouse.delete()
        org.delete()


def test_parse_dbt_manifest_to_canvas_warehouse_columns(sample_manifest):
    """Test parsing with warehouse column fetching and fallback to manifest"""
    org = Org.objects.create(name="test-org", slug="test-org")

    # Create OrgWarehouse for the function
    from ddpui.models.org import OrgWarehouse

    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres")

    # Create OrgDbt that will be used by CanvasNode
    orgdbt = OrgDbt.objects.create(target_type="postgres", default_schema="public")
    org.dbt = orgdbt
    org.save()

    try:
        # Mock warehouse connection that fails for some tables
        mock_warehouse = Mock()

        def mock_get_columns(schema_name, table_name):
            if table_name == "table1":
                return [
                    {"name": "id", "data_type": "integer"},
                    {"name": "name", "data_type": "text"},
                    {"name": "warehouse_col", "data_type": "text"},
                ]
            else:
                # Return None to simulate failure, should fallback to manifest
                return None

        mock_warehouse.get_table_columns.side_effect = mock_get_columns

        with patch(
            "ddpui.ddpdbt.dbt_service.WarehouseFactory.connect", return_value=mock_warehouse
        ) as mock_connect, patch(
            "ddpui.ddpdbt.dbt_service.secretsmanager.retrieve_warehouse_credentials",
            return_value={"host": "localhost"},
        ) as mock_creds:
            result = parse_dbt_manifest_to_canvas(org, orgdbt, warehouse, sample_manifest)

            # Check the returned statistics
            assert "sources_processed" in result

            # Check that warehouse was called for both tables
            assert mock_warehouse.get_table_columns.call_count == 4  # 2 sources + 2 models

            # Verify source nodes were created with correct columns
            table1_node = CanvasNode.objects.get(orgdbt=orgdbt, name="source1.table1")
            table1_columns = table1_node.output_cols

            # table1 should have warehouse columns (3 columns)
            assert len(table1_columns) == 3
            assert "warehouse_col" in table1_columns

            # table2 should fallback to manifest columns (2 columns)
            table2_node = CanvasNode.objects.get(orgdbt=orgdbt, name="source1.table2")
            table2_columns = table2_node.output_cols
            assert len(table2_columns) == 2
            assert all(col in ["user_id", "email"] for col in table2_columns)

    finally:
        # Cleanup
        CanvasNode.objects.filter(orgdbt=orgdbt).delete()
        CanvasEdge.objects.filter(from_node__orgdbt=orgdbt).delete()
        orgdbt.delete()
        warehouse.delete()
        org.delete()


def test_parse_dbt_manifest_to_canvas_update_existing(sample_manifest):
    """Test updating existing canvas nodes when they already exist"""
    org = Org.objects.create(name="test-org", slug="test-org")

    # Create OrgWarehouse for the function
    from ddpui.models.org import OrgWarehouse

    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres")

    # Create OrgDbt that will be used by CanvasNode
    orgdbt = OrgDbt.objects.create(target_type="postgres", default_schema="public")
    org.dbt = orgdbt
    org.save()

    try:
        # Create existing OrgDbtModel that the function will find
        from ddpui.models.dbt_workflow import OrgDbtModel, OrgDbtModelType

        existing_orgdbt_model = OrgDbtModel.objects.create(
            orgdbt=orgdbt,
            name="table1",
            source_name="source1",
            type=OrgDbtModelType.SOURCE,
            display_name="source1.table1",
            schema="raw",
            output_cols=["old_col1", "old_col2"],
            under_construction=False,
        )

        # Create existing node that should be updated
        existing_node = CanvasNode.objects.create(
            orgdbt=orgdbt,
            name="source1.table1",
            node_type=CanvasNodeType.SOURCE,
            output_cols=["old_col1", "old_col2"],
            dbtmodel=existing_orgdbt_model,
        )

        mock_warehouse = Mock()
        mock_warehouse.get_table_columns.return_value = [
            {"name": "id", "data_type": "integer"},
            {"name": "name", "data_type": "text"},
        ]

        with patch(
            "ddpui.ddpdbt.dbt_service.WarehouseFactory.connect", return_value=mock_warehouse
        ) as mock_connect, patch(
            "ddpui.ddpdbt.dbt_service.secretsmanager.retrieve_warehouse_credentials",
            return_value={"host": "localhost"},
        ) as mock_creds:
            result = parse_dbt_manifest_to_canvas(org, orgdbt, warehouse, sample_manifest)

            # Check the returned statistics
            assert "sources_processed" in result

            # Verify existing node was updated, not duplicated
            source_nodes = CanvasNode.objects.filter(orgdbt=orgdbt, node_type=CanvasNodeType.SOURCE)
            assert source_nodes.count() == 2  # Still only 2 source nodes

            # Verify the existing node was updated
            updated_node = CanvasNode.objects.get(orgdbt=orgdbt, name="source1.table1")
            assert updated_node.id == existing_node.id  # Same node
            assert "id" in updated_node.output_cols  # New columns added
            assert "name" in updated_node.output_cols

    finally:
        # Cleanup
        CanvasNode.objects.filter(orgdbt=orgdbt).delete()
        CanvasEdge.objects.filter(from_node__orgdbt=orgdbt).delete()
        orgdbt.delete()
        warehouse.delete()
        org.delete()


def mock_open_manifest(manifest_data):
    """Helper function to mock file opening for manifest.json"""
    mock_file = MagicMock()
    mock_file.read.return_value = json.dumps(manifest_data)
    mock_file.__enter__ = Mock(return_value=mock_file)
    mock_file.__exit__ = Mock(return_value=None)
    return Mock(return_value=mock_file)
