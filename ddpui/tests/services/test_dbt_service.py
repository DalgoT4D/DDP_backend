import yaml
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
    cleanup_unused_sources,
)
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.models.dbt_workflow import OrgDbtModel, OrgDbtModelType
from ddpui.ddpdbt.schema import DbtProjectParams
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


# ============= Tests for generate_manifest_json_for_dbt_project =============


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
    cli_profile_block = OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=DBTCLIPROFILE,
        block_name="test-cli-block",
        block_id="test-block-id",
    )
    dbt.cli_profile_block = cli_profile_block
    dbt.save()
    yield org
    print("deleting org_with_dbt_workspace")
    org.delete()


def test_generate_manifest_no_cli_profile_block(org_with_dbt_workspace: Org):
    """Test that exception is raised when CLI profile block is missing"""

    org_with_dbt_workspace.dbt.cli_profile_block = None

    with pytest.raises(Exception) as exc_info:
        generate_manifest_json_for_dbt_project(org_with_dbt_workspace, org_with_dbt_workspace.dbt)

    assert "DBT CLI profile block not found" in str(exc_info.value)


def test_generate_manifest_success(org_with_dbt_workspace: Org):
    """Test successful manifest generation"""
    # Create CLI profile block

    # Mock the subprocess call to simulate successful dbt docs generation
    mock_manifest = {"metadata": {"project_name": "test_project"}}

    # Create the manifest file in tmp_path
    dbtrepo_dir = DbtProjectManager.get_dbt_project_dir(org_with_dbt_workspace.dbt)
    manifest_path = Path(dbtrepo_dir) / "target" / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(mock_manifest))

    with patch(
        "ddpui.ddpdbt.dbt_service.prefect_service.get_dbt_cli_profile_block"
    ) as mock_get_profile, patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.run_dbt_command"
    ) as mock_run_dbt:
        # Mock the profile block content
        mock_get_profile.return_value = {
            "profile": {
                "test_profile": {
                    "outputs": {"public": {"type": "postgres", "host": "localhost", "port": 5432}}
                }
            }
        }

        result = generate_manifest_json_for_dbt_project(
            org_with_dbt_workspace, org_with_dbt_workspace.dbt
        )

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


def test_generate_manifest_error(org_with_dbt_workspace: Org):
    """Test error handling when dbt docs generate fails"""

    with patch(
        "ddpui.ddpdbt.dbt_service.prefect_service.get_dbt_cli_profile_block"
    ) as mock_get_profile, patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.run_dbt_command"
    ) as mock_run_dbt:
        # Mock the profile block content
        mock_get_profile.return_value = {
            "profile": {
                "test_profile": {
                    "outputs": {"public": {"type": "postgres", "host": "localhost", "port": 5432}}
                }
            }
        }

        # Mock dbt command to fail
        mock_run_dbt.side_effect = DbtCommandError("dbt deps failed", "command failed")

        with pytest.raises(Exception) as exc_info:
            generate_manifest_json_for_dbt_project(
                org_with_dbt_workspace, org_with_dbt_workspace.dbt
            )

        assert "Something went wrong while generating manifest.json" in str(exc_info.value)


# ============= Tests for parse_dbt_manifest_to_canvas =============
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


def test_parse_dbt_manifest_to_canvas_success(sample_manifest, org_with_dbt_workspace: Org):
    """Test successful parsing of manifest to canvas nodes"""

    warehouse = OrgWarehouse.objects.create(org=org_with_dbt_workspace, wtype="postgres")
    orgdbt = org_with_dbt_workspace.dbt

    # Mock warehouse connection to return column info
    mock_warehouse = Mock()
    mock_warehouse.get_table_columns.return_value = [
        {"name": "id", "data_type": "integer"},
        {"name": "name", "data_type": "text"},
        {"name": "extra_col", "data_type": "text"},
    ]

    with patch(
        "ddpui.ddpdbt.dbt_service.prefect_service.get_dbt_cli_profile_block"
    ) as mock_get_profile, patch(
        "ddpui.ddpdbt.dbt_service.WarehouseFactory.connect", return_value=mock_warehouse
    ) as mock_connect, patch(
        "ddpui.ddpdbt.dbt_service.secretsmanager.retrieve_warehouse_credentials",
        return_value={"host": "localhost"},
    ) as mock_creds, patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.run_dbt_command"
    ) as mock_run_dbt:
        # Mock the profile block content
        mock_get_profile.return_value = {
            "profile": {
                "test_profile": {
                    "outputs": {"public": {"type": "postgres", "host": "localhost", "port": 5432}}
                }
            }
        }

        result = parse_dbt_manifest_to_canvas(
            org_with_dbt_workspace, orgdbt, warehouse, sample_manifest, refresh=False
        )

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
        assert result["edges_created"] == 3  # model1 -> table1, model2 -> model1, model2 -> table2
        edges = CanvasEdge.objects.filter(from_node__orgdbt=orgdbt)
        assert edges.count() == 3


def test_parse_dbt_manifest_to_canvas_warehouse_columns(
    sample_manifest, org_with_dbt_workspace: Org
):
    """Test parsing with warehouse column fetching and fallback to manifest"""

    warehouse = OrgWarehouse.objects.create(org=org_with_dbt_workspace, wtype="postgres")
    orgdbt = org_with_dbt_workspace.dbt

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
        result = parse_dbt_manifest_to_canvas(
            org_with_dbt_workspace, orgdbt, warehouse, sample_manifest, refresh=False
        )

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


def test_parse_dbt_manifest_to_canvas_update_existing(org_with_dbt_workspace: Org, sample_manifest):
    """Test updating existing canvas nodes when they already exist"""

    warehouse = OrgWarehouse.objects.create(org=org_with_dbt_workspace, wtype="postgres")
    orgdbt = org_with_dbt_workspace.dbt

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
        result = parse_dbt_manifest_to_canvas(
            org_with_dbt_workspace, orgdbt, warehouse, sample_manifest, refresh=False
        )

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


# ============= Fixtures for Operation Chain Tests =============


@pytest.fixture
def operation_chain_graph(org_with_dbt_workspace: Org):
    """
    Creates a canvas graph with operation chain: model1 -> op1 -> op2 -> model2
    Returns dict with all nodes for easy access in tests.
    """
    warehouse = OrgWarehouse.objects.create(org=org_with_dbt_workspace, wtype="postgres")
    orgdbt = org_with_dbt_workspace.dbt

    # Create OrgDbtModel instances for model nodes (required for MODEL/SOURCE types)
    model1_dbtmodel = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="model1",
        type=OrgDbtModelType.MODEL,
        display_name="model1",
        schema="analytics",
        output_cols=["id", "name"],
        under_construction=False,
    )

    model2_dbtmodel = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="model2",
        type=OrgDbtModelType.MODEL,
        display_name="model2",
        schema="analytics",
        output_cols=["id", "final_name", "created_at"],
        under_construction=False,
    )

    # Create canvas nodes with proper OrgDbtModel references
    model1_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        name="model1",
        node_type=CanvasNodeType.MODEL,
        output_cols=["id", "name"],
        dbtmodel=model1_dbtmodel,
    )

    op1_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        name="op1",
        node_type=CanvasNodeType.OPERATION,
        output_cols=["id", "processed_name"],
        # Operations don't need dbtmodel
    )

    op2_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        name="op2",
        node_type=CanvasNodeType.OPERATION,
        output_cols=["id", "final_name"],
        # Operations don't need dbtmodel
    )

    model2_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        name="model2",
        node_type=CanvasNodeType.MODEL,
        output_cols=["id", "final_name", "created_at"],
        dbtmodel=model2_dbtmodel,
    )

    # Create the operation chain edges: model1 -> op1 -> op2 -> model2
    edge1 = CanvasEdge.objects.create(from_node=model1_node, to_node=op1_node)
    edge2 = CanvasEdge.objects.create(from_node=op1_node, to_node=op2_node)
    edge3 = CanvasEdge.objects.create(from_node=op2_node, to_node=model2_node)

    return {
        "warehouse": warehouse,
        "orgdbt": orgdbt,
        "model1_node": model1_node,
        "op1_node": op1_node,
        "op2_node": op2_node,
        "model2_node": model2_node,
        "model1_dbtmodel": model1_dbtmodel,
        "model2_dbtmodel": model2_dbtmodel,
        "edges": [edge1, edge2, edge3],
    }


@pytest.fixture
def direct_edge_with_operation_chain_graph(org_with_dbt_workspace: Org):
    """
    Creates a canvas graph with BOTH direct edge AND operation chain:
    - Direct: model1 -> model2
    - Chain: model1 -> op1 -> op2 -> model2
    Used to test that direct edge gets deleted when operation chain exists.
    """
    warehouse = OrgWarehouse.objects.create(org=org_with_dbt_workspace, wtype="postgres")
    orgdbt = org_with_dbt_workspace.dbt

    # Create OrgDbtModel instances
    model1_dbtmodel = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="model1",
        type=OrgDbtModelType.MODEL,
        display_name="model1",
        schema="analytics",
        output_cols=["id", "name"],
        under_construction=False,
    )

    model2_dbtmodel = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="model2",
        type=OrgDbtModelType.MODEL,
        display_name="model2",
        schema="analytics",
        output_cols=["id", "processed_name"],
        under_construction=False,
    )

    # Create canvas nodes
    model1_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        name="model1",
        node_type=CanvasNodeType.MODEL,
        output_cols=["id", "name"],
        dbtmodel=model1_dbtmodel,
    )

    model2_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        name="model2",
        node_type=CanvasNodeType.MODEL,
        output_cols=["id", "processed_name"],
        dbtmodel=model2_dbtmodel,
    )

    # Create DIRECT edge first (this should be deleted later)
    direct_edge = CanvasEdge.objects.create(from_node=model1_node, to_node=model2_node)

    # Create operation nodes
    op1_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        name="op1",
        node_type=CanvasNodeType.OPERATION,
        output_cols=["id", "temp_name"],
    )

    op2_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        name="op2",
        node_type=CanvasNodeType.OPERATION,
        output_cols=["id", "processed_name"],
    )

    # Create the operation chain edges
    chain_edge1 = CanvasEdge.objects.create(from_node=model1_node, to_node=op1_node)
    chain_edge2 = CanvasEdge.objects.create(from_node=op1_node, to_node=op2_node)
    chain_edge3 = CanvasEdge.objects.create(from_node=op2_node, to_node=model2_node)

    return {
        "warehouse": warehouse,
        "orgdbt": orgdbt,
        "model1_node": model1_node,
        "model2_node": model2_node,
        "op1_node": op1_node,
        "op2_node": op2_node,
        "model1_dbtmodel": model1_dbtmodel,
        "model2_dbtmodel": model2_dbtmodel,
        "direct_edge": direct_edge,
        "chain_edges": [chain_edge1, chain_edge2, chain_edge3],
    }


# ============= Operation Chain Tests =============


def test_parse_dbt_manifest_preserves_operation_chains(operation_chain_graph):
    """
    Test that existing operation chains are preserved and direct edges are not created.

    Scenario: model1 -> op1 -> op2 -> model2
    The function should NOT create a direct edge from model1 -> model2
    when an operation chain already exists.
    """
    # Extract nodes and objects from fixture
    warehouse = operation_chain_graph["warehouse"]
    orgdbt = operation_chain_graph["orgdbt"]
    model1_node = operation_chain_graph["model1_node"]
    model2_node = operation_chain_graph["model2_node"]
    op1_node = operation_chain_graph["op1_node"]
    op2_node = operation_chain_graph["op2_node"]

    # Verify initial state - should have 3 edges for the operation chain
    initial_edge_count = CanvasEdge.objects.filter(from_node__orgdbt=orgdbt).count()
    assert initial_edge_count == 3

    # Create manifest with model2 depending on model1 (this would normally create a direct edge)
    manifest_with_operation_chain = {
        "metadata": {"project_name": "test_project"},
        "sources": {},
        "nodes": {
            "model.test_project.model1": {
                "unique_id": "model.test_project.model1",
                "resource_type": "model",
                "package_name": "test_project",
                "name": "model1",
                "database": "test_db",
                "schema": "analytics",
                "path": "models/model1.sql",
                "depends_on": {"nodes": []},  # No dependencies
                "columns": {
                    "id": {"name": "id", "data_type": "integer"},
                    "name": {"name": "name", "data_type": "text"},
                },
            },
            "model.test_project.model2": {
                "unique_id": "model.test_project.model2",
                "resource_type": "model",
                "package_name": "test_project",
                "name": "model2",
                "database": "test_db",
                "schema": "analytics",
                "path": "models/model2.sql",
                "depends_on": {"nodes": ["model.test_project.model1"]},  # Depends on model1
                "columns": {
                    "id": {"name": "id", "data_type": "integer"},
                    "final_name": {"name": "final_name", "data_type": "text"},
                    "created_at": {"name": "created_at", "data_type": "timestamp"},
                },
            },
        },
    }

    # Mock warehouse connection
    mock_warehouse = Mock()
    mock_warehouse.get_table_columns.return_value = None  # Use manifest columns

    with patch(
        "ddpui.ddpdbt.dbt_service.WarehouseFactory.connect", return_value=mock_warehouse
    ), patch(
        "ddpui.ddpdbt.dbt_service.secretsmanager.retrieve_warehouse_credentials",
        return_value={"host": "localhost"},
    ):
        # Parse the manifest - this should detect the operation chain and NOT create direct edge
        result = parse_dbt_manifest_to_canvas(
            org_with_dbt_workspace, orgdbt, warehouse, manifest_with_operation_chain, refresh=False
        )

        # Verify that no additional edges were created
        final_edge_count = CanvasEdge.objects.filter(from_node__orgdbt=orgdbt).count()
        assert (
            final_edge_count == 3
        ), "Operation chain should be preserved, no direct edge should be added"

        # Verify the operation chain still exists intact
        assert CanvasEdge.objects.filter(from_node=model1_node, to_node=op1_node).exists()
        assert CanvasEdge.objects.filter(from_node=op1_node, to_node=op2_node).exists()
        assert CanvasEdge.objects.filter(from_node=op2_node, to_node=model2_node).exists()

        # Verify NO direct edge was created from model1 to model2
        direct_edge_exists = CanvasEdge.objects.filter(
            from_node=model1_node, to_node=model2_node
        ).exists()
        assert (
            not direct_edge_exists
        ), "Direct edge should NOT exist when operation chain is present"

        # Verify nodes were updated with manifest data (but chain preserved)
        updated_model1 = CanvasNode.objects.get(orgdbt=orgdbt, name="model1")
        updated_model2 = CanvasNode.objects.get(orgdbt=orgdbt, name="model2")

        # Model columns should be updated from manifest
        assert "id" in updated_model1.output_cols
        assert "name" in updated_model1.output_cols
        assert "id" in updated_model2.output_cols
        assert "final_name" in updated_model2.output_cols
        assert "created_at" in updated_model2.output_cols


def test_parse_dbt_manifest_deletes_existing_direct_edge_when_operation_chain_exists(
    direct_edge_with_operation_chain_graph,
):
    """
    Test that existing direct edges are DELETED when operation chains are detected.

    Scenario:
    1. Initial state: model1 -> model2 (direct edge)
    2. Add operation chain: model1 -> op1 -> op2 -> model2
    3. Parse manifest: should delete direct edge and preserve operation chain
    """
    # Extract from fixture
    warehouse = direct_edge_with_operation_chain_graph["warehouse"]
    orgdbt = direct_edge_with_operation_chain_graph["orgdbt"]
    model1_node = direct_edge_with_operation_chain_graph["model1_node"]
    model2_node = direct_edge_with_operation_chain_graph["model2_node"]
    op1_node = direct_edge_with_operation_chain_graph["op1_node"]
    op2_node = direct_edge_with_operation_chain_graph["op2_node"]

    # Verify initial state - should have 4 edges (1 direct + 3 operation chain)
    initial_edge_count = CanvasEdge.objects.filter(from_node__orgdbt=orgdbt).count()
    assert initial_edge_count == 4

    # Verify direct edge exists initially
    assert CanvasEdge.objects.filter(from_node=model1_node, to_node=model2_node).exists()

    # Create manifest that would create the direct dependency
    manifest = {
        "metadata": {"project_name": "test_project"},
        "sources": {},
        "nodes": {
            "model.test_project.model1": {
                "unique_id": "model.test_project.model1",
                "resource_type": "model",
                "package_name": "test_project",
                "name": "model1",
                "database": "test_db",
                "schema": "analytics",
                "path": "models/model1.sql",
                "depends_on": {"nodes": []},
                "columns": {
                    "id": {"name": "id", "data_type": "integer"},
                    "name": {"name": "name", "data_type": "text"},
                },
            },
            "model.test_project.model2": {
                "unique_id": "model.test_project.model2",
                "resource_type": "model",
                "package_name": "test_project",
                "name": "model2",
                "database": "test_db",
                "schema": "analytics",
                "path": "models/model2.sql",
                "depends_on": {"nodes": ["model.test_project.model1"]},  # This dependency exists
                "columns": {
                    "id": {"name": "id", "data_type": "integer"},
                    "processed_name": {"name": "processed_name", "data_type": "text"},
                },
            },
        },
    }

    # Mock warehouse connection
    mock_warehouse = Mock()
    mock_warehouse.get_table_columns.return_value = None

    with patch(
        "ddpui.ddpdbt.dbt_service.WarehouseFactory.connect", return_value=mock_warehouse
    ), patch(
        "ddpui.ddpdbt.dbt_service.secretsmanager.retrieve_warehouse_credentials",
        return_value={"host": "localhost"},
    ):
        # Parse manifest - should detect operation chain and DELETE direct edge
        result = parse_dbt_manifest_to_canvas(
            org_with_dbt_workspace, orgdbt, warehouse, manifest, refresh=False
        )

        # Verify direct edge was DELETED (operation chain detection removes it)
        direct_edge_exists = CanvasEdge.objects.filter(
            from_node=model1_node, to_node=model2_node
        ).exists()
        assert not direct_edge_exists, "Direct edge should be DELETED when operation chain exists"

        # Verify operation chain is intact
        assert CanvasEdge.objects.filter(from_node=model1_node, to_node=op1_node).exists()
        assert CanvasEdge.objects.filter(from_node=op1_node, to_node=op2_node).exists()
        assert CanvasEdge.objects.filter(from_node=op2_node, to_node=model2_node).exists()

        # Final edge count should be 3 (operation chain only, direct edge deleted)
        final_edge_count = CanvasEdge.objects.filter(from_node__orgdbt=orgdbt).count()
        assert final_edge_count == 3, "Should only have operation chain edges, direct edge deleted"


def test_cleanup_unused_sources_with_manifest_provided():
    """Test cleanup_unused_sources function with manifest provided"""
    # Create test organization and dbt setup
    org = Org.objects.create(name="test-cleanup-org", slug="test-cleanup-org")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres")
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/test/repo",
        project_dir="/tmp/test",
        default_schema="public",
    )
    org.dbt = orgdbt
    org.save()

    # Create OrgDbtModel instances for sources
    used_source_model = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="used_table",
        type=OrgDbtModelType.SOURCE,
        display_name="used_table",
        schema="raw_data",
        source_name="test_source",
        output_cols=["id", "name"],
    )

    unused_source_model = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="unused_table",
        type=OrgDbtModelType.SOURCE,
        display_name="unused_table",
        schema="raw_data",
        source_name="test_source",
        output_cols=["id", "data"],
    )

    # Create CanvasNodes
    used_canvas_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        name="test_source.used_table",
        node_type=CanvasNodeType.SOURCE,
        output_cols=["id", "name"],
        dbtmodel=used_source_model,
    )

    unused_canvas_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        name="test_source.unused_table",
        node_type=CanvasNodeType.SOURCE,
        output_cols=["id", "data"],
        dbtmodel=unused_source_model,
    )

    # Create a model node that depends on the used source
    model_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        name="my_model",
        node_type=CanvasNodeType.MODEL,
        output_cols=["id", "name", "processed"],
    )

    # Create edge from used source to model (this should prevent used_canvas_node from being deleted)
    CanvasEdge.objects.create(from_node=used_canvas_node, to_node=model_node)

    # Mock manifest with used and unused sources
    mock_manifest = {
        "sources": {
            "source.test_project.test_source.used_table": {
                "source_name": "test_source",
                "name": "used_table",
                "schema": "raw_data",
            },
            "source.test_project.test_source.unused_table": {
                "source_name": "test_source",
                "name": "unused_table",
                "schema": "raw_data",
            },
        },
        "nodes": {
            "model.test_project.my_model": {
                "resource_type": "model",
                "depends_on": {"nodes": ["source.test_project.test_source.used_table"]},
            }
        },
        "child_map": {},
    }

    with patch("ddpui.core.dbtautomation_service.delete_dbt_source_in_project") as mock_delete:
        result = cleanup_unused_sources(org, orgdbt, manifest_json=mock_manifest)

    # Verify results
    assert len(result["sources_removed"]) == 1
    assert "raw_data.unused_table" in result["sources_removed"]
    assert len(result["sources_with_edges_skipped"]) == 0
    assert len(result["errors"]) == 0

    # Verify unused canvas node was deleted
    assert not CanvasNode.objects.filter(uuid=unused_canvas_node.uuid).exists()

    # Verify used canvas node still exists
    assert CanvasNode.objects.filter(uuid=used_canvas_node.uuid).exists()

    # Verify delete_dbt_source_in_project was called once
    mock_delete.assert_called_once_with(unused_source_model)


def test_cleanup_unused_sources_with_edges_skipped():
    """Test cleanup_unused_sources function when sources have canvas edges"""
    # Create test organization and dbt setup
    org = Org.objects.create(name="test-cleanup-edges", slug="test-cleanup-edges")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres")
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/test/repo",
        project_dir="/tmp/test",
        default_schema="public",
    )
    org.dbt = orgdbt
    org.save()

    # Create OrgDbtModel for unused source with edges
    unused_source_model = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="source_with_edges",
        type=OrgDbtModelType.SOURCE,
        display_name="source_with_edges",
        schema="raw_data",
        source_name="test_source",
        output_cols=["id", "data"],
    )

    # Create OrgDbtModel for target model
    target_model = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="target_model",
        type=OrgDbtModelType.MODEL,
        display_name="target_model",
        schema="analytics",
        output_cols=["id", "processed_data"],
    )

    # Create CanvasNodes
    source_canvas_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        name="test_source.source_with_edges",
        node_type=CanvasNodeType.SOURCE,
        output_cols=["id", "data"],
        dbtmodel=unused_source_model,
    )

    target_canvas_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        name="target_model",
        node_type=CanvasNodeType.MODEL,
        output_cols=["id", "processed_data"],
        dbtmodel=target_model,
    )

    # Create canvas edge (this should prevent deletion)
    CanvasEdge.objects.create(
        from_node=source_canvas_node,
        to_node=target_canvas_node,
    )

    # Mock manifest with unused source (not referenced by any model in manifest)
    mock_manifest = {
        "sources": {
            "source.test_project.test_source.source_with_edges": {
                "source_name": "test_source",
                "name": "source_with_edges",
                "schema": "raw_data",
            }
        },
        "nodes": {
            "model.test_project.other_model": {
                "resource_type": "model",
                "depends_on": {"nodes": []},  # No dependencies on our source
            }
        },
        "child_map": {},
    }

    with patch("ddpui.core.dbtautomation_service.delete_dbt_source_in_project") as mock_delete:
        result = cleanup_unused_sources(org, orgdbt, manifest_json=mock_manifest)

    # Verify results - source should be skipped due to edges
    assert len(result["sources_removed"]) == 0
    assert len(result["sources_with_edges_skipped"]) == 1
    assert "raw_data.source_with_edges" in result["sources_with_edges_skipped"]
    assert len(result["errors"]) == 0

    # Verify canvas node still exists (not deleted due to edges)
    assert CanvasNode.objects.filter(uuid=source_canvas_node.uuid).exists()

    # Verify delete_dbt_source_in_project was NOT called
    mock_delete.assert_not_called()


def test_cleanup_unused_sources_no_canvas_node():
    """Test cleanup_unused_sources when OrgDbtModel exists but no CanvasNode"""
    # Create test organization and dbt setup
    org = Org.objects.create(name="test-no-canvas", slug="test-no-canvas")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres")
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/test/repo",
        project_dir="/tmp/test",
        default_schema="public",
    )
    org.dbt = orgdbt
    org.save()

    # Create OrgDbtModel for unused source (but no CanvasNode)
    unused_source_model = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="orphan_table",
        type=OrgDbtModelType.SOURCE,
        display_name="orphan_table",
        schema="raw_data",
        source_name="test_source",
        output_cols=["id", "data"],
    )

    # Mock manifest with unused source
    mock_manifest = {
        "sources": {
            "source.test_project.test_source.orphan_table": {
                "source_name": "test_source",
                "name": "orphan_table",
                "schema": "raw_data",
            }
        },
        "nodes": {},  # No models using this source
        "child_map": {},
    }

    with patch("ddpui.core.dbtautomation_service.delete_dbt_source_in_project") as mock_delete:
        result = cleanup_unused_sources(org, orgdbt, manifest_json=mock_manifest)

    # Verify results - source should be removed even without CanvasNode
    assert len(result["sources_removed"]) == 1
    assert "raw_data.orphan_table" in result["sources_removed"]
    assert len(result["sources_with_edges_skipped"]) == 0
    assert len(result["errors"]) == 0

    # Verify delete_dbt_source_in_project was called
    mock_delete.assert_called_once_with(unused_source_model)


def test_cleanup_unused_sources_generate_manifest():
    """Test cleanup_unused_sources function when manifest_json is None (should generate)"""
    # Create test organization and dbt setup
    org = Org.objects.create(name="test-gen-manifest", slug="test-gen-manifest")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres")
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/test/repo",
        project_dir="/tmp/test",
        default_schema="public",
    )
    org.dbt = orgdbt
    org.save()

    # Mock generated manifest
    mock_manifest = {"sources": {}, "nodes": {}, "child_map": {}}

    with patch("ddpui.ddpdbt.dbt_service.generate_manifest_json_for_dbt_project") as mock_generate:
        mock_generate.return_value = mock_manifest

        result = cleanup_unused_sources(org, orgdbt)  # manifest_json=None

    # Verify generate_manifest_json_for_dbt_project was called
    mock_generate.assert_called_once_with(org, orgdbt)

    # Verify results (empty manifest means no sources to clean)
    assert len(result["sources_removed"]) == 0
    assert len(result["sources_with_edges_skipped"]) == 0
    assert len(result["errors"]) == 0


def test_cleanup_unused_sources_child_map_dependencies():
    """Test cleanup_unused_sources function with child_map dependencies"""
    # Create test organization and dbt setup
    org = Org.objects.create(name="test-child-map", slug="test-child-map")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres")
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/test/repo",
        project_dir="/tmp/test",
        default_schema="public",
    )
    org.dbt = orgdbt
    org.save()

    # Create OrgDbtModel for source used via child_map
    used_source_model = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="indirect_used_table",
        type=OrgDbtModelType.SOURCE,
        display_name="indirect_used_table",
        schema="raw_data",
        source_name="test_source",
        output_cols=["id", "name"],
    )

    # Create CanvasNode
    used_canvas_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        name="test_source.indirect_used_table",
        node_type=CanvasNodeType.SOURCE,
        output_cols=["id", "name"],
        dbtmodel=used_source_model,
    )

    # Create a model node that depends on the source via child_map
    model_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        name="my_model",
        node_type=CanvasNodeType.MODEL,
        output_cols=["id", "name", "processed"],
    )

    # Create edge from used source to model (this should prevent used_canvas_node from being deleted)
    CanvasEdge.objects.create(from_node=used_canvas_node, to_node=model_node)

    # Mock manifest where source is used via child_map (indirect dependency)
    mock_manifest = {
        "sources": {
            "source.test_project.test_source.indirect_used_table": {
                "source_name": "test_source",
                "name": "indirect_used_table",
                "schema": "raw_data",
            }
        },
        "nodes": {
            "model.test_project.my_model": {
                "resource_type": "model",
                "depends_on": {"nodes": []},  # No direct dependencies
            }
        },
        "child_map": {
            "source.test_project.test_source.indirect_used_table": [
                "model.test_project.my_model"  # Indirect dependency via child_map
            ]
        },
    }

    with patch("ddpui.core.dbtautomation_service.delete_dbt_source_in_project") as mock_delete:
        result = cleanup_unused_sources(org, orgdbt, manifest_json=mock_manifest)

    # Verify results - source should NOT be removed due to child_map dependency
    assert len(result["sources_removed"]) == 0
    assert len(result["sources_with_edges_skipped"]) == 0
    assert len(result["errors"]) == 0

    # Verify canvas node still exists
    assert CanvasNode.objects.filter(uuid=used_canvas_node.uuid).exists()

    # Verify delete_dbt_source_in_project was NOT called
    mock_delete.assert_not_called()


def test_cleanup_unused_sources_error_handling():
    """Test cleanup_unused_sources function error handling"""
    # Create test organization and dbt setup
    org = Org.objects.create(name="test-errors", slug="test-errors")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres")
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/test/repo",
        project_dir="/tmp/test",
        default_schema="public",
    )
    org.dbt = orgdbt
    org.save()

    # Mock manifest generation failure
    with patch("ddpui.ddpdbt.dbt_service.generate_manifest_json_for_dbt_project") as mock_generate:
        mock_generate.side_effect = Exception("Manifest generation failed")

        result = cleanup_unused_sources(org, orgdbt)  # manifest_json=None

    # Verify error was captured
    assert len(result["sources_removed"]) == 0
    assert len(result["sources_with_edges_skipped"]) == 0
    assert len(result["errors"]) == 1
    assert "Manifest generation failed" in result["errors"][0]


def test_cleanup_unused_sources_canvas_only_cleanup():
    """Test cleanup of canvas source nodes that have no edges and aren't in manifest"""
    # Create test organization and dbt setup
    org = Org.objects.create(name="test-canvas-cleanup", slug="test-canvas-cleanup")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres")
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/test/repo",
        project_dir="/tmp/test",
        default_schema="public",
    )
    org.dbt = orgdbt
    org.save()

    # Create OrgDbtModel for orphaned source
    orphaned_source_model = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="orphaned_table",
        type=OrgDbtModelType.SOURCE,
        display_name="orphaned_table",
        schema="raw_data",
        source_name="test_source",
        output_cols=["id", "data"],
    )

    # Create CanvasNode for orphaned source (not in manifest, no edges)
    orphaned_canvas_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        name="test_source.orphaned_table",
        node_type=CanvasNodeType.SOURCE,
        output_cols=["id", "data"],
        dbtmodel=orphaned_source_model,
    )

    # Create CanvasNode without dbtmodel (should also be cleaned up)
    no_model_canvas_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        name="no_model_source",
        node_type=CanvasNodeType.SOURCE,
        output_cols=[],
        dbtmodel=None,
    )

    # Mock manifest with no sources (so orphaned source won't be found in manifest)
    mock_manifest = {"sources": {}, "nodes": {}, "child_map": {}}

    # Mock the delete function
    with patch("ddpui.core.dbtautomation_service.delete_dbt_source_in_project") as mock_delete:
        result = cleanup_unused_sources(org, orgdbt, mock_manifest)

    # Should find and remove the orphaned canvas node
    assert "raw_data.orphaned_table" in result["sources_removed"]
    assert len(result["sources_with_edges_skipped"]) == 0
    assert len(result["errors"]) == 0

    # Verify cleanup happened
    mock_delete.assert_called_once_with(orphaned_source_model)

    # Verify nodes were deleted
    with pytest.raises(CanvasNode.DoesNotExist):
        CanvasNode.objects.get(id=orphaned_canvas_node.id)
    with pytest.raises(CanvasNode.DoesNotExist):
        CanvasNode.objects.get(id=no_model_canvas_node.id)
