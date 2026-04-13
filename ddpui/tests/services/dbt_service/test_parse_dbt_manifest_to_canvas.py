"""Tests for parse_dbt_manifest_to_canvas function."""

import pytest
from unittest.mock import patch, Mock
from ddpui.ddpdbt.dbt_service import parse_dbt_manifest_to_canvas
from ddpui.models.org import Org, OrgDbt, OrgWarehouse
from ddpui.models.dbt_workflow import OrgDbtModel, OrgDbtModelType
from ddpui.models.canvas_models import CanvasNode, CanvasNodeType, CanvasEdge


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


@pytest.fixture
def org_with_dbt_workspace():
    """Fixture for organization with dbt workspace"""
    org = Org.objects.create(name="test-org", slug="test-org")
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/test/repo",
        project_dir="/tmp/test-proj",
        default_schema="public",
    )
    org.dbt = orgdbt
    org.save()
    return org


@pytest.mark.django_db
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


@pytest.mark.django_db
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


@pytest.mark.django_db
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
        "org": org_with_dbt_workspace,
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
        "org": org_with_dbt_workspace,
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


@pytest.mark.django_db
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

    # Get org from fixture
    org_with_dbt_workspace = operation_chain_graph["org"]

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


@pytest.mark.django_db
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

    # Get org from fixture
    org_with_dbt_workspace = direct_edge_with_operation_chain_graph["org"]

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
