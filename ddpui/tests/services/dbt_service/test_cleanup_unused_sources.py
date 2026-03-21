"""Tests for cleanup_unused_sources function."""

import pytest
from unittest.mock import patch
from ddpui.ddpdbt.dbt_service import cleanup_unused_sources
from ddpui.models.org import Org, OrgWarehouse, OrgDbt
from ddpui.models.dbt_workflow import OrgDbtModel, OrgDbtModelType
from ddpui.models.canvas_models import CanvasNode, CanvasNodeType, CanvasEdge


@pytest.mark.django_db
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


@pytest.mark.django_db
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


@pytest.mark.django_db
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


@pytest.mark.django_db
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


@pytest.mark.django_db
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


@pytest.mark.django_db
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


@pytest.mark.django_db
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
