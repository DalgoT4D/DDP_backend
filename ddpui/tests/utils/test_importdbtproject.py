"""Tests for ddpui/utils/importdbtproject.py"""

from unittest.mock import Mock, patch


from ddpui.utils.importdbtproject import (
    ModelMetadata,
    SourceMetaData,
    extract_models,
    extract_sources,
)


class TestModelMetadata:
    """Test ModelMetadata schema"""

    def test_model_metadata_creation(self):
        """Test ModelMetadata schema creation with valid data"""
        metadata = ModelMetadata(
            name="test_model",
            dbschema="test_schema",
            original_file_path="models/test_model.sql",
            resource_type="model",
            source_name="test_package",
            depends_on=["source.test.table1"],
            columns=["col1", "col2"],
        )
        assert metadata.name == "test_model"
        assert metadata.dbschema == "test_schema"
        assert metadata.original_file_path == "models/test_model.sql"
        assert metadata.resource_type == "model"
        assert metadata.source_name == "test_package"
        assert metadata.depends_on == ["source.test.table1"]
        assert metadata.columns == ["col1", "col2"]

    def test_model_metadata_with_none_values(self):
        """Test ModelMetadata schema with None values"""
        metadata = ModelMetadata(
            name=None,
            dbschema=None,
            original_file_path=None,
            resource_type=None,
            source_name=None,
            depends_on=None,
            columns=None,
        )
        assert metadata.name is None
        assert metadata.dbschema is None
        assert metadata.original_file_path is None
        assert metadata.resource_type is None
        assert metadata.source_name is None
        assert metadata.depends_on is None
        assert metadata.columns is None


class TestSourceMetaData:
    """Test SourceMetaData schema"""

    def test_source_metadata_creation(self):
        """Test SourceMetaData schema creation with valid data"""
        metadata = SourceMetaData(
            name="test_source",
            identifier="test_table",
            dbschema="test_schema",
            database="test_db",
            resource_type="source",
            package_name="test_package",
            source_name="test_source_name",
            path="models/sources.yml",
        )
        assert metadata.name == "test_source"
        assert metadata.identifier == "test_table"
        assert metadata.dbschema == "test_schema"
        assert metadata.database == "test_db"
        assert metadata.resource_type == "source"
        assert metadata.package_name == "test_package"
        assert metadata.source_name == "test_source_name"
        assert metadata.path == "models/sources.yml"

    def test_source_metadata_with_none_values(self):
        """Test SourceMetaData schema with None values"""
        metadata = SourceMetaData(
            name=None,
            identifier=None,
            dbschema=None,
            database=None,
            resource_type=None,
            package_name=None,
            source_name=None,
            path=None,
        )
        assert metadata.name is None
        assert metadata.identifier is None
        assert metadata.dbschema is None
        assert metadata.database is None
        assert metadata.resource_type is None
        assert metadata.package_name is None
        assert metadata.source_name is None
        assert metadata.path is None


class TestExtractModels:
    """Test extract_models function"""

    def test_extract_models_valid_manifest(self):
        """Test extract_models with valid manifest data"""
        manifest = {
            "nodes": {
                "model.test_package.test_model": {
                    "name": "test_model",
                    "schema": "test_schema",
                    "original_file_path": "models/test_model.sql",
                    "resource_type": "model",
                    "package_name": "test_package",
                    "depends_on": {"nodes": ["source.test.table1"]},
                    "columns": {"col1": {}, "col2": {}},
                },
                "model.test_package.another_model": {
                    "name": "another_model",
                    "schema": "test_schema",
                    "original_file_path": "models/another_model.sql",
                    "resource_type": "model",
                    "package_name": "test_package",
                    "depends_on": {"nodes": ["model.test_package.test_model"]},
                    "columns": {"col3": {}, "col4": {}},
                },
                "test.test_package.test_test": {
                    "name": "test_test",
                    "resource_type": "test",
                },
            }
        }

        result = extract_models(manifest)

        assert len(result) == 2
        assert "model.test_package.test_model" in result
        assert "model.test_package.another_model" in result
        assert "test.test_package.test_test" not in result

        test_model = result["model.test_package.test_model"]
        assert test_model.name == "test_model"
        assert test_model.dbschema == "test_schema"
        assert test_model.original_file_path == "models/test_model.sql"
        assert test_model.resource_type == "model"
        assert test_model.source_name == "test_package"
        assert test_model.depends_on == ["source.test.table1"]
        assert test_model.columns == ["col1", "col2"]

    def test_extract_models_empty_manifest(self):
        """Test extract_models with empty manifest"""
        manifest = {"nodes": {}}
        result = extract_models(manifest)
        assert result == {}

    def test_extract_models_no_model_nodes(self):
        """Test extract_models with manifest containing no model nodes"""
        manifest = {
            "nodes": {
                "test.test_package.test_test": {
                    "name": "test_test",
                    "resource_type": "test",
                },
                "source.test_package.test_source": {
                    "name": "test_source",
                    "resource_type": "source",
                },
            }
        }
        result = extract_models(manifest)
        assert result == {}

    def test_extract_models_missing_depends_on(self):
        """Test extract_models with model missing depends_on"""
        manifest = {
            "nodes": {
                "model.test_package.test_model": {
                    "name": "test_model",
                    "schema": "test_schema",
                    "original_file_path": "models/test_model.sql",
                    "resource_type": "model",
                    "package_name": "test_package",
                    "columns": {"col1": {}, "col2": {}},
                }
            }
        }

        result = extract_models(manifest)
        test_model = result["model.test_package.test_model"]
        assert test_model.depends_on == []

    def test_extract_models_missing_columns(self):
        """Test extract_models with model missing columns"""
        manifest = {
            "nodes": {
                "model.test_package.test_model": {
                    "name": "test_model",
                    "schema": "test_schema",
                    "original_file_path": "models/test_model.sql",
                    "resource_type": "model",
                    "package_name": "test_package",
                    "depends_on": {"nodes": ["source.test.table1"]},
                }
            }
        }

        result = extract_models(manifest)
        test_model = result["model.test_package.test_model"]
        assert test_model.columns == []


class TestExtractSources:
    """Test extract_sources function"""

    def test_extract_sources_valid_manifest(self):
        """Test extract_sources with valid manifest data"""
        manifest = {
            "sources": {
                "source.test_package.test_source.table1": {
                    "name": "table1",
                    "identifier": "table1",
                    "schema": "raw_data",
                    "database": "test_db",
                    "resource_type": "source",
                    "package_name": "test_package",
                    "source_name": "test_source",
                    "path": "models/sources.yml",
                },
                "source.test_package.test_source.table2": {
                    "name": "table2",
                    "identifier": "table2_renamed",
                    "schema": "raw_data",
                    "database": "test_db",
                    "resource_type": "source",
                    "package_name": "test_package",
                    "source_name": "test_source",
                    "path": "models/sources.yml",
                },
            }
        }

        result = extract_sources(manifest)

        assert len(result) == 2
        assert "source.test_package.test_source.table1" in result
        assert "source.test_package.test_source.table2" in result

        table1 = result["source.test_package.test_source.table1"]
        assert table1.name == "table1"
        assert table1.identifier == "table1"
        assert table1.dbschema == "raw_data"
        assert table1.database == "test_db"
        assert table1.resource_type == "source"
        assert table1.package_name == "test_package"
        assert table1.source_name == "test_source"
        assert table1.path == "models/sources.yml"

    def test_extract_sources_empty_manifest(self):
        """Test extract_sources with empty manifest"""
        manifest = {"sources": {}}
        result = extract_sources(manifest)
        assert result == {}

    def test_extract_sources_missing_fields(self):
        """Test extract_sources with sources missing some fields"""
        manifest = {
            "sources": {
                "source.test_package.test_source.table1": {
                    "name": "table1",
                    "resource_type": "source",
                    # Missing other fields
                }
            }
        }

        result = extract_sources(manifest)
        table1 = result["source.test_package.test_source.table1"]
        assert table1.name == "table1"
        assert table1.identifier is None
        assert table1.dbschema is None
        assert table1.database is None
        assert table1.resource_type == "source"
        assert table1.package_name is None
        assert table1.source_name is None
        assert table1.path is None


# Tests for Django model creation functions - these require mocking
class TestCreateOrgDbtModelModel:
    """Test create_orgdbtmodel_model function"""

    @patch("ddpui.utils.importdbtproject.OrgDbtModel")
    @patch("ddpui.utils.importdbtproject.uuid")
    def test_create_orgdbtmodel_model_success(self, mock_uuid, mock_orgdbtmodel):
        """Test successful creation of OrgDbtModel from model metadata"""
        from ddpui.utils.importdbtproject import create_orgdbtmodel_model

        # Setup mocks
        mock_uuid.uuid4.return_value = "test-uuid"
        mock_orgdbt = Mock()
        mock_orgdbtmodel.objects.filter.return_value.exists.return_value = False

        model_metadata = ModelMetadata(
            name="test_model",
            dbschema="test_schema",
            original_file_path="models/test_model.sql",
            resource_type="model",
            source_name="test_package",
            depends_on=["source.test.table1"],
            columns=["col1", "col2"],
        )

        create_orgdbtmodel_model(mock_orgdbt, model_metadata)

        # Verify filter was called to check existence
        mock_orgdbtmodel.objects.filter.assert_called_once_with(
            orgdbt=mock_orgdbt, schema="test_schema", name="test_model"
        )

        # Verify create was called
        mock_orgdbtmodel.objects.create.assert_called_once_with(
            uuid="test-uuid",
            orgdbt=mock_orgdbt,
            schema="test_schema",
            name="test_model",
            sql_path="models/test_model.sql",
            type="model",
            source_name="test_schema",
            output_cols=["col1", "col2"],
        )

    @patch("ddpui.utils.importdbtproject.OrgDbtModel")
    def test_create_orgdbtmodel_model_already_exists(self, mock_orgdbtmodel):
        """Test that model is not created if it already exists"""
        from ddpui.utils.importdbtproject import create_orgdbtmodel_model

        mock_orgdbt = Mock()
        mock_orgdbtmodel.objects.filter.return_value.exists.return_value = True

        model_metadata = ModelMetadata(
            name="test_model",
            dbschema="test_schema",
            original_file_path="models/test_model.sql",
            resource_type="model",
            source_name="test_package",
            depends_on=["source.test.table1"],
            columns=["col1", "col2"],
        )

        create_orgdbtmodel_model(mock_orgdbt, model_metadata)

        # Verify filter was called but create was not
        mock_orgdbtmodel.objects.filter.assert_called_once()
        mock_orgdbtmodel.objects.create.assert_not_called()

    @patch("ddpui.utils.importdbtproject.OrgDbtModel")
    def test_create_orgdbtmodel_model_missing_required_fields(self, mock_orgdbtmodel):
        """Test that model is not created if required fields are missing"""
        from ddpui.utils.importdbtproject import create_orgdbtmodel_model

        mock_orgdbt = Mock()

        # Test with missing name
        model_metadata = ModelMetadata(
            name=None,
            dbschema="test_schema",
            original_file_path="models/test_model.sql",
            resource_type="model",
            source_name="test_package",
            depends_on=["source.test.table1"],
            columns=["col1", "col2"],
        )

        create_orgdbtmodel_model(mock_orgdbt, model_metadata)
        mock_orgdbtmodel.objects.filter.assert_not_called()
        mock_orgdbtmodel.objects.create.assert_not_called()


class TestCreateOrgDbtModelSource:
    """Test create_orgdbtmodel_source function"""

    @patch("ddpui.utils.importdbtproject.OrgDbtModel")
    @patch("ddpui.utils.importdbtproject.uuid")
    def test_create_orgdbtmodel_source_success(self, mock_uuid, mock_orgdbtmodel):
        """Test successful creation of OrgDbtModel from source metadata"""
        from ddpui.utils.importdbtproject import create_orgdbtmodel_source

        # Setup mocks
        mock_uuid.uuid4.return_value = "test-uuid"
        mock_orgdbt = Mock()
        mock_orgdbtmodel.objects.filter.return_value.exists.return_value = False

        source_metadata = SourceMetaData(
            name="test_source",
            identifier="test_table",
            dbschema="raw_data",
            database="test_db",
            resource_type="source",
            package_name="test_package",
            source_name="test_source_name",
            path="models/sources.yml",
        )

        create_orgdbtmodel_source(mock_orgdbt, source_metadata)

        # Verify filter was called to check existence
        mock_orgdbtmodel.objects.filter.assert_called_once_with(
            orgdbt=mock_orgdbt, schema="raw_data", name="test_table"
        )

        # Verify create was called
        mock_orgdbtmodel.objects.create.assert_called_once_with(
            uuid="test-uuid",
            orgdbt=mock_orgdbt,
            schema="raw_data",
            name="test_table",
            display_name="test_source",
            type="source",
            source_name="test_source_name",
            sql_path="models/sources.yml",
        )


class TestCreateOrgDbtEdges:
    """Test create_orgdbtedges function"""

    @patch("ddpui.utils.importdbtproject.DbtEdge")
    @patch("ddpui.utils.importdbtproject.OrgDbtModel")
    def test_create_orgdbtedges_source_dependency(self, mock_orgdbtmodel, mock_dbtedge):
        """Test creating edges for source dependencies"""
        from ddpui.utils.importdbtproject import create_orgdbtedges

        mock_orgdbt = Mock()
        mock_child_model = Mock()
        mock_parent_model = Mock()

        # Setup model queries
        mock_orgdbtmodel.objects.get.side_effect = [mock_child_model, mock_parent_model]

        model_metadata = ModelMetadata(
            name="test_model",
            dbschema="test_schema",
            original_file_path="models/test_model.sql",
            resource_type="model",
            source_name="test_package",
            depends_on=["source.test_package.test_source.table1"],
            columns=["col1", "col2"],
        )

        create_orgdbtedges(mock_orgdbt, model_metadata)

        # Verify model queries
        assert mock_orgdbtmodel.objects.get.call_count == 2
        mock_orgdbtmodel.objects.get.assert_any_call(
            orgdbt=mock_orgdbt, schema="test_schema", name="test_model"
        )
        mock_orgdbtmodel.objects.get.assert_any_call(display_name="table1", type="source")

        # Verify edge creation
        mock_dbtedge.objects.create.assert_called_once_with(
            from_node=mock_parent_model, to_node=mock_child_model
        )


class TestCreateOrgDbtOperation:
    """Test create_orgdbtoperation function"""

    @patch("ddpui.utils.importdbtproject.OrgDbtOperation")
    @patch("ddpui.utils.importdbtproject.OrgDbtModel")
    @patch("ddpui.utils.importdbtproject.uuid")
    @patch("builtins.print")
    def test_create_orgdbtoperation_source_dependency(
        self, mock_print, mock_uuid, mock_orgdbtmodel, mock_orgdbtoperation
    ):
        """Test creating operation with source dependency"""
        from ddpui.utils.importdbtproject import create_orgdbtoperation

        mock_uuid.uuid4.return_value = "test-uuid"
        mock_orgdbt = Mock()
        mock_child_model = Mock()
        mock_parent_model = Mock()
        mock_parent_model.uuid = "parent-uuid"
        mock_parent_model.name = "table1"
        mock_parent_model.source_name = "test_source"
        mock_parent_model.schema = "raw_data"
        mock_parent_model.type = "source"

        # Setup model queries
        mock_orgdbtmodel.objects.get.side_effect = [mock_child_model, mock_parent_model]
        mock_orgdbtoperation.objects.filter.return_value.exists.return_value = False

        model_metadata = ModelMetadata(
            name="test_model",
            dbschema="test_schema",
            original_file_path="models/test_model.sql",
            resource_type="model",
            source_name="test_package",
            depends_on=["source.test_package.test_source.table1"],
            columns=["col1", "col2"],
        )

        create_orgdbtoperation(mock_orgdbt, model_metadata)

        # Verify operation creation
        mock_orgdbtoperation.objects.create.assert_called_once()
        call_args = mock_orgdbtoperation.objects.create.call_args[1]
        assert call_args["dbtmodel"] == mock_child_model
        assert call_args["uuid"] == "test-uuid"
        assert call_args["seq"] == 1
        assert call_args["output_cols"] == ["col1", "col2"]
        assert "config" in call_args
        assert call_args["config"]["input_models"][0]["uuid"] == "parent-uuid"


# Additional integration test class for when Django is available


class TestDjangoIntegration:
    """Integration tests requiring Django models"""

    def test_import_all_functions(self):
        """Test that all functions can be imported when Django is available"""
        from ddpui.utils.importdbtproject import (
            create_orgdbtmodel_model,
            create_orgdbtmodel_source,
            create_orgdbtedges,
            create_orgdbtoperation,
        )

        # Just verify they can be imported
        assert callable(create_orgdbtmodel_model)
        assert callable(create_orgdbtmodel_source)
        assert callable(create_orgdbtedges)
        assert callable(create_orgdbtoperation)
