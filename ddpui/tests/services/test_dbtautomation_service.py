import os, uuid
from unittest.mock import patch, ANY, Mock
import django
import pytest


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


from ddpui.models.org import Org, OrgDbt
from ddpui.models.dbt_workflow import OrgDbtModel, OrgDbtOperation, OrgDbtModelType
from ddpui.models.canvas_models import CanvasNode, CanvasEdge, CanvasNodeType
from ddpui.core.git_manager import GitChangedFile
from ddpui.core.dbtautomation_service import (
    delete_org_dbt_model,
    delete_org_dbt_source,
    delete_dbt_source_in_project,
    convert_canvas_node_to_frontend_format,
)

pytestmark = pytest.mark.django_db


@pytest.fixture
def org_without_workspace():
    """a pytest fixture which creates an Org without an airbyte workspace"""
    org = Org.objects.create(
        airbyte_workspace_id=None, slug="test-org-WO-slug", name="test-org-WO-name"
    )
    yield org
    org.delete()


@pytest.fixture
def orgdbt():
    """a pytest fixture which creates an Org with a dbt model"""
    dbt = OrgDbt.objects.create(
        gitrepo_url="dummy-git-url.github.com",
        project_dir="tmp/",
        dbt_venv="/dbt/venv",
        target_type="postgres",
        default_schema="prod",
    )
    yield dbt


@pytest.fixture
def orgdbt_model(orgdbt):
    """a pytest fixture which creates an Org with a dbt model"""
    orgdbt_model = OrgDbtModel.objects.create(
        uuid=uuid.uuid4(),
        orgdbt=orgdbt,
        type=OrgDbtModelType.MODEL,
        name="test-model",
        schema="staging",
        sql_path="src/path",
    )

    for seq, op_type in zip(range(1, 3), ["drop", "rename"]):
        OrgDbtOperation.objects.create(
            dbtmodel=orgdbt_model,
            uuid=uuid.uuid4(),
            seq=seq,
            output_cols=[],
            config={
                "type": op_type,
            },
        )

    yield orgdbt_model


@pytest.fixture
def orgdbt_source(orgdbt):
    """a pytest fixture which creates an Org with a dbt source"""
    orgdbt_source = OrgDbtModel.objects.create(
        uuid=uuid.uuid4(),
        orgdbt=orgdbt,
        type=OrgDbtModelType.SOURCE,
        name="test-src",
        display_name="test-src",
        schema="staging",
        sql_path="src/path",
    )

    for seq, op_type in zip(range(1, 3), ["drop", "rename"]):
        OrgDbtOperation.objects.create(
            dbtmodel=orgdbt_source,
            uuid=uuid.uuid4(),
            seq=seq,
            output_cols=[],
            config={
                "type": op_type,
            },
        )

    yield orgdbt_source


def test_delete_org_dbt_model_of_wrong_type(orgdbt_model: OrgDbtModel):
    """Test that the function raises an error if the orgdbt_model is of the wrong type"""
    orgdbt_model.type = OrgDbtModelType.SOURCE
    with pytest.raises(ValueError, match="Cannot delete a source as a model"):
        delete_org_dbt_model(orgdbt_model, cascade=False)


@patch("ddpui.core.dbtautomation_service.delete_dbt_model_in_project")
def test_delete_org_dbt_model_operations_chained(
    mock_delete_dbt_model_in_project: Mock, orgdbt_model: OrgDbtModel
):
    """Delete org dbt model with operations chained & pointing to it"""
    delete_org_dbt_model(orgdbt_model, cascade=False)

    assert orgdbt_model.under_construction is True
    mock_delete_dbt_model_in_project.assert_called_once_with(orgdbt_model)


@patch("ddpui.core.dbtautomation_service.delete_dbt_model_in_project")
def test_delete_org_dbt_model_with_dangling_model_node(
    mock_delete_dbt_model_in_project: Mock, orgdbt_model: OrgDbtModel
):
    """Delete org dbt model with nothing pointing in & out of the model"""
    uuid = orgdbt_model.uuid
    OrgDbtOperation.objects.filter(dbtmodel=orgdbt_model).delete()

    delete_org_dbt_model(orgdbt_model, cascade=False)

    mock_delete_dbt_model_in_project.assert_called_once_with(orgdbt_model)
    assert OrgDbtModel.objects.filter(uuid=uuid).count() == 0


def test_delete_org_dbt_source_of_wrong_type(orgdbt_source: OrgDbtModel):
    """Test that the function raises an error if the orgdbt_model is of the wrong type"""
    orgdbt_source.type = OrgDbtModelType.MODEL
    with pytest.raises(ValueError, match="Cannot delete a model as a source"):
        delete_org_dbt_source(orgdbt_source, cascade=False)


@patch("ddpui.core.dbtautomation_service.delete_dbt_source_in_project")
def test_delete_org_dbt_source_success(
    mock_delete_dbt_source_in_project: Mock, orgdbt_source: OrgDbtModel
):
    delete_org_dbt_source(orgdbt_source, cascade=False)

    assert OrgDbtModel.objects.filter(uuid=orgdbt_source.uuid).count() == 0
    mock_delete_dbt_source_in_project.assert_called_once_with(orgdbt_source)


@patch("ddpui.core.dbtautomation_service.generate_source_definitions_yaml")
@patch("ddpui.core.dbtautomation_service.read_sources_from_yaml")
@patch("ddpui.core.dbtautomation_service.DbtProjectManager.get_dbt_project_dir")
@patch("ddpui.core.dbtautomation_service.Path")
def test_delete_dbt_source_in_project_success(
    mock_path: Mock,
    mock_get_dbt_project_dir: Mock,
    mock_read_sources_from_yaml: Mock,
    mock_generate_source_definitions_yaml: Mock,
    orgdbt_source: OrgDbtModel,
):
    """Test that the function deletes the dbt source in the project"""
    # Setup mocks to simulate file exists
    mock_get_dbt_project_dir.return_value = "/fake/dbt/project"
    mock_path_instance = Mock()
    mock_path_instance.exists.return_value = True  # File exists
    mock_path.return_value.__truediv__.return_value = mock_path_instance

    schema_srcs = [
        {
            "source_name": src_name,
            "input_name": src_name,  # table
            "input_type": "source",
            "schema": orgdbt_source.schema,
            "sql_path": orgdbt_source.sql_path,
        }
        for src_name in [orgdbt_source.name, "src-1", "src-2"]
    ]
    mock_read_sources_from_yaml.return_value = schema_srcs

    delete_dbt_source_in_project(orgdbt_source)

    mock_read_sources_from_yaml.assert_called_once()
    mock_generate_source_definitions_yaml.assert_called_once_with(
        orgdbt_source.schema,
        orgdbt_source.source_name,
        [src["input_name"] for src in schema_srcs if src["source_name"] != orgdbt_source.name],
        ANY,  # dbtProject object
        rel_dir_to_models=ANY,  # Path(...).parent.relative_to("models")
    )


# Tests for convert_canvas_node_to_frontend_format


def test_convert_canvas_node_source_type_no_changed_files(orgdbt):
    """Test converting SOURCE type canvas node without changed files"""
    # Create a SOURCE type canvas node
    source_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.SOURCE,
        name="users_source",
        uuid=uuid.uuid4(),
        output_cols=["id", "name", "email"],
        # operation_config defaults to {}
    )

    result = convert_canvas_node_to_frontend_format(source_node)

    # Verify basic structure
    assert result["uuid"] == str(source_node.uuid)
    assert result["node_type"] == CanvasNodeType.SOURCE
    assert result["name"] == "users_source"
    assert (
        result["operation_config"] == {} or result["operation_config"] is None
    )  # Default is empty dict
    assert result["output_columns"] == ["id", "name", "email"]
    assert result["dbtmodel"] is None
    assert result["is_last_in_chain"] is True  # Sources are always last in chain
    assert result["isPublished"] is None  # Non-model nodes have null isPublished


def test_convert_canvas_node_operation_type_last_in_chain(orgdbt):
    """Test converting OPERATION type canvas node that is last in chain"""
    # Create an OPERATION type canvas node
    operation_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.OPERATION,
        name="filter_operation",
        uuid=uuid.uuid4(),
        operation_config={"type": "where", "config": {"clauses": []}},
        output_cols=["id", "name"],
    )

    # No outgoing edges to other operations, so it should be last in chain
    result = convert_canvas_node_to_frontend_format(operation_node)

    assert result["uuid"] == str(operation_node.uuid)
    assert result["node_type"] == CanvasNodeType.OPERATION
    assert result["name"] == "filter_operation"
    assert result["operation_config"] == {"type": "where", "config": {"clauses": []}}
    assert result["output_columns"] == ["id", "name"]
    assert result["dbtmodel"] is None
    assert result["is_last_in_chain"] is True  # No outgoing operation edges
    assert result["isPublished"] is None  # Non-model nodes have null isPublished


def test_convert_canvas_node_operation_type_not_last_in_chain(orgdbt):
    """Test converting OPERATION type canvas node that is NOT last in chain"""
    # Create two operation nodes
    operation_node_1 = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.OPERATION,
        name="filter_operation",
        uuid=uuid.uuid4(),
        operation_config={"type": "where", "config": {"clauses": []}},
        output_cols=["id", "name"],
    )

    operation_node_2 = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.OPERATION,
        name="rename_operation",
        uuid=uuid.uuid4(),
        operation_config={"type": "renamecolumns", "config": {"columns": {}}},
        output_cols=["id", "full_name"],
    )

    # Create edge from operation_node_1 to operation_node_2
    CanvasEdge.objects.create(from_node=operation_node_1, to_node=operation_node_2, seq=1)

    result = convert_canvas_node_to_frontend_format(operation_node_1)

    assert result["uuid"] == str(operation_node_1.uuid)
    assert result["node_type"] == CanvasNodeType.OPERATION
    assert result["name"] == "filter_operation"
    assert result["is_last_in_chain"] is False  # Has outgoing edge to another operation


def test_convert_canvas_node_model_type_with_dbtmodel_no_changed_files(orgdbt):
    """Test converting MODEL type canvas node with dbtmodel but no changed files"""
    # Create a dbt model
    dbt_model = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="user_model",
        display_name="User Model",
        schema="analytics",
        type=OrgDbtModelType.MODEL,
        sql_path="models/user_model.sql",
        source_name=None,
        output_cols=["id", "name"],
        uuid=uuid.uuid4(),
    )

    # Create MODEL type canvas node with dbtmodel
    model_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.MODEL,
        name="user_model",
        uuid=uuid.uuid4(),
        output_cols=["id", "name"],
        dbtmodel=dbt_model,
        # operation_config defaults to {}  # Explicitly set operation_config for MODEL
    )

    result = convert_canvas_node_to_frontend_format(model_node)

    assert result["uuid"] == str(model_node.uuid)
    assert result["node_type"] == CanvasNodeType.MODEL
    assert result["name"] == "user_model"
    assert (
        result["operation_config"] == {} or result["operation_config"] is None
    )  # Default is empty dict
    assert result["output_columns"] == ["id", "name"]
    assert result["is_last_in_chain"] is True  # Models are always last in chain
    assert result["isPublished"] is None  # No changed_files provided

    # Check dbtmodel structure
    assert result["dbtmodel"] is not None
    assert result["dbtmodel"]["schema"] == "analytics"
    assert result["dbtmodel"]["sql_path"] == "models/user_model.sql"
    assert result["dbtmodel"]["name"] == "user_model"
    assert result["dbtmodel"]["display_name"] == "User Model"
    assert result["dbtmodel"]["uuid"] == str(dbt_model.uuid)
    assert result["dbtmodel"]["type"] == OrgDbtModelType.MODEL
    assert result["dbtmodel"]["source_name"] is None
    assert result["dbtmodel"]["output_cols"] == ["id", "name"]


def test_convert_canvas_node_model_type_with_changed_files_published(orgdbt):
    """Test MODEL node with changed files - file not in changed list (published)"""
    # Create a dbt model
    dbt_model = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="user_model",
        display_name="User Model",
        schema="analytics",
        type=OrgDbtModelType.MODEL,
        sql_path="models/user_model.sql",
        output_cols=["id", "name"],
        uuid=uuid.uuid4(),
    )

    # Create MODEL type canvas node
    model_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.MODEL,
        name="user_model",
        uuid=uuid.uuid4(),
        output_cols=["id", "name"],
        dbtmodel=dbt_model,
        # operation_config defaults to {}
    )

    # Create changed files list that doesn't include this model
    changed_files = [
        GitChangedFile(filename="models/other_model.sql", status="modified"),
        GitChangedFile(filename="macros/helper.sql", status="added"),
    ]

    result = convert_canvas_node_to_frontend_format(model_node, changed_files)

    assert result["isPublished"] is True  # File not in changed list, so published


def test_convert_canvas_node_model_type_with_changed_files_unpublished_exact_match(orgdbt):
    """Test MODEL node with changed files - exact file match (unpublished)"""
    # Create a dbt model
    dbt_model = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="user_model",
        display_name="User Model",
        schema="analytics",
        type=OrgDbtModelType.MODEL,
        sql_path="models/user_model.sql",
        output_cols=["id", "name"],
        uuid=uuid.uuid4(),
    )

    # Create MODEL type canvas node
    model_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.MODEL,
        name="user_model",
        uuid=uuid.uuid4(),
        output_cols=["id", "name"],
        dbtmodel=dbt_model,
        # operation_config defaults to {}
    )

    # Create changed files list that includes this model's exact path
    changed_files = [
        GitChangedFile(filename="models/user_model.sql", status="modified"),  # Exact match
        GitChangedFile(filename="models/other_model.sql", status="added"),
    ]

    result = convert_canvas_node_to_frontend_format(model_node, changed_files)

    assert result["isPublished"] is False  # Exact file match, so unpublished


def test_convert_canvas_node_model_type_with_changed_files_unpublished_parent_directory(orgdbt):
    """Test MODEL node with changed files - parent directory match (unpublished)"""
    # Create a dbt model
    dbt_model = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="user_model",
        display_name="User Model",
        schema="analytics",
        type=OrgDbtModelType.MODEL,
        sql_path="models/staging/user_model.sql",
        output_cols=["id", "name"],
        uuid=uuid.uuid4(),
    )

    # Create MODEL type canvas node
    model_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.MODEL,
        name="user_model",
        uuid=uuid.uuid4(),
        output_cols=["id", "name"],
        dbtmodel=dbt_model,
        # operation_config defaults to {}
    )

    # Create changed files list with parent directory
    changed_files = [
        GitChangedFile(filename="models/staging/", status="added"),  # Parent directory ends with /
        GitChangedFile(filename="models/other_model.sql", status="modified"),
    ]

    result = convert_canvas_node_to_frontend_format(model_node, changed_files)

    assert result["isPublished"] is False  # File under changed parent directory, so unpublished


def test_convert_canvas_node_model_type_with_changed_files_unpublished_directory_prefix(orgdbt):
    """Test MODEL node with changed files - directory prefix match (unpublished)"""
    # Create a dbt model
    dbt_model = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name="user_model",
        display_name="User Model",
        schema="analytics",
        type=OrgDbtModelType.MODEL,
        sql_path="models/staging/users/user_model.sql",
        output_cols=["id", "name"],
        uuid=uuid.uuid4(),
    )

    # Create MODEL type canvas node
    model_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.MODEL,
        name="user_model",
        uuid=uuid.uuid4(),
        output_cols=["id", "name"],
        dbtmodel=dbt_model,
        # operation_config defaults to {}
    )

    # Create changed files list with directory prefix
    changed_files = [
        GitChangedFile(
            filename="models/staging/users", status="added"
        ),  # Directory prefix (without /)
        GitChangedFile(filename="models/other_model.sql", status="modified"),
    ]

    result = convert_canvas_node_to_frontend_format(model_node, changed_files)

    assert result["isPublished"] is False  # File under changed directory, so unpublished


def test_convert_canvas_node_model_type_without_dbtmodel(orgdbt):
    """Test converting MODEL type canvas node without associated dbtmodel"""
    # Create MODEL type canvas node without dbtmodel
    model_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.MODEL,
        name="temp_model",
        uuid=uuid.uuid4(),
        output_cols=["id", "data"],
        dbtmodel=None,
        # operation_config defaults to {}
    )

    result = convert_canvas_node_to_frontend_format(model_node)

    assert result["uuid"] == str(model_node.uuid)
    assert result["node_type"] == CanvasNodeType.MODEL
    assert result["name"] == "temp_model"
    assert result["dbtmodel"] is None
    assert result["is_last_in_chain"] is True  # Models are always last in chain
    assert result["isPublished"] is None  # No dbtmodel, so null isPublished


def test_convert_canvas_node_model_type_with_changed_files_but_no_dbtmodel(orgdbt):
    """Test MODEL node with changed files but no dbtmodel - should have null isPublished"""
    # Create MODEL type canvas node without dbtmodel
    model_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.MODEL,
        name="temp_model",
        uuid=uuid.uuid4(),
        output_cols=["id", "data"],
        dbtmodel=None,
        # operation_config defaults to {}
    )

    changed_files = [GitChangedFile(filename="models/some_model.sql", status="modified")]

    result = convert_canvas_node_to_frontend_format(model_node, changed_files)

    assert result["isPublished"] is None  # No dbtmodel, so null even with changed_files


def test_convert_canvas_node_comprehensive_structure_validation(orgdbt):
    """Test that all expected fields are present in the output"""
    # Create a comprehensive canvas node
    operation_node = CanvasNode.objects.create(
        orgdbt=orgdbt,
        node_type=CanvasNodeType.OPERATION,
        name="comprehensive_operation",
        uuid=uuid.uuid4(),
        operation_config={
            "type": "aggregate",
            "config": {
                "aggregate_on": [
                    {"column": "amount", "operation": "sum", "output_column_name": "total_amount"}
                ]
            },
        },
        output_cols=["user_id", "total_amount"],
    )

    result = convert_canvas_node_to_frontend_format(operation_node)

    # Verify all expected fields are present
    expected_fields = [
        "uuid",
        "node_type",
        "name",
        "operation_config",
        "output_columns",
        "dbtmodel",
        "is_last_in_chain",
        "isPublished",
    ]

    for field in expected_fields:
        assert field in result, f"Expected field '{field}' not found in result"

    # Verify types and values
    assert isinstance(result["uuid"], str)
    assert result["node_type"] == CanvasNodeType.OPERATION
    assert isinstance(result["name"], str)
    assert isinstance(result["operation_config"], dict)
    assert isinstance(result["output_columns"], list)
    assert result["dbtmodel"] is None
    assert isinstance(result["is_last_in_chain"], bool)
    assert result["isPublished"] is None
