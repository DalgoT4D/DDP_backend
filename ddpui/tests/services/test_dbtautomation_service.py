import os, uuid
from unittest.mock import patch, ANY, Mock
import django
import pytest


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


from ddpui.models.org import Org, OrgDbt
from ddpui.models.dbt_workflow import OrgDbtModel, OrgDbtOperation, OrgDbtModelType
from ddpui.core.dbtautomation_service import (
    delete_org_dbt_model,
    delete_org_dbt_source,
    delete_dbt_source_in_project,
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
    """a pytest fixture which creates an Org with a dbt model"""
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


@patch("ddpui.core.dbtautomation_service.cascade_delete_org_dbt_model")
@patch("ddpui.core.dbtautomation_service.delete_dbt_model_in_project")
def test_delete_org_dbt_model_operations_chained_cascade(
    mock_delete_dbt_model_in_project: Mock,
    mock_cascade_delete_org_dbt_model: Mock,
    orgdbt_model: OrgDbtModel,
):
    """Delete org dbt model with operations chained & pointing to it. Do a cascade delete"""
    delete_org_dbt_model(orgdbt_model, cascade=True)

    assert orgdbt_model.under_construction is True
    mock_cascade_delete_org_dbt_model.assert_called_once_with(orgdbt_model)
    mock_delete_dbt_model_in_project.assert_not_called()


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


@patch("ddpui.core.dbtautomation_service.cascade_delete_org_dbt_model")
@patch("ddpui.core.dbtautomation_service.delete_dbt_source_in_project")
def test_delete_org_dbt_source_cascade_success(
    mock_delete_dbt_source_in_project: Mock,
    mock_cascade_delete_org_dbt_model: Mock,
    orgdbt_source: OrgDbtModel,
):
    delete_org_dbt_source(orgdbt_source, cascade=True)

    mock_cascade_delete_org_dbt_model.assert_called_once_with(orgdbt_source)
    mock_delete_dbt_source_in_project.assert_called_once_with(orgdbt_source)


@patch("ddpui.core.dbtautomation_service.generate_source_definitions_yaml")
@patch("ddpui.core.dbtautomation_service.read_sources_from_yaml")
def test_delete_dbt_source_in_project_success(
    mock_read_sources_from_yaml: Mock,
    mock_generate_source_definitions_yaml: Mock,
    orgdbt_source: OrgDbtModel,
):
    """Test that the function deletes the dbt source in the project"""
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
        ANY,
    )
