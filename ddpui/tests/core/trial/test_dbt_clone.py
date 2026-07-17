import uuid as uuid_module
from unittest.mock import patch

import pytest

from ddpui.models.org import Org, OrgDbt, OrgWarehouse
from ddpui.models.dbt_workflow import (
    OrgDbtModel,
    OrgDbtModelType,
    OrgDbtOperation,
    DbtEdge,
)
from ddpui.models.canvas_models import CanvasNode, CanvasNodeType, CanvasEdge
from ddpui.core.dbtautomation_service import SourceYmlDefinition
from ddpui.core.trial import dbt_clone

pytestmark = pytest.mark.django_db


def _make_orgdbt(org: Org) -> OrgDbt:
    return OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir=f"test_project_dir_{org.slug}",
        dbt_venv="test_venv",
        target_type="postgres",
        default_schema="default_schema",
        transform_type="github",
    )


def test_copy_dbt_dag_copies_models_operations_edges():
    template_org = Org.objects.create(name="tmpl", slug="tmpl")
    trial_org = Org.objects.create(name="trial", slug="trial")
    template_dbt = _make_orgdbt(template_org)
    trial_dbt = _make_orgdbt(trial_org)

    src_model = OrgDbtModel.objects.create(
        orgdbt=template_dbt,
        name="stg_customers",
        display_name="stg_customers",
        schema="staging",
        sql_path="models/staging/stg_customers.sql",
        type=OrgDbtModelType.SOURCE,
        source_name="raw",
        output_cols=["id", "name"],
        under_construction=False,
    )
    dest_model = OrgDbtModel.objects.create(
        orgdbt=template_dbt,
        name="customers",
        display_name="customers",
        schema="analytics",
        sql_path="models/marts/customers.sql",
        type=OrgDbtModelType.MODEL,
        source_name=None,
        output_cols=["id", "name"],
        under_construction=False,
    )
    OrgDbtOperation.objects.create(
        dbtmodel=dest_model,
        uuid=uuid_module.uuid4(),
        seq=1,
        output_cols=["id", "name"],
        config={"type": "rename"},
    )
    DbtEdge.objects.create(from_node=src_model, to_node=dest_model)

    model_map = dbt_clone.copy_dbt_dag(template_dbt, trial_dbt)

    trial_models = list(OrgDbtModel.objects.filter(orgdbt=trial_dbt))
    assert len(trial_models) == 2
    for m in trial_models:
        assert m.sql_path is None
        assert m.uuid is not None
        assert m.uuid not in {src_model.uuid, dest_model.uuid}

    assert set(model_map.keys()) == {src_model.id, dest_model.id}
    new_src = model_map[src_model.id]
    new_dest = model_map[dest_model.id]
    assert new_src.orgdbt_id == trial_dbt.id
    assert new_src.name == src_model.name
    assert new_dest.name == dest_model.name

    trial_ops = list(OrgDbtOperation.objects.filter(dbtmodel__orgdbt=trial_dbt))
    assert len(trial_ops) == 1
    assert trial_ops[0].dbtmodel_id == new_dest.id
    assert trial_ops[0].config == {"type": "rename"}
    assert trial_ops[0].uuid is not None

    trial_edges = list(DbtEdge.objects.filter(from_node__orgdbt=trial_dbt))
    assert len(trial_edges) == 1
    assert trial_edges[0].from_node_id == new_src.id
    assert trial_edges[0].to_node_id == new_dest.id

    # template rows are untouched
    assert OrgDbtModel.objects.filter(orgdbt=template_dbt).count() == 2


def test_copy_dbt_dag_copies_canvas():
    template_org = Org.objects.create(name="tmpl2", slug="tmpl2")
    trial_org = Org.objects.create(name="trial2", slug="trial2")
    template_dbt = _make_orgdbt(template_org)
    trial_dbt = _make_orgdbt(trial_org)

    src_model = OrgDbtModel.objects.create(
        orgdbt=template_dbt,
        name="stg_customers",
        display_name="stg_customers",
        schema="staging",
        type=OrgDbtModelType.SOURCE,
        source_name="raw",
    )
    dest_model = OrgDbtModel.objects.create(
        orgdbt=template_dbt,
        name="customers",
        display_name="customers",
        schema="analytics",
        type=OrgDbtModelType.MODEL,
    )

    src_node = CanvasNode.objects.create(
        orgdbt=template_dbt,
        node_type=CanvasNodeType.SOURCE,
        name="stg_customers",
        output_cols=["id"],
        dbtmodel=src_model,
    )
    op_node = CanvasNode.objects.create(
        orgdbt=template_dbt,
        node_type=CanvasNodeType.OPERATION,
        name="rename-op",
        operation_config={"operation_type": "rename"},
        output_cols=["id"],
        dbtmodel=None,
    )
    dest_node = CanvasNode.objects.create(
        orgdbt=template_dbt,
        node_type=CanvasNodeType.MODEL,
        name="customers",
        output_cols=["id"],
        dbtmodel=dest_model,
    )
    CanvasEdge.objects.create(from_node=src_node, to_node=op_node, seq=1)
    CanvasEdge.objects.create(from_node=op_node, to_node=dest_node, seq=2)

    model_map = dbt_clone.copy_dbt_dag(template_dbt, trial_dbt)

    trial_nodes = {n.name: n for n in CanvasNode.objects.filter(orgdbt=trial_dbt)}
    assert set(trial_nodes.keys()) == {"stg_customers", "rename-op", "customers"}

    new_src_node = trial_nodes["stg_customers"]
    new_op_node = trial_nodes["rename-op"]
    new_dest_node = trial_nodes["customers"]

    assert new_src_node.dbtmodel_id == model_map[src_model.id].id
    assert new_dest_node.dbtmodel_id == model_map[dest_model.id].id
    assert new_op_node.dbtmodel_id is None
    assert new_op_node.operation_config == {"operation_type": "rename"}
    assert new_src_node.uuid != src_node.uuid

    trial_edges = list(CanvasEdge.objects.filter(from_node__orgdbt=trial_dbt).order_by("seq"))
    assert len(trial_edges) == 2
    assert trial_edges[0].from_node_id == new_src_node.id
    assert trial_edges[0].to_node_id == new_op_node.id
    assert trial_edges[1].from_node_id == new_op_node.id
    assert trial_edges[1].to_node_id == new_dest_node.id

    # template rows are untouched
    assert CanvasNode.objects.filter(orgdbt=template_dbt).count() == 3


def test_copy_dbt_dag_remaps_input_models_uuid():
    template_org = Org.objects.create(name="tmpl3", slug="tmpl3")
    trial_org = Org.objects.create(name="trial3", slug="trial3")
    template_dbt = _make_orgdbt(template_org)
    trial_dbt = _make_orgdbt(trial_org)

    parent_model = OrgDbtModel.objects.create(
        orgdbt=template_dbt,
        uuid=uuid_module.uuid4(),
        name="stg_customers",
        display_name="stg_customers",
        schema="staging",
        sql_path="models/staging/stg_customers.sql",
        type=OrgDbtModelType.SOURCE,
        source_name="raw",
        output_cols=["id", "name"],
        under_construction=False,
    )
    child_model = OrgDbtModel.objects.create(
        orgdbt=template_dbt,
        uuid=uuid_module.uuid4(),
        name="customers",
        display_name="customers",
        schema="analytics",
        sql_path="models/marts/customers.sql",
        type=OrgDbtModelType.MODEL,
        source_name=None,
        output_cols=["id", "name"],
        under_construction=False,
    )

    original_config = {
        "type": "rename",
        "input_models": [
            {
                "uuid": str(parent_model.uuid),
                "name": parent_model.name,
                "source_name": parent_model.source_name,
                "schema": parent_model.schema,
                "type": "source",
            }
        ],
    }
    OrgDbtOperation.objects.create(
        dbtmodel=child_model,
        uuid=uuid_module.uuid4(),
        seq=1,
        output_cols=["id", "name"],
        config=original_config,
    )

    model_map = dbt_clone.copy_dbt_dag(template_dbt, trial_dbt)

    new_parent = model_map[parent_model.id]
    new_child = model_map[child_model.id]

    trial_op = OrgDbtOperation.objects.get(dbtmodel=new_child)
    assert trial_op.config["input_models"][0]["uuid"] == str(new_parent.uuid)
    assert trial_op.config["input_models"][0]["name"] == parent_model.name
    assert trial_op.config["input_models"][0]["schema"] == parent_model.schema

    # template operation's config must be unmutated (deepcopy, not shared reference)
    template_op = OrgDbtOperation.objects.get(dbtmodel=child_model)
    assert template_op.config["input_models"][0]["uuid"] == str(parent_model.uuid)
    assert original_config["input_models"][0]["uuid"] == str(parent_model.uuid)


def test_copy_dbt_dag_handles_missing_or_none_config():
    template_org = Org.objects.create(name="tmpl4", slug="tmpl4")
    trial_org = Org.objects.create(name="trial4", slug="trial4")
    template_dbt = _make_orgdbt(template_org)
    trial_dbt = _make_orgdbt(trial_org)

    model_none_config = OrgDbtModel.objects.create(
        orgdbt=template_dbt,
        uuid=uuid_module.uuid4(),
        name="model_none",
        display_name="model_none",
        schema="analytics",
        type=OrgDbtModelType.MODEL,
    )
    model_no_input_models = OrgDbtModel.objects.create(
        orgdbt=template_dbt,
        uuid=uuid_module.uuid4(),
        name="model_no_input_models",
        display_name="model_no_input_models",
        schema="analytics",
        type=OrgDbtModelType.MODEL,
    )
    OrgDbtOperation.objects.create(
        dbtmodel=model_none_config,
        uuid=uuid_module.uuid4(),
        seq=1,
        output_cols=[],
        config=None,
    )
    OrgDbtOperation.objects.create(
        dbtmodel=model_no_input_models,
        uuid=uuid_module.uuid4(),
        seq=1,
        output_cols=[],
        config={"type": "rename"},
    )

    # should not raise
    model_map = dbt_clone.copy_dbt_dag(template_dbt, trial_dbt)

    new_none_config_model = model_map[model_none_config.id]
    new_no_input_models_model = model_map[model_no_input_models.id]

    op1 = OrgDbtOperation.objects.get(dbtmodel=new_none_config_model)
    assert op1.config is None

    op2 = OrgDbtOperation.objects.get(dbtmodel=new_no_input_models_model)
    assert op2.config == {"type": "rename"}


def _make_warehouse(org: Org) -> OrgWarehouse:
    return OrgWarehouse.objects.create(org=org, wtype="postgres", credentials="secret")


@patch("ddpui.core.trial.dbt_clone.GitManager")
@patch("ddpui.core.trial.dbt_clone.create_or_update_dbt_model_in_project_v2")
@patch("ddpui.core.trial.dbt_clone.ensure_source_yml_definition_in_project")
def test_regenerate_and_push_regenerates_sources_and_models(
    mock_ensure_source, mock_regen_model, mock_git_manager_cls
):
    trial_org = Org.objects.create(name="trial-regen", slug="trial-regen")
    trial_dbt = _make_orgdbt(trial_org)
    trial_org.dbt = trial_dbt
    trial_org.save()
    _make_warehouse(trial_org)

    source_model = OrgDbtModel.objects.create(
        orgdbt=trial_dbt,
        name="raw_customers",
        display_name="raw_customers",
        schema="raw",
        type=OrgDbtModelType.SOURCE,
        source_name="raw",
    )
    dest_model = OrgDbtModel.objects.create(
        orgdbt=trial_dbt,
        name="customers",
        display_name="customers",
        schema="analytics",
        type=OrgDbtModelType.MODEL,
    )
    DbtEdge.objects.create(from_node=source_model, to_node=dest_model)

    source_node = CanvasNode.objects.create(
        orgdbt=trial_dbt,
        node_type=CanvasNodeType.SOURCE,
        name="raw_customers",
        dbtmodel=source_model,
    )
    op_node = CanvasNode.objects.create(
        orgdbt=trial_dbt,
        node_type=CanvasNodeType.OPERATION,
        name="rename-op",
        operation_config={"type": "rename", "config": {}},
    )
    dest_node = CanvasNode.objects.create(
        orgdbt=trial_dbt,
        node_type=CanvasNodeType.MODEL,
        name="customers",
        dbtmodel=dest_model,
    )
    CanvasEdge.objects.create(from_node=source_node, to_node=op_node, seq=1)
    CanvasEdge.objects.create(from_node=op_node, to_node=dest_node, seq=1)

    mock_ensure_source.return_value = SourceYmlDefinition(
        source_name="raw",
        source_schema="raw",
        table="raw_customers",
        sql_path="models/sources/sources.yml",
    )
    mock_regen_model.return_value = ("models/analytics/customers.sql", ["id", "name"])
    mock_git_instance = mock_git_manager_cls.return_value

    count = dbt_clone.regenerate_and_push(trial_org, trial_dbt)

    assert count == 1
    mock_ensure_source.assert_called_once_with(trial_dbt, "raw", "raw_customers")

    source_model.refresh_from_db()
    dest_model.refresh_from_db()
    assert source_model.sql_path == "models/sources/sources.yml"
    assert dest_model.sql_path == "models/analytics/customers.sql"

    mock_regen_model.assert_called_once()
    call_args = mock_regen_model.call_args.args
    assert call_args[0].id == OrgWarehouse.objects.get(org=trial_org).id
    config = call_args[1]
    assert config["dest_schema"] == "analytics"
    assert config["output_name"] == "customers"
    assert isinstance(config["operations"], list) and len(config["operations"]) == 1
    assert call_args[2] == trial_dbt

    mock_git_manager_cls.assert_called_once()
    mock_git_instance.commit_changes.assert_called_once_with("clone template dbt models")
    mock_git_instance.push_changes.assert_called_once()


@patch("ddpui.core.trial.dbt_clone.GitManager")
@patch("ddpui.core.trial.dbt_clone.create_or_update_dbt_model_in_project_v2")
def test_regenerate_and_push_raises_without_warehouse(mock_regen_model, mock_git_manager_cls):
    trial_org = Org.objects.create(name="trial-nowh", slug="trial-nowh")
    trial_dbt = _make_orgdbt(trial_org)

    with pytest.raises(RuntimeError, match="no warehouse"):
        dbt_clone.regenerate_and_push(trial_org, trial_dbt)

    mock_regen_model.assert_not_called()
    mock_git_manager_cls.assert_not_called()


@patch("ddpui.core.trial.dbt_clone.GitManager")
@patch("ddpui.core.trial.dbt_clone.create_or_update_dbt_model_in_project_v2")
def test_regenerate_and_push_raises_when_model_has_no_operation_chain(
    mock_regen_model, mock_git_manager_cls
):
    trial_org = Org.objects.create(name="trial-noop", slug="trial-noop")
    trial_dbt = _make_orgdbt(trial_org)
    _make_warehouse(trial_org)

    dest_model = OrgDbtModel.objects.create(
        orgdbt=trial_dbt,
        name="orphan_model",
        display_name="orphan_model",
        schema="analytics",
        type=OrgDbtModelType.MODEL,
    )
    CanvasNode.objects.create(
        orgdbt=trial_dbt,
        node_type=CanvasNodeType.MODEL,
        name="orphan_model",
        dbtmodel=dest_model,
    )

    with pytest.raises(RuntimeError, match="no upstream operation chain"):
        dbt_clone.regenerate_and_push(trial_org, trial_dbt)

    mock_regen_model.assert_not_called()
    mock_git_manager_cls.assert_not_called()
