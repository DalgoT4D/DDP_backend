import uuid as uuid_module

import pytest

from ddpui.models.org import Org, OrgDbt
from ddpui.models.dbt_workflow import (
    OrgDbtModel,
    OrgDbtModelType,
    OrgDbtOperation,
    DbtEdge,
)
from ddpui.models.canvas_models import CanvasNode, CanvasNodeType, CanvasEdge
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
