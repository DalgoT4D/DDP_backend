import uuid as uuid_module
from pathlib import Path
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
from ddpui.core.orgdbt_manager import DbtProjectManager
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


def test_copy_dbt_dag_copies_models_and_skips_v1_tables():
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
        assert m.uuid is not None
        assert m.uuid not in {src_model.uuid, dest_model.uuid}
    # sql_path preserved — copy_repo_models_from_template lands the files at the same
    # project-relative paths, so the copied rows' paths stay valid
    by_name = {m.name: m for m in trial_models}
    assert by_name["stg_customers"].sql_path == src_model.sql_path
    assert by_name["customers"].sql_path == dest_model.sql_path

    assert set(model_map.keys()) == {src_model.id, dest_model.id}
    new_src = model_map[src_model.id]
    new_dest = model_map[dest_model.id]
    assert new_src.orgdbt_id == trial_dbt.id
    assert new_src.name == src_model.name
    assert new_dest.name == dest_model.name

    # the v1 tables are deliberately NOT copied — nothing in the live UI4T flow reads them
    assert not OrgDbtOperation.objects.filter(dbtmodel__orgdbt=trial_dbt).exists()
    assert not DbtEdge.objects.filter(from_node__orgdbt=trial_dbt).exists()

    # template rows are untouched
    assert OrgDbtModel.objects.filter(orgdbt=template_dbt).count() == 2
    assert OrgDbtOperation.objects.filter(dbtmodel__orgdbt=template_dbt).count() == 1
    assert DbtEdge.objects.filter(from_node__orgdbt=template_dbt).count() == 1


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
        position_x=-120.5,
        position_y=80.25,
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
    assert new_src_node.position_x == -120.5
    assert new_src_node.position_y == 80.25

    trial_edges = list(CanvasEdge.objects.filter(from_node__orgdbt=trial_dbt).order_by("seq"))
    assert len(trial_edges) == 2
    assert trial_edges[0].from_node_id == new_src_node.id
    assert trial_edges[0].to_node_id == new_op_node.id
    assert trial_edges[1].from_node_id == new_op_node.id
    assert trial_edges[1].to_node_id == new_dest_node.id

    # template rows are untouched
    assert CanvasNode.objects.filter(orgdbt=template_dbt).count() == 3


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
    git_call_args = mock_git_manager_cls.call_args.args
    assert git_call_args[0] == DbtProjectManager.get_dbt_project_dir(trial_dbt)
    assert git_call_args[1] == mock_git_manager_cls.get_org_admin_pat.return_value
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


@patch("ddpui.core.trial.dbt_clone.GitManager")
@patch("ddpui.core.trial.dbt_clone.create_or_update_dbt_model_in_project_v2")
def test_regenerate_and_push_regenerates_models_in_topological_order(
    mock_regen_model, mock_git_manager_cls
):
    """model_b depends on model_a via a DbtEdge; each has its own canvas operation chain so
    regen can find a terminal node for both. The topological sort must regenerate model_a
    (the dependency) before model_b (the dependent)."""
    trial_org = Org.objects.create(name="trial-topo", slug="trial-topo")
    trial_dbt = _make_orgdbt(trial_org)
    _make_warehouse(trial_org)

    model_a = OrgDbtModel.objects.create(
        orgdbt=trial_dbt,
        name="model_a",
        display_name="model_a",
        schema="analytics",
        type=OrgDbtModelType.MODEL,
    )
    model_b = OrgDbtModel.objects.create(
        orgdbt=trial_dbt,
        name="model_b",
        display_name="model_b",
        schema="analytics",
        type=OrgDbtModelType.MODEL,
    )
    DbtEdge.objects.create(from_node=model_a, to_node=model_b)

    op_node_a = CanvasNode.objects.create(
        orgdbt=trial_dbt,
        node_type=CanvasNodeType.OPERATION,
        name="op-a",
        operation_config={"type": "rename", "config": {}},
    )
    model_node_a = CanvasNode.objects.create(
        orgdbt=trial_dbt,
        node_type=CanvasNodeType.MODEL,
        name="model_a",
        dbtmodel=model_a,
    )
    CanvasEdge.objects.create(from_node=op_node_a, to_node=model_node_a, seq=1)

    op_node_b = CanvasNode.objects.create(
        orgdbt=trial_dbt,
        node_type=CanvasNodeType.OPERATION,
        name="op-b",
        operation_config={"type": "rename", "config": {}},
    )
    model_node_b = CanvasNode.objects.create(
        orgdbt=trial_dbt,
        node_type=CanvasNodeType.MODEL,
        name="model_b",
        dbtmodel=model_b,
    )
    CanvasEdge.objects.create(from_node=op_node_b, to_node=model_node_b, seq=1)

    mock_regen_model.side_effect = [
        ("models/analytics/model_a.sql", []),
        ("models/analytics/model_b.sql", []),
    ]

    count = dbt_clone.regenerate_and_push(trial_org, trial_dbt)

    assert count == 2
    assert mock_regen_model.call_count == 2
    first_call_config = mock_regen_model.call_args_list[0].args[1]
    second_call_config = mock_regen_model.call_args_list[1].args[1]
    assert first_call_config["output_name"] == "model_a"
    assert second_call_config["output_name"] == "model_b"


@patch("ddpui.core.trial.dbt_clone.GitManager")
@patch("ddpui.core.trial.dbt_clone.create_or_update_dbt_model_in_project_v2")
def test_regenerate_and_push_raises_on_ambiguous_terminal(mock_regen_model, mock_git_manager_cls):
    """A MODEL canvas node with TWO incoming CanvasEdges (e.g. a stale edge left behind by
    re-terminating the model from a different operation chain during editing) must raise a
    loud, clear error rather than silently regenerating from whichever edge `.first()` happens
    to return."""
    trial_org = Org.objects.create(name="trial-ambig", slug="trial-ambig")
    trial_dbt = _make_orgdbt(trial_org)
    _make_warehouse(trial_org)

    dest_model = OrgDbtModel.objects.create(
        orgdbt=trial_dbt,
        name="ambiguous_model",
        display_name="ambiguous_model",
        schema="analytics",
        type=OrgDbtModelType.MODEL,
    )
    dest_node = CanvasNode.objects.create(
        orgdbt=trial_dbt,
        node_type=CanvasNodeType.MODEL,
        name="ambiguous_model",
        dbtmodel=dest_model,
    )
    op_node_1 = CanvasNode.objects.create(
        orgdbt=trial_dbt,
        node_type=CanvasNodeType.OPERATION,
        name="op-1-stale",
        operation_config={"type": "rename", "config": {}},
    )
    op_node_2 = CanvasNode.objects.create(
        orgdbt=trial_dbt,
        node_type=CanvasNodeType.OPERATION,
        name="op-2-current",
        operation_config={"type": "rename", "config": {}},
    )
    CanvasEdge.objects.create(from_node=op_node_1, to_node=dest_node, seq=1)
    CanvasEdge.objects.create(from_node=op_node_2, to_node=dest_node, seq=1)

    with pytest.raises(RuntimeError, match="ambiguous terminal node"):
        dbt_clone.regenerate_and_push(trial_org, trial_dbt)

    mock_regen_model.assert_not_called()
    mock_git_manager_cls.assert_not_called()


@patch("ddpui.core.trial.dbt_clone.GitManager")
@patch("ddpui.core.trial.dbt_clone.create_or_update_dbt_model_in_project_v2")
@patch("ddpui.core.trial.dbt_clone.ensure_source_yml_definition_in_project")
def test_regenerate_and_push_skips_sources_not_on_canvas(
    mock_ensure_source, mock_regen_model, mock_git_manager_cls
):
    """Copied SOURCE rows include every warehouse table the template synced (raw _airbyte_tmp
    tables etc.) — only the ones with a CANVAS NODE get a sources.yml entry. Writing all of
    them let the repo-to-canvas sync (transform_type=github) mint canvas nodes for every entry,
    flooding the trial canvas with nodes the template canvas never had."""
    template_org = Org.objects.create(name="tmpl-skip", slug="tmpl-skip")
    trial_org = Org.objects.create(name="trial-skip", slug="trial-skip")
    trial_dbt = _make_orgdbt(trial_org)
    trial_org.dbt = trial_dbt
    trial_org.save()
    OrgWarehouse.objects.create(org=trial_org, wtype="postgres", credentials="secret")

    on_canvas = OrgDbtModel.objects.create(
        orgdbt=trial_dbt,
        name="pivottest",
        display_name="pivottest",
        schema="staging",
        type=OrgDbtModelType.SOURCE,
        source_name="staging",
    )
    CanvasNode.objects.create(
        orgdbt=trial_dbt,
        node_type=CanvasNodeType.SOURCE,
        name="staging.pivottest",
        dbtmodel=on_canvas,
        output_cols=[],
    )
    # synced raw table — copied row, NO canvas node -> must be skipped
    OrgDbtModel.objects.create(
        orgdbt=trial_dbt,
        name="pivottest_airbyte_tmp",
        display_name="pivottest_airbyte_tmp",
        schema="staging",
        type=OrgDbtModelType.SOURCE,
        source_name="staging",
    )

    mock_ensure_source.return_value = SourceYmlDefinition(
        source_name="staging",
        source_schema="staging",
        table="pivottest",
        sql_path="models/sources/sources.yml",
    )

    count = dbt_clone.regenerate_and_push(trial_org, trial_dbt)

    assert count == 0  # no MODEL rows in this fixture
    mock_ensure_source.assert_called_once_with(trial_dbt, "staging", "pivottest")


@patch("ddpui.core.trial.dbt_clone.GitManager")
def test_copy_repo_models_from_template_copies_verbatim_and_pushes(mock_git_manager_cls, tmp_path):
    """models/ dir (sql + sources.yml + docs) copied byte-identical from a FRESH CLONE of the
    template's remote repo (never the template's local working dir) into the trial repo;
    commit + push with the org-admin PAT; file count returned."""
    import os

    os.environ["CLIENTDBT_ROOT"] = str(tmp_path)
    template_org = Org.objects.create(name="tmpl-vc", slug="tmpl-vc")
    trial_org = Org.objects.create(name="trial-vc", slug="trial-vc")
    template_dbt = _make_orgdbt(template_org)
    template_dbt.gitrepo_url = "https://github.com/dalgo/tmpl-vc-dbtrepo.git"
    template_dbt.save()
    trial_dbt = _make_orgdbt(trial_org)

    # the template's LOCAL working dir gets a decoy file — proves it's never read
    template_dir = Path(DbtProjectManager.get_dbt_project_dir(template_dbt))
    (template_dir / "models" / "staging").mkdir(parents=True)
    (template_dir / "models" / "staging" / "decoy_local_only.sql").write_text("select 999")

    trial_dir = Path(DbtProjectManager.get_dbt_project_dir(trial_dbt))
    (trial_dir / "models").mkdir(parents=True)  # empty scaffold

    mock_git_manager_cls.get_org_admin_pat.return_value = "admin-pat"

    def _fake_clone(cwd, remote_repo_url, relative_path, pat=None):
        assert remote_repo_url == template_dbt.gitrepo_url
        assert pat == "admin-pat"
        cloned_models = Path(cwd) / relative_path / "models" / "staging"
        cloned_models.mkdir(parents=True)
        (cloned_models / "sources.yml").write_text("sources: []")
        (cloned_models / "casted_pivottest.sql").write_text("select 1")

    mock_git_manager_cls.clone.side_effect = _fake_clone

    count = dbt_clone.copy_repo_models_from_template(template_dbt, trial_dbt)

    assert count == 2
    assert (trial_dir / "models" / "staging" / "casted_pivottest.sql").read_text() == "select 1"
    assert (trial_dir / "models" / "staging" / "sources.yml").read_text() == "sources: []"
    assert not (trial_dir / "models" / "staging" / "decoy_local_only.sql").exists()
    mock_git_manager_cls.assert_called_once_with(str(trial_dir), "admin-pat")
    push_instance = mock_git_manager_cls.return_value
    push_instance.commit_changes.assert_called_once_with("clone template dbt models")
    push_instance.push_changes.assert_called_once()


@patch("ddpui.core.trial.dbt_clone.GitManager")
def test_copy_repo_models_from_template_raises_when_no_models_dir(mock_git_manager_cls, tmp_path):
    """template's remote clone has no models/ dir (e.g. an empty repo) — fails loud."""
    import os

    os.environ["CLIENTDBT_ROOT"] = str(tmp_path)
    template_org = Org.objects.create(name="tmpl-nm", slug="tmpl-nm")
    trial_org = Org.objects.create(name="trial-nm", slug="trial-nm")
    template_dbt = _make_orgdbt(template_org)
    trial_dbt = _make_orgdbt(trial_org)

    def _fake_clone_empty(cwd, remote_repo_url, relative_path, pat=None):
        (Path(cwd) / relative_path).mkdir(parents=True)  # cloned repo, no models/ dir

    mock_git_manager_cls.get_org_admin_pat.return_value = "admin-pat"
    mock_git_manager_cls.clone.side_effect = _fake_clone_empty

    with pytest.raises(RuntimeError, match="no models/ directory"):
        dbt_clone.copy_repo_models_from_template(template_dbt, trial_dbt)
    mock_git_manager_cls.return_value.push_changes.assert_not_called()
