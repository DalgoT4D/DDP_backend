import os

import yaml
import uuid as uuid_module
from pathlib import Path
from unittest.mock import MagicMock, patch

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


def test_copy_dbt_dag_preserves_sql_path_when_requested():
    """preserve_sql_path=True (the file-based/COPY-path branch) must carry `sql_path` forward
    unchanged, since the copy path copies the actual .sql files to those same project-relative
    paths in the trial repo — nulling it (the default, regen-path behaviour) would leave the
    copied model rows pointing at nothing."""
    template_org = Org.objects.create(name="tmpl-preserve", slug="tmpl-preserve")
    trial_org = Org.objects.create(name="trial-preserve", slug="trial-preserve")
    template_dbt = _make_orgdbt(template_org)
    trial_dbt = _make_orgdbt(trial_org)

    OrgDbtModel.objects.create(
        orgdbt=template_dbt,
        name="customers",
        display_name="customers",
        schema="analytics",
        sql_path="models/marts/customers.sql",
        type=OrgDbtModelType.MODEL,
    )

    model_map = dbt_clone.copy_dbt_dag(template_dbt, trial_dbt, preserve_sql_path=True)

    trial_model = list(model_map.values())[0]
    assert trial_model.sql_path == "models/marts/customers.sql"
    trial_model.refresh_from_db()
    assert trial_model.sql_path == "models/marts/customers.sql"


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


def _make_trial_dbt_with_scaffold(org: Org, tmp_path: Path) -> OrgDbt:
    """A trial OrgDbt whose project dir already exists on disk with an empty `models/` dir —
    mirrors the empty scaffold `setup_managed_git_workspace` creates before `copy_dbt_repo_files`
    is ever called."""
    os.environ["CLIENTDBT_ROOT"] = str(tmp_path)
    trial_dbt = OrgDbt.objects.create(
        gitrepo_url=f"https://github.com/dalgo-managed/{org.slug}.git",
        is_repo_managed_by_system=True,
        project_dir=f"{org.slug}/dbtrepo",
        dbt_venv="test_venv",
        target_type="postgres",
        default_schema="default_schema",
        transform_type="github",
    )
    project_dir = Path(DbtProjectManager.get_dbt_project_dir(trial_dbt))
    (project_dir / "models").mkdir(parents=True, exist_ok=True)
    return trial_dbt


@patch("ddpui.core.trial.dbt_clone.retrieve_github_pat")
@patch("ddpui.core.trial.dbt_clone.GitManager")
def test_copy_dbt_repo_files_clones_and_pushes(mock_git_manager_cls, mock_retrieve_pat, tmp_path):
    """external (non-Dalgo-managed) template repo: PAT comes from
    retrieve_github_pat(gitrepo_access_token_secret); model files land in the trial repo dir;
    the trial repo is committed + pushed exactly once."""
    template_org = Org.objects.create(name="tmpl-copyfiles", slug="tmpl-copyfiles")
    trial_org = Org.objects.create(name="trial-copyfiles", slug="trial-copyfiles")

    template_dbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/some-ngo/health_org.git",
        gitrepo_access_token_secret="template-pat-secret",
        is_repo_managed_by_system=False,
        project_dir="tmpl-copyfiles/dbtrepo",
        dbt_venv="test_venv",
        target_type="postgres",
        default_schema="default_schema",
        transform_type="github",
    )
    trial_dbt = _make_trial_dbt_with_scaffold(trial_org, tmp_path)

    # fake the cloned template repo: a real dir on disk with a models/ folder to copy from
    fake_clone_dir = tmp_path / "cloned_template_repo"
    (fake_clone_dir / "models" / "staging").mkdir(parents=True)
    (fake_clone_dir / "models" / "staging" / "stg_customers.sql").write_text("select 1")

    mock_clone_instance = MagicMock()
    mock_clone_instance.repo_local_path = str(fake_clone_dir)
    mock_git_manager_cls.clone.return_value = mock_clone_instance
    mock_git_manager_cls.get_org_admin_pat.return_value = "admin-pat"
    mock_push_instance = mock_git_manager_cls.return_value
    mock_retrieve_pat.return_value = "template-pat"

    dbt_clone.copy_dbt_repo_files(template_dbt, trial_dbt)

    mock_retrieve_pat.assert_called_once_with("template-pat-secret")

    clone_call = mock_git_manager_cls.clone.call_args
    assert clone_call.kwargs["remote_repo_url"] == template_dbt.gitrepo_url
    assert clone_call.kwargs["pat"] == "template-pat"

    trial_repo_dir = Path(DbtProjectManager.get_dbt_project_dir(trial_dbt))
    copied_file = trial_repo_dir / "models" / "staging" / "stg_customers.sql"
    assert copied_file.exists()
    assert copied_file.read_text() == "select 1"

    mock_git_manager_cls.assert_called_once_with(
        repo_local_path=str(trial_repo_dir), pat="admin-pat"
    )
    mock_push_instance.commit_changes.assert_called_once_with("clone template dbt models from git")
    mock_push_instance.push_changes.assert_called_once()


@patch("ddpui.core.trial.dbt_clone.retrieve_github_pat")
@patch("ddpui.core.trial.dbt_clone.GitManager")
def test_copy_dbt_repo_files_carries_project_config_macros_and_packages(
    mock_git_manager_cls, mock_retrieve_pat, tmp_path
):
    """the full-project copy: macros/, seeds/, packages.yml must land in the trial repo, and the
    template's dbt_project.yml folder-level config (+materialized etc.) must be merged over the
    scaffold's — re-keyed from the template's project name to the scaffold's, with the
    scaffold's name/profile preserved (they must keep matching the cli profile block)."""
    template_org = Org.objects.create(name="tmpl-fullcopy", slug="tmpl-fullcopy")
    trial_org = Org.objects.create(name="trial-fullcopy", slug="trial-fullcopy")

    template_dbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/some-ngo/health_org.git",
        gitrepo_access_token_secret="template-pat-secret",
        is_repo_managed_by_system=False,
        project_dir="tmpl-fullcopy/dbtrepo",
        dbt_venv="test_venv",
        target_type="postgres",
        default_schema="default_schema",
        transform_type="github",
    )
    trial_dbt = _make_trial_dbt_with_scaffold(trial_org, tmp_path)
    trial_repo_dir = Path(DbtProjectManager.get_dbt_project_dir(trial_dbt))
    # scaffold's stock dbt_project.yml (what `dbt init` + setup_managed_git_workspace leave)
    (trial_repo_dir / "dbt_project.yml").write_text(
        "name: dbtrepo\nprofile: dbtrepo\nmodels:\n  dbtrepo:\n    example:\n"
        "      +materialized: view\n"
    )

    fake_clone_dir = tmp_path / "cloned_template_repo_full"
    (fake_clone_dir / "models").mkdir(parents=True)
    (fake_clone_dir / "models" / "mart.sql").write_text("select 1")
    (fake_clone_dir / "macros").mkdir()
    (fake_clone_dir / "macros" / "unpivot_custom.sql").write_text("{% macro m() %}{% endmacro %}")
    (fake_clone_dir / "seeds").mkdir()
    (fake_clone_dir / "seeds" / "lookup.csv").write_text("id\n1")
    (fake_clone_dir / "packages.yml").write_text(
        "packages:\n  - package: dbt-labs/dbt_utils\n    version: 1.3.0\n"
    )
    # template project named differently, with folder-level config that must survive re-keyed
    (fake_clone_dir / "dbt_project.yml").write_text(
        "name: health_org\nprofile: health_org\nmodels:\n  health_org:\n    marts:\n"
        "      +materialized: table\nvars:\n  start_year: 2020\n"
    )

    mock_clone_instance = MagicMock()
    mock_clone_instance.repo_local_path = str(fake_clone_dir)
    mock_git_manager_cls.clone.return_value = mock_clone_instance
    mock_git_manager_cls.get_org_admin_pat.return_value = "admin-pat"
    mock_retrieve_pat.return_value = "template-pat"

    dbt_clone.copy_dbt_repo_files(template_dbt, trial_dbt)

    assert (trial_repo_dir / "models" / "mart.sql").exists()
    assert (trial_repo_dir / "macros" / "unpivot_custom.sql").exists()
    assert (trial_repo_dir / "seeds" / "lookup.csv").exists()
    assert "1.3.0" in (trial_repo_dir / "packages.yml").read_text()

    merged = yaml.safe_load((trial_repo_dir / "dbt_project.yml").read_text())
    assert merged["name"] == "dbtrepo"  # scaffold identity preserved
    assert merged["profile"] == "dbtrepo"
    assert merged["models"]["dbtrepo"]["marts"]["+materialized"] == "table"  # re-keyed
    assert "health_org" not in merged["models"]
    assert merged["vars"] == {"start_year": 2020}  # template vars survive


@patch("ddpui.core.trial.dbt_clone.retrieve_github_pat")
@patch("ddpui.core.trial.dbt_clone.GitManager")
def test_copy_dbt_repo_files_raises_when_template_has_no_models_dir(
    mock_git_manager_cls, mock_retrieve_pat, tmp_path
):
    """copy_dbt_dag has already created trial OrgDbtModel rows with sql_path set; if the template
    repo has no models/ dir to copy, the trial dbt would be half-populated (metadata, no .sql).
    Must raise loudly, not silently continue."""
    template_org = Org.objects.create(name="tmpl-nomodels", slug="tmpl-nomodels")
    trial_org = Org.objects.create(name="trial-nomodels", slug="trial-nomodels")

    template_dbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/some-ngo/empty.git",
        gitrepo_access_token_secret="template-pat-secret",
        is_repo_managed_by_system=False,
        project_dir="tmpl-nomodels/dbtrepo",
        dbt_venv="test_venv",
        target_type="postgres",
        default_schema="default_schema",
        transform_type="github",
    )
    trial_dbt = _make_trial_dbt_with_scaffold(trial_org, tmp_path)

    # cloned template repo has NO models/ dir
    fake_clone_dir = tmp_path / "cloned_empty_repo"
    fake_clone_dir.mkdir(parents=True)

    mock_clone_instance = MagicMock()
    mock_clone_instance.repo_local_path = str(fake_clone_dir)
    mock_git_manager_cls.clone.return_value = mock_clone_instance
    mock_git_manager_cls.get_org_admin_pat.return_value = "admin-pat"
    mock_retrieve_pat.return_value = "template-pat"

    with pytest.raises(RuntimeError, match="no models/ directory"):
        dbt_clone.copy_dbt_repo_files(template_dbt, trial_dbt)

    # never pushed a half-populated repo
    mock_git_manager_cls.return_value.push_changes.assert_not_called()


@patch("ddpui.core.trial.dbt_clone.retrieve_github_pat")
@patch("ddpui.core.trial.dbt_clone.GitManager")
def test_copy_dbt_repo_files_uses_admin_pat_for_managed_template(
    mock_git_manager_cls, mock_retrieve_pat, tmp_path
):
    """Dalgo-managed template repo (is_repo_managed_by_system=True): cloning must use the
    org-admin PAT, never retrieve_github_pat (managed repos don't store a per-org secret)."""
    template_org = Org.objects.create(name="tmpl-managed", slug="tmpl-managed")
    trial_org = Org.objects.create(name="trial-managed", slug="trial-managed")

    template_dbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/dalgo-managed/tmpl-managed.git",
        is_repo_managed_by_system=True,
        project_dir="tmpl-managed/dbtrepo",
        dbt_venv="test_venv",
        target_type="postgres",
        default_schema="default_schema",
        transform_type="github",
    )
    trial_dbt = _make_trial_dbt_with_scaffold(trial_org, tmp_path)

    fake_clone_dir = tmp_path / "cloned_template_repo_managed"
    (fake_clone_dir / "models").mkdir(parents=True)

    mock_clone_instance = MagicMock()
    mock_clone_instance.repo_local_path = str(fake_clone_dir)
    mock_git_manager_cls.clone.return_value = mock_clone_instance
    mock_git_manager_cls.get_org_admin_pat.return_value = "admin-pat"

    dbt_clone.copy_dbt_repo_files(template_dbt, trial_dbt)

    mock_retrieve_pat.assert_not_called()
    clone_call = mock_git_manager_cls.clone.call_args
    assert clone_call.kwargs["pat"] == "admin-pat"
