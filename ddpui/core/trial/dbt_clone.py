"""Deep-copy the template org's dbt transform DAG onto a freshly-created trial OrgDbt.

Copies BOTH the legacy row-model path (OrgDbtModel/OrgDbtOperation/DbtEdge) and the active v2
canvas path (CanvasNode/CanvasEdge) — the two systems coexist today (canvas_models.py:1-3), so a
clone that only copied one would leave the trial org's transform UI half-populated.

Rows only — the template's dbt CONTENT (`.sql` files / `sources.yml` / repo files) is
deliberately NOT cloned in v1. `sql_path` is left None on every copied OrgDbtModel: the trial's
dbt project is an empty scaffold (created by `setup_managed_git_workspace`, called by `_step_dbt`
before this runs) and no files exist for the copied rows. The dbt-content cloning paths
(`regenerate_and_push` for UI-operation-built templates, `copy_dbt_repo_files` for git-imported
ones) were removed as dead code — recover them from git history if content cloning is added later.

`uuid` is never copied from the template row — every copy mints its own via `uuid.uuid4()`,
matching how the rest of the codebase creates these rows (see
`ddpui/core/dbtautomation_service.py`, `ddpui/api/transform_api.py`,
`ddpui/management/commands/github_to_ui4t.py`) — `OrgDbtOperation.uuid` in particular is a
NOT NULL unique field with no model-level default, so it must always be supplied explicitly.
"""

import copy
import uuid

from ddpui.core.trial.exceptions import TrialCloneError
from ddpui.models.canvas_models import CanvasEdge, CanvasNode
from ddpui.models.dbt_workflow import DbtEdge, OrgDbtModel, OrgDbtOperation
from ddpui.models.org import OrgDbt
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.dbt_clone")


def _copy_canvas(template_dbt: OrgDbt, trial_dbt: OrgDbt, model_map: dict) -> None:
    """Copy CanvasNode/CanvasEdge rows (the active v2 transform-canvas path) re-parented onto
    `trial_dbt`. MODEL/SOURCE nodes' `dbtmodel` FK is remapped via `model_map`; OPERATION nodes
    carry `dbtmodel=None` already and are copied as-is."""
    node_map: dict = {}
    for node in CanvasNode.objects.filter(orgdbt=template_dbt):
        new_dbtmodel = None
        if node.dbtmodel_id:
            if node.dbtmodel_id not in model_map:
                raise TrialCloneError(
                    f"canvas node {node.uuid} references a dbtmodel outside the template dbt"
                )
            new_dbtmodel = model_map[node.dbtmodel_id]
        new_node = CanvasNode.objects.create(
            orgdbt=trial_dbt,
            uuid=uuid.uuid4(),
            node_type=node.node_type,
            name=node.name,
            operation_config=node.operation_config,
            output_cols=node.output_cols,
            dbtmodel=new_dbtmodel,
        )
        node_map[node.id] = new_node

    for edge in CanvasEdge.objects.filter(from_node__orgdbt=template_dbt):
        if edge.to_node_id not in node_map:
            raise TrialCloneError(
                f"canvas edge {edge.id} points at a node outside the template dbt"
            )
        CanvasEdge.objects.create(
            from_node=node_map[edge.from_node_id],
            to_node=node_map[edge.to_node_id],
            seq=edge.seq,
        )

    # The mirror case — an edge INTO the template graph from an outside node — would be
    # silently dropped by the from_node filter above, losing a dependency. Fail loud instead.
    stray = (
        CanvasEdge.objects.filter(to_node__orgdbt=template_dbt)
        .exclude(from_node__orgdbt=template_dbt)
        .first()
    )
    if stray is not None:
        raise TrialCloneError(f"canvas edge {stray.id} comes from a node outside the template dbt")


def _remap_operation_config(config, uuid_map: dict):
    """Deep-copy `config` (an `OrgDbtOperation.config`) and, for operations whose config is a
    dict with an `input_models` list (seq==1 primary input, plus multi-input ops such as
    join/unionall at any seq), remap each entry's `uuid` (a reference to a parent
    `OrgDbtModel.uuid`) via `uuid_map`. Never mutates the caller's `config`. Defensive against
    `config` being None/non-dict, `input_models` being absent, and entries lacking a `uuid` or
    referencing a uuid outside `uuid_map` (e.g. a cross-org/legacy reference) — those are left
    untouched."""
    new_config = copy.deepcopy(config)
    if not isinstance(new_config, dict):
        return new_config

    input_models = new_config.get("input_models")
    if not isinstance(input_models, list):
        return new_config

    for entry in input_models:
        if not isinstance(entry, dict):
            continue
        old_uuid = entry.get("uuid")
        if old_uuid is not None and str(old_uuid) in uuid_map:
            entry["uuid"] = uuid_map[str(old_uuid)]

    return new_config


def copy_dbt_dag(template_dbt: OrgDbt, trial_dbt: OrgDbt) -> dict:
    """Deep-copy every OrgDbtModel/OrgDbtOperation/DbtEdge (legacy) AND CanvasNode/CanvasEdge
    (active v2) row from `template_dbt` onto `trial_dbt`, re-parenting FKs to the new rows via
    an old-model-id -> new-model old->new map.

    `sql_path` is nulled on every copied OrgDbtModel — the trial's dbt project is an empty
    scaffold and no `.sql` files exist for the copied rows (dbt content is not cloned in v1).

    Returns `model_map` (`{old OrgDbtModel.id: new OrgDbtModel}`).
    """
    model_map: dict = {}
    uuid_map: dict = {}
    for m in OrgDbtModel.objects.filter(orgdbt=template_dbt):
        new_m = OrgDbtModel.objects.create(
            orgdbt=trial_dbt,
            uuid=uuid.uuid4(),
            name=m.name,
            display_name=m.display_name,
            schema=m.schema,
            sql_path=None,
            type=m.type,
            source_name=m.source_name,
            output_cols=m.output_cols,
            under_construction=m.under_construction,
        )
        model_map[m.id] = new_m
        if m.uuid is not None:
            uuid_map[str(m.uuid)] = str(new_m.uuid)

    for op in OrgDbtOperation.objects.filter(dbtmodel__orgdbt=template_dbt):
        OrgDbtOperation.objects.create(
            dbtmodel=model_map[op.dbtmodel_id],
            uuid=uuid.uuid4(),
            seq=op.seq,
            output_cols=op.output_cols,
            config=_remap_operation_config(op.config, uuid_map),
        )

    # Filter by from_node only, then guard the to_node lookup: a cross-orgdbt edge (producible
    # by `github_to_ui4t.py`'s org-unscoped model lookups) must fail loud, not KeyError or be
    # silently half-copied.
    for e in DbtEdge.objects.filter(from_node__orgdbt=template_dbt):
        if e.to_node_id not in model_map:
            raise TrialCloneError(f"dbt edge {e.id} points at a model outside the template dbt")
        DbtEdge.objects.create(
            from_node=model_map[e.from_node_id],
            to_node=model_map[e.to_node_id],
        )

    stray_edge = (
        DbtEdge.objects.filter(to_node__orgdbt=template_dbt)
        .exclude(from_node__orgdbt=template_dbt)
        .first()
    )
    if stray_edge is not None:
        raise TrialCloneError(
            f"dbt edge {stray_edge.id} comes from a model outside the template dbt"
        )

    _copy_canvas(template_dbt, trial_dbt, model_map)

    logger.info(
        f"copied dbt dag: {len(model_map)} models from orgdbt={template_dbt.id} to orgdbt={trial_dbt.id}"
    )
    return model_map
