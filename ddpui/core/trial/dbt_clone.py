"""Deep-copy the template org's dbt transform DAG onto a freshly-created trial OrgDbt.

Copies BOTH the legacy row-model path (OrgDbtModel/OrgDbtOperation/DbtEdge) and the active v2
canvas path (CanvasNode/CanvasEdge) — the two systems coexist today (canvas_models.py:1-3), so a
clone that only copied one would leave the trial org's transform UI half-populated.

`copy_dbt_dag` copies the DB rows with `sql_path=None`; `regenerate_and_push` then materializes
the trial repo's CONTENT from those copied rows — `sources.yml` for every SOURCE row and a
`.sql` per MODEL row, regenerated from the copied operation chains (never by reading the
template's repo) — and pushes it to the trial's managed repo. Without that content, the copied
canvas is view-only: any new model built on a copied source/model fails dbt compilation
("depends on a source/model ... which was not found") and `dbt run` has nothing to build.
UI4T-built templates only; git-imported templates (no operation chains) raise.

`uuid` is never copied from the template row — every copy mints its own via `uuid.uuid4()`,
matching how the rest of the codebase creates these rows (see
`ddpui/core/dbtautomation_service.py`, `ddpui/api/transform_api.py`,
`ddpui/management/commands/github_to_ui4t.py`) — `OrgDbtOperation.uuid` in particular is a
NOT NULL unique field with no model-level default, so it must always be supplied explicitly.
"""

import copy
import uuid

from ddpui.core.dbtautomation_service import (
    create_or_update_dbt_model_in_project_v2,
    ensure_source_yml_definition_in_project,
    tranverse_graph_and_return_operations_list,
)
from ddpui.core.git_manager import GitManager
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.core.trial.exceptions import TrialCloneError
from ddpui.models.canvas_models import CanvasEdge, CanvasNode, CanvasNodeType
from ddpui.models.dbt_workflow import DbtEdge, OrgDbtModel, OrgDbtModelType, OrgDbtOperation
from ddpui.models.org import Org, OrgDbt, OrgWarehouse
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


def _topological_model_order(trial_dbt: OrgDbt) -> list[OrgDbtModel]:
    """Kahn's-algorithm topological order of `trial_dbt`'s OrgDbtModel rows (SOURCE and MODEL),
    derived from the copied `DbtEdge` graph, filtered down to the MODEL-type rows (SOURCE rows
    need no `.sql` regeneration — `sources.yml` handles those separately).

    Any row a `DbtEdge` never touches (no recorded dependency) is appended at the end so nothing
    is silently skipped."""
    all_models = list(OrgDbtModel.objects.filter(orgdbt=trial_dbt))
    by_id = {m.id: m for m in all_models}
    edges = list(
        DbtEdge.objects.filter(from_node__orgdbt=trial_dbt).select_related("from_node", "to_node")
    )

    indegree = {m.id: 0 for m in all_models}
    adjacency: dict = {m.id: [] for m in all_models}
    for e in edges:
        if e.from_node_id in adjacency and e.to_node_id in indegree:
            adjacency[e.from_node_id].append(e.to_node_id)
            indegree[e.to_node_id] += 1

    queue = [model_id for model_id, deg in indegree.items() if deg == 0]
    ordered_ids: list = []
    while queue:
        node_id = queue.pop(0)
        ordered_ids.append(node_id)
        for neighbour in adjacency[node_id]:
            indegree[neighbour] -= 1
            if indegree[neighbour] == 0:
                queue.append(neighbour)

    # DbtEdge cycles should never happen, but never silently drop a model if one does.
    for model_id in indegree:
        if model_id not in ordered_ids:
            ordered_ids.append(model_id)

    return [by_id[mid] for mid in ordered_ids if by_id[mid].type == OrgDbtModelType.MODEL]


def _regenerate_source(model: OrgDbtModel, trial_dbt: OrgDbt) -> None:
    """Materialize `sources.yml` for a copied SOURCE-type OrgDbtModel and set its `sql_path`
    from the return — mirrors `transform_api.py`'s `post_create_src_model_node` handling of
    SOURCE nodes (the only other caller of `ensure_source_yml_definition_in_project`)."""
    source_yml_def = ensure_source_yml_definition_in_project(trial_dbt, model.schema, model.name)
    model.sql_path = source_yml_def.sql_path
    model.source_name = source_yml_def.source_name
    model.name = source_yml_def.table
    model.schema = source_yml_def.source_schema
    model.save()


def _terminal_operation_node(trial_dbt: OrgDbt, model: OrgDbtModel) -> CanvasNode:
    """The OPERATION CanvasNode feeding the given MODEL-type OrgDbtModel's CanvasNode. The v2
    regen path (`create_or_update_dbt_model_in_project_v2`) always builds a model's SQL from an
    upstream operation chain — every MODEL row this codebase creates is the result of
    terminating one (see `transform_api.py:post_terminate_operation_node`), so a copied MODEL
    with no such upstream chain indicates a DAG the clone cannot regenerate and must fail loud
    on rather than silently skip.

    KNOWN LIMITATION: this whole regeneration path assumes every template MODEL was built via
    the UI operation-builder, i.e. each MODEL canvas node is terminated from an OPERATION chain.
    Templates whose dbt was git-imported (`parse_dbt_manifest_to_canvas` — MODEL nodes fed
    directly by SOURCE/MODEL edges with no OPERATION node) are NOT supported here and will raise
    below. A trial TEMPLATE org must therefore have a UI-operation-built dbt project.

    A MODEL canvas node may have more than one incoming CanvasEdge (e.g. the model was
    re-terminated from a different operation chain during editing, leaving a stale edge behind).
    Picking an arbitrary one (e.g. via an unordered `.first()`) risks silently regenerating from
    the wrong (stale) operation chain, so ambiguity is treated as a hard failure instead of a
    guess."""
    model_node = CanvasNode.objects.filter(
        orgdbt=trial_dbt, dbtmodel=model, node_type=CanvasNodeType.MODEL
    ).first()
    if model_node is None:
        raise TrialCloneError(f"no canvas node found for copied model {model.name}")

    incoming = list(CanvasEdge.objects.filter(to_node=model_node).select_related("from_node"))
    if len(incoming) == 0:
        raise TrialCloneError(
            f"copied model {model.name} has no upstream operation chain to regenerate from"
        )
    if len(incoming) > 1:
        raise TrialCloneError(
            f"model canvas node {model_node.uuid} has {len(incoming)} incoming operation "
            "edges; ambiguous terminal node — cannot safely regenerate"
        )

    from_node = incoming[0].from_node
    if from_node.node_type != CanvasNodeType.OPERATION:
        raise TrialCloneError(
            f"copied model {model.name} has no upstream operation chain to regenerate from"
        )
    return from_node


def _regenerate_model(model: OrgDbtModel, trial_dbt: OrgDbt, org_warehouse: OrgWarehouse) -> None:
    """Regenerate a copied MODEL-type OrgDbtModel's `.sql` from its copied operation chain and
    set its `sql_path` from the return — mirrors `transform_api.py:post_terminate_operation_node`'s
    call shape (`operations`/`dest_schema`/`output_name`/`rel_dir_to_models` config keys, as
    consumed by `dbt_automation.operations.mergeoperations.merge_operations_v2`)."""
    terminal_op_node = _terminal_operation_node(trial_dbt, model)
    operations_list = tranverse_graph_and_return_operations_list(terminal_op_node)
    config = {
        "operations": operations_list,
        "dest_schema": model.schema,
        "output_name": model.name,
        "rel_dir_to_models": None,
    }
    model_sql_path, _output_cols = create_or_update_dbt_model_in_project_v2(
        org_warehouse, config, trial_dbt
    )
    model.sql_path = str(model_sql_path)
    model.save()


def regenerate_and_push(trial_org: Org, trial_dbt: OrgDbt) -> int:
    """Regenerate every `.sql` model + `sources.yml` on the trial dbt project (an empty scaffold
    at this point — `copy_dbt_dag` deliberately leaves `sql_path=None`), then commit + push the
    result to the trial's Dalgo-managed GitHub repo.

    Uses the org-admin PAT (`GitManager.get_org_admin_pat()`) for the push, matching how
    `setup_managed_git_workspace` authenticates all git operations against Dalgo-managed repos
    (as opposed to a per-org secret, which managed repos don't store).

    KNOWN LIMITATION: model regeneration (`_regenerate_model` -> `_terminal_operation_node`)
    assumes every template MODEL was built via the UI operation-builder, i.e. each MODEL canvas
    node is terminated from an OPERATION chain. Templates whose dbt was git-imported
    (`parse_dbt_manifest_to_canvas` — MODEL nodes fed directly by SOURCE/MODEL edges with no
    OPERATION node) are NOT supported by this path and will raise. A trial TEMPLATE org must
    therefore have a UI-operation-built dbt project; the git-import clone path is not implemented.

    Returns the number of MODEL-type OrgDbtModel rows regenerated.
    """
    org_warehouse = OrgWarehouse.objects.filter(org=trial_org).first()
    if org_warehouse is None:
        raise TrialCloneError(
            f"trial org {trial_org.slug} has no warehouse to regenerate dbt models against"
        )

    for source_model in OrgDbtModel.objects.filter(orgdbt=trial_dbt, type=OrgDbtModelType.SOURCE):
        _regenerate_source(source_model, trial_dbt)

    ordered_models = _topological_model_order(trial_dbt)
    for model in ordered_models:
        _regenerate_model(model, trial_dbt, org_warehouse)

    repo_dir = DbtProjectManager.get_dbt_project_dir(trial_dbt)
    pat = GitManager.get_org_admin_pat()
    git_manager = GitManager(repo_dir, pat)
    git_manager.commit_changes("clone template dbt models")
    git_manager.push_changes()

    logger.info(
        f"regenerated {len(ordered_models)} dbt models and pushed to {trial_dbt.gitrepo_url}"
    )
    return len(ordered_models)
