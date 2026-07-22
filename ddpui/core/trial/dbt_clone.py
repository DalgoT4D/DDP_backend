"""Deep-copy the template org's dbt transform DAG onto a freshly-created trial OrgDbt.

Copies BOTH the legacy row-model path (OrgDbtModel/OrgDbtOperation/DbtEdge) and the active v2
canvas path (CanvasNode/CanvasEdge) — the two systems coexist today (canvas_models.py:1-3), so a
clone that only copied one would leave the trial org's transform UI half-populated.

`sql_path` is intentionally left None on every copied OrgDbtModel: the trial's dbt project is an
empty scaffold at this point (created by `setup_managed_git_workspace`, called by `_step_dbt`
before this runs) — Task P4.2 regenerates every `.sql` file and sets `sql_path` from that regen's
return value. `uuid` is never copied from the template row — every copy mints its own via
`uuid.uuid4()`, matching how the rest of the codebase creates these rows (see
`ddpui/core/dbtautomation_service.py`, `ddpui/api/transform_api.py`,
`ddpui/management/commands/github_to_ui4t.py`) — `OrgDbtOperation.uuid` in particular is a
NOT NULL unique field with no model-level default, so it must always be supplied explicitly.
"""

import copy
import shutil
import tempfile
import uuid
from pathlib import Path

import yaml

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
from ddpui.utils.secretsmanager import retrieve_github_pat

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
        CanvasEdge.objects.create(
            from_node=node_map[edge.from_node_id],
            to_node=node_map[edge.to_node_id],
            seq=edge.seq,
        )


def _remap_operation_config(config, uuid_map: dict):
    """Deep-copy `config` (an `OrgDbtOperation.config`) and, for seq==1 operations whose config
    is a dict with an `input_models` list, remap each entry's `uuid` (a reference to a parent
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


def copy_dbt_dag(template_dbt: OrgDbt, trial_dbt: OrgDbt, preserve_sql_path: bool = False) -> dict:
    """Deep-copy every OrgDbtModel/OrgDbtOperation/DbtEdge (legacy) AND CanvasNode/CanvasEdge
    (active v2) row from `template_dbt` onto `trial_dbt`, re-parenting FKs to the new rows via
    an old-model-id -> new-model old->new map.

    `preserve_sql_path` controls what happens to each copied OrgDbtModel's `sql_path`:
    - False (default, the UI-operation-built/regen path): `sql_path` is nulled — the trial's
      dbt project is an empty scaffold at this point, and the later `regenerate_and_push` step
      regenerates every `.sql` file from the copied operation chains and sets `sql_path` itself.
    - True (the github/file-based/copy path, e.g. `health_org`-style templates with zero
      OrgDbtOperation rows): `sql_path` is copied AS-IS — the caller (`copy_dbt_repo_files`)
      copies the actual `.sql` files to those same project-relative paths in the trial repo, so
      the inherited `sql_path` stays valid without any regeneration.

    Returns `model_map` (`{old OrgDbtModel.id: new OrgDbtModel}`) so callers (e.g. the later
    `.sql` regeneration step) can walk the copied DAG.
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
            sql_path=m.sql_path if preserve_sql_path else None,
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

    for e in DbtEdge.objects.filter(from_node__orgdbt=template_dbt):
        DbtEdge.objects.create(
            from_node=model_map[e.from_node_id],
            to_node=model_map[e.to_node_id],
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


# template repo directories copied wholesale onto the trial repo (merged over the scaffold,
# template files win on collision). `models/` is required — the rest are copied only if present.
_TEMPLATE_REPO_DIRS = ("models", "macros", "seeds", "snapshots", "tests")
# template repo top-level files copied over the scaffold's versions if present. packages.yml:
# the scaffold ships the Dalgo-assets version (dbt_utils only) — a template pinning a different
# dbt_utils or adding packages (codegen, …) must carry its own.
_TEMPLATE_REPO_FILES = ("packages.yml",)


def _merge_template_project_config(template_repo_dir: Path, trial_repo_dir: Path) -> None:
    """Carry the TEMPLATE's `dbt_project.yml` config onto the trial repo, re-keyed to the trial's
    scaffold project identity.

    The scaffold's `dbt_project.yml` is stock `dbt init` output — any template folder-level
    config (`models: {..., +materialized/+schema}`, `vars:`, `seeds:`, `on-run-start/end` hooks)
    would silently be LOST and the trial's dbt builds would materialize differently (e.g. every
    un-inline-configured model falling back to `view`). So the template's file is taken as the
    base, with the scaffold's own `name:`/`profile:` preserved (the trial project is always
    `dbtrepo` and its profile must keep matching the cli-profile block `setup_managed_git_workspace`
    created) — and the per-resource config sections re-keyed from the template's project name to
    the scaffold's, since dbt scopes those sections by project name.

    A template repo without a `dbt_project.yml` (not a valid dbt project, but conceivable for a
    bare models dump) keeps the scaffold's file untouched.
    """
    template_yml = template_repo_dir / "dbt_project.yml"
    trial_yml = trial_repo_dir / "dbt_project.yml"
    if not template_yml.exists():
        logger.warning(f"template repo has no dbt_project.yml; trial keeps the scaffold default")
        return

    template_cfg = yaml.safe_load(template_yml.read_text(encoding="utf-8")) or {}
    scaffold_cfg = yaml.safe_load(trial_yml.read_text(encoding="utf-8")) or {}

    template_name = template_cfg.get("name")
    scaffold_name = scaffold_cfg.get("name")
    template_cfg["name"] = scaffold_name
    template_cfg["profile"] = scaffold_cfg.get("profile")
    if template_name and scaffold_name and template_name != scaffold_name:
        for section_key in ("models", "seeds", "snapshots", "tests"):
            section = template_cfg.get(section_key)
            if isinstance(section, dict) and template_name in section:
                section[scaffold_name] = section.pop(template_name)

    trial_yml.write_text(yaml.safe_dump(template_cfg, sort_keys=False), encoding="utf-8")
    logger.info("merged template dbt_project.yml config onto trial repo")


def copy_dbt_repo_files(template_dbt: OrgDbt, trial_dbt: OrgDbt) -> None:
    """Copy the template's dbt project files directly from ITS git repo into the trial's
    freshly-created managed repo, for templates with ZERO OrgDbtOperation rows — i.e.
    github/file-based dbt projects (e.g. `health_org`) whose canvas is parsed straight from the
    repo rather than built via the UI operation-builder. `regenerate_and_push` cannot handle
    these (there's no operation chain to regenerate `.sql` from), so this mirrors the existing
    "copy Dalgo managed models to external repository" flow
    (`dbt_service.switch_git_repository_v1`) instead: clone the source repo, copy its project
    files onto the destination working dir, commit + push.

    What gets copied (over the `setup_managed_git_workspace` scaffold, template files winning):
    - directories: `models/` (required) plus `macros/`, `seeds/`, `snapshots/`, `tests/` when
      present — a template model calling its own macro, seeding a lookup table, or shipping
      singular tests would otherwise fail/degrade silently on the trial;
    - files: `packages.yml` when present (template's pinned deps win over the scaffold's
      asset copy);
    - `dbt_project.yml` config, re-keyed to the scaffold's project name/profile — see
      `_merge_template_project_config` (folder-level +materialized/+schema/vars/hooks survive).

    Auth for cloning the TEMPLATE repo depends on how it's hosted:
    - Dalgo-managed (`is_repo_managed_by_system=True`): the org-admin PAT (managed repos don't
      store a per-org secret — see `setup_managed_git_workspace`).
    - External (`is_repo_managed_by_system=False`, e.g. `health_org`'s own repo): the org's
      stored PAT via `retrieve_github_pat(template_dbt.gitrepo_access_token_secret)`.

    The TRIAL repo (always Dalgo-managed — `setup_managed_git_workspace` created it just before
    this runs) is always pushed with the org-admin PAT, matching `regenerate_and_push`.

    The template repo is cloned into a throwaway temp directory (never under `CLIENTDBT_ROOT` —
    it's only a source of files here, not a live client project) which is removed once the copy
    is done.
    """
    if not template_dbt.gitrepo_url:
        raise TrialCloneError(
            f"template dbt (orgdbt={template_dbt.id}) has no gitrepo_url to clone"
        )

    template_pat = (
        GitManager.get_org_admin_pat()
        if template_dbt.is_repo_managed_by_system
        else retrieve_github_pat(template_dbt.gitrepo_access_token_secret)
    )

    trial_repo_dir = Path(DbtProjectManager.get_dbt_project_dir(trial_dbt))

    with tempfile.TemporaryDirectory() as tmp_cwd:
        template_clone = GitManager.clone(
            cwd=tmp_cwd,
            remote_repo_url=template_dbt.gitrepo_url,
            relative_path="template_dbtrepo",
            pat=template_pat,
        )
        template_repo_dir = Path(template_clone.repo_local_path)
        template_models_dir = template_repo_dir / "models"
        if not template_models_dir.exists():
            # copy_dbt_dag has already created trial OrgDbtModel rows with sql_path set — if we
            # copy no .sql files, the trial dbt project is half-populated (DAG metadata but no
            # backing files). Fail loud rather than ship a broken transform layer.
            raise TrialCloneError(
                f"template dbt repo {template_dbt.gitrepo_url} has no models/ directory to copy"
            )
        for dirname in _TEMPLATE_REPO_DIRS:
            src_dir = template_repo_dir / dirname
            if src_dir.exists():
                shutil.copytree(src_dir, trial_repo_dir / dirname, dirs_exist_ok=True)
        for filename in _TEMPLATE_REPO_FILES:
            src_file = template_repo_dir / filename
            if src_file.exists():
                shutil.copy(src_file, trial_repo_dir / filename)
        _merge_template_project_config(template_repo_dir, trial_repo_dir)

    trial_pat = GitManager.get_org_admin_pat()
    git_manager = GitManager(repo_local_path=str(trial_repo_dir), pat=trial_pat)
    git_manager.commit_changes("clone template dbt models from git")
    git_manager.push_changes()

    logger.info(f"copied dbt repo files from {template_dbt.gitrepo_url} to {trial_dbt.gitrepo_url}")
