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

import uuid

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
        new_node = CanvasNode.objects.create(
            orgdbt=trial_dbt,
            uuid=uuid.uuid4(),
            node_type=node.node_type,
            name=node.name,
            operation_config=node.operation_config,
            output_cols=node.output_cols,
            dbtmodel=model_map[node.dbtmodel_id] if node.dbtmodel_id else None,
        )
        node_map[node.id] = new_node

    for edge in CanvasEdge.objects.filter(from_node__orgdbt=template_dbt):
        CanvasEdge.objects.create(
            from_node=node_map[edge.from_node_id],
            to_node=node_map[edge.to_node_id],
            seq=edge.seq,
        )


def copy_dbt_dag(template_dbt: OrgDbt, trial_dbt: OrgDbt) -> dict:
    """Deep-copy every OrgDbtModel/OrgDbtOperation/DbtEdge (legacy) AND CanvasNode/CanvasEdge
    (active v2) row from `template_dbt` onto `trial_dbt`, re-parenting FKs to the new rows via
    an old-model-id -> new-model old->new map.

    Returns `model_map` (`{old OrgDbtModel.id: new OrgDbtModel}`) so callers (e.g. the later
    `.sql` regeneration step) can walk the copied DAG.
    """
    model_map: dict = {}
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

    for op in OrgDbtOperation.objects.filter(dbtmodel__orgdbt=template_dbt):
        OrgDbtOperation.objects.create(
            dbtmodel=model_map[op.dbtmodel_id],
            uuid=uuid.uuid4(),
            seq=op.seq,
            output_cols=op.output_cols,
            config=op.config,
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
