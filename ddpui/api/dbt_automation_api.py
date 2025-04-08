import uuid
import shutil
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

from ninja import Router
from ninja.errors import HttpError

from django.db import transaction
from django.db.models import Q
from django.utils.text import slugify

from ddpui import auth
from ddpui.ddpdbt.dbt_service import setup_local_dbt_workspace
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgDbt, OrgWarehouse, TransformType
from ddpui.models.dbt_automation import (
    OrgDbtModelv1,
    OrgDbtOperationv1,
    DbtEdgev1,
    DbtNode,
)
from ddpui.models.canvaslock import CanvasLock

from ddpui.schemas.org_task_schema import DbtProjectSchema

from ddpui.schemas.dbt_automation_schema import (
    CreateDbtModelNodePayload,
    ChainOperationPayload,
    EdgeConfig,
)
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.utils.taskprogress import TaskProgress
from ddpui.core.transformfunctions import validate_operation_config, check_canvas_locked
from ddpui.api.warehouse_api import get_warehouse_data
from ddpui.models.tasks import TaskProgressHashPrefix

from ddpui.core import dbtautomation_service
from ddpui.core.dbtautomation_service import sync_sources_for_warehouse
from ddpui.auth import has_permission

from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.transform_workflow_helpers import (
    from_orgdbtoperation,
)

dbtautomation_router = Router()
load_dotenv()
logger = CustomLogger("ddpui")


@dbtautomation_router.post("/dbtmodel/{model_uuid}/node/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_create_dbt_model"])
def post_create_dbt_model_node(request, model_uuid: str, payload: CreateDbtModelNodePayload):
    """Move a dbt model to the canvas by creating a DbtNode"""

    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    # make sure the orgdbt here is the one we create locally
    orgdbt = OrgDbt.objects.filter(org=org, gitrepo_url=TransformType.UI).first()
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    check_canvas_locked(orguser, payload.canvas_lock_id)

    # create a DbtNode pointing to the dbt model
    dbtmodel = OrgDbtModelv1.objects.filter(uuid=model_uuid).first()

    if not dbtmodel:
        raise HttpError(404, "dbt model not found")

    DbtNode.objects.create(
        orgdbt=orgdbt,
        uuid=uuid.uuid4(),
        dbtmodel=dbtmodel,
    )


@dbtautomation_router.post("/node/{node_uuid}/chain/operation/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_create_dbt_model"])
def post_chain_operation_node(request, node_uuid: str, payload: ChainOperationPayload):
    """Chain a new operation node on an existing node"""

    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    # make sure the orgdbt here is the one we create locally
    orgdbt = OrgDbt.objects.filter(org=org, gitrepo_url=TransformType.UI).first()
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    check_canvas_locked(orguser, payload.canvas_lock_id)

    curr_node = DbtNode.objects.filter(uuid=node_uuid).first()

    if not curr_node:
        raise HttpError(404, "node not found")

    # TODO: run all all operations upto this one & figure out the output columns
    computed_output_cols = []

    with transaction.atomic():
        # create a new DbtNode for the operation
        dbtoperation = OrgDbtOperationv1.objects.create(
            uuid=uuid.uuid4(),
            op_type=payload.op_type,
            config=payload.config,
            output_cols=computed_output_cols,
        )

        op_node = DbtNode.objects.create(
            orgdbt=orgdbt,
            uuid=uuid.uuid4(),
            dbtoperation=dbtoperation,
        )

        # create an edge
        DbtEdgev1.objects.create(
            uuid=uuid.uuid4(),
            from_node=curr_node,
            to_node=op_node,
        )


@dbtautomation_router.get("/graph/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_dbt_workspace"])
def get_dbt_project_DAG(request):
    """Get the project DAG i.e. nodes and edges"""

    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    # make sure the orgdbt here is the one we create locally
    orgdbt = OrgDbt.objects.filter(org=org, gitrepo_url=None).first()
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    # Fetch all nodes and edges
    nodes = DbtNode.objects.filter(orgdbt=orgdbt).select_related("dbtoperation", "dbtmodel")
    edges = DbtEdgev1.objects.filter(
        Q(from_node__orgdbt=orgdbt) | Q(to_node__orgdbt=orgdbt)
    ).select_related("from_node", "to_node")

    # Convert nodes to the desired format
    # TODO: we might need to change this because of what frontend needs
    node_list = [
        {
            "id": node.uuid,
            "dbtoperation": (
                {
                    "id": node.dbtoperation.uuid,
                    "op_type": node.dbtoperation.op_type,
                    "config": node.dbtoperation.config,
                    "output_cols": node.dbtoperation.output_cols,
                }
                if node.dbtoperation
                else None
            ),
            "dbtmodel": (
                {
                    "id": node.dbtmodel.uuid,
                    "name": node.dbtmodel.name,
                    "schema": node.dbtmodel.schema,
                    "sql_path": node.dbtmodel.sql_path,
                }
                if node.dbtmodel
                else None
            ),
        }
        for node in nodes
    ]

    # Convert edges to the desired format
    edge_list = [
        {
            "id": edge.uuid,
            "source": edge.from_node.uuid,
            "target": edge.to_node.uuid,
        }
        for edge in edges
    ]

    # Return the DAG as a dictionary
    return {
        "nodes": node_list,
        "edges": edge_list,
    }


@dbtautomation_router.delete("/node/{node_uuid}/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_create_dbt_model"])
def post_chain_operation_node(request, node_uuid: str, canvas_lock_id: str = None):
    """Delete a node (model or operation) and its edges"""

    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    # make sure the orgdbt here is the one we create locally
    orgdbt = OrgDbt.objects.filter(org=org, gitrepo_url=None).first()
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    check_canvas_locked(orguser, canvas_lock_id)

    curr_node = DbtNode.objects.filter(uuid=node_uuid).first()

    if not curr_node:
        raise HttpError(404, "node not found")

    # TODO: delete the stuff from dbt project on the disk

    with transaction.atomic():
        # delete the edges
        DbtEdgev1.objects.filter(Q(from_node=curr_node) | Q(to_node=curr_node)).delete()

        # delete the node
        curr_node.delete()
