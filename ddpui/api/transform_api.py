import uuid
import shutil
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

from ninja import Router
from ninja.errors import HttpError

from django.db.models import Q
from django.utils.text import slugify
from django.db import transaction
from django.forms import model_to_dict

from ddpui.datainsights import warehouse
from ddpui.ddpdbt.dbt_service import setup_local_dbt_workspace
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgDbt, OrgWarehouse, TransformType
from ddpui.models.dbt_workflow import OrgDbtModel, DbtEdge, OrgDbtOperation, OrgDbtModelType
from ddpui.models.canvaslock import CanvasLock
from ddpui.models.canvas_models import CanvasNode, CanvasEdge, CanvasNodeType

from ddpui.schemas.org_task_schema import DbtProjectSchema
from ddpui.schemas.dbt_workflow_schema import (
    CreateDbtModelPayload,
    CompleteDbtModelPayload,
    CreateOperationNodePayload,
    EditDbtOperationPayload,
    LockCanvasRequestSchema,
    LockCanvasResponseSchema,
    EditOperationNodePayload,
    ModelSrcInputsForMultiInputOp,
    validate_operation_config_v2,
    TerminateChainAndCreateModelPayload,
)
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.utils.taskprogress import TaskProgress
from ddpui.core.transformfunctions import validate_operation_config, check_canvas_locked
from ddpui.api.warehouse_api import get_warehouse_data
from ddpui.models.tasks import TaskProgressHashPrefix

from ddpui.core import dbtautomation_service
from ddpui.core.dbtautomation_service import (
    create_or_update_dbt_model_in_project_v2,
    sync_sources_for_warehouse,
    convert_canvas_node_to_frontend_format,
    tranverse_graph_and_return_operations_list,
    validate_and_return_inputs_for_multi_input_op,
)
from ddpui.auth import has_permission

from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.transform_workflow_helpers import (
    from_orgdbtoperation,
    from_orgdbtmodel,
)

transform_router = Router()
load_dotenv()
logger = CustomLogger("ddpui")


@transform_router.post("/dbt_project/")
@has_permission(["can_create_dbt_workspace"])
def create_dbt_project(request, payload: DbtProjectSchema):
    """
    Create a new dbt project.
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_dir = Path(DbtProjectManager.get_org_dir(org))
    org_dir.mkdir(parents=True, exist_ok=True)

    # Call the post_dbt_workspace function
    _, error = setup_local_dbt_workspace(
        org, project_name="dbtrepo", default_schema=payload.default_schema
    )
    if error:
        raise HttpError(422, error)

    return {"message": f"Project {org.slug} created successfully"}


@transform_router.delete("/dbt_project/{project_name}")
@has_permission(["can_delete_dbt_workspace"])
def delete_dbt_project(request, project_name: str):
    """
    Delete a dbt project in this org
    """
    orguser: OrgUser = request.orguser
    org = orguser.org
    orgdbt = org.dbt

    org_dir = Path(DbtProjectManager.get_org_dir(org))

    if not org_dir.exists():
        raise HttpError(404, f"Organization {org.slug} does not have any projects")

    project_dir: Path = org_dir / project_name

    if not project_dir.exists():
        raise HttpError(422, f"Project {project_name} does not exist in organization {org.slug}")

    if orgdbt:
        org.dbt = None
        org.save()

        orgdbt.delete()

    shutil.rmtree(project_dir)

    return {"message": f"Project {project_name} deleted successfully"}


@transform_router.post("/dbt_project/sync_sources/")
@has_permission(["can_sync_sources"])
def sync_sources(request):
    """
    Sync sources from a given schema.
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    orgdbt = OrgDbt.objects.filter(org=org, gitrepo_url=None).first()
    if not orgdbt:
        raise HttpError(404, "DBT workspace not set up")

    task_id = str(uuid.uuid4())
    hashkey = f"{TaskProgressHashPrefix.SYNCSOURCES.value}-{org.slug}"

    taskprogress = TaskProgress(
        task_id=task_id,
        hashkey=hashkey,
        expire_in_seconds=10 * 60,  # max 10 minutes)
    )
    taskprogress.add(
        {
            "message": "Started syncing sources",
            "status": "runnning",
        }
    )

    sync_sources_for_warehouse.delay(orgdbt.id, org_warehouse.id, task_id, hashkey)

    return {"task_id": task_id, "hashkey": hashkey}


########################## Models & Sources #############################################


@transform_router.post("/dbt_project/model/")
@has_permission(["can_create_dbt_model"])
def post_construct_dbt_model_operation(request, payload: CreateDbtModelPayload):
    """
    Construct a model or chain operations on a under construction target model
    """

    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    # make sure the orgdbt here is the one we create locally
    orgdbt = OrgDbt.objects.filter(org=org, gitrepo_url=None).first()
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    check_canvas_locked(orguser, payload.canvas_lock_id)

    if payload.op_type not in dbtautomation_service.OPERATIONS_DICT.keys():
        raise HttpError(422, "Operation not supported")

    is_multi_input_op = payload.op_type in ["join", "unionall"]

    target_model = None
    if payload.target_model_uuid:
        target_model = OrgDbtModel.objects.filter(uuid=payload.target_model_uuid).first()

    if not target_model:
        target_model = OrgDbtModel.objects.create(
            uuid=uuid.uuid4(),
            orgdbt=orgdbt,
            under_construction=True,
        )

    # only under construction models can be modified
    if not target_model.under_construction:
        raise HttpError(422, "model is locked")

    current_operations_chained = OrgDbtOperation.objects.filter(dbtmodel=target_model).count()

    final_config, all_input_models = validate_operation_config(
        payload, target_model, is_multi_input_op, current_operations_chained
    )

    # we create edges only with tables/models
    for source in all_input_models:
        edge = DbtEdge.objects.filter(from_node=source, to_node=target_model).first()
        if not edge:
            DbtEdge.objects.create(
                from_node=source,
                to_node=target_model,
            )

    output_cols = dbtautomation_service.get_output_cols_for_operation(
        org_warehouse,
        payload.op_type,
        final_config["config"].copy(),
    )
    logger.info("creating operation")

    dbt_op = OrgDbtOperation.objects.create(
        dbtmodel=target_model,
        uuid=uuid.uuid4(),
        seq=current_operations_chained + 1,
        config=final_config,
        output_cols=output_cols,
    )

    logger.info("created operation")

    # save the output cols of the latest operation to the dbt model
    target_model.output_cols = dbt_op.output_cols
    target_model.save()

    logger.info("updated output cols for the model")

    return from_orgdbtoperation(dbt_op, chain_length=dbt_op.seq)


@transform_router.put("/dbt_project/model/operations/{operation_uuid}/")
@has_permission(["can_edit_dbt_operation"])
def put_operation(request, operation_uuid: str, payload: EditDbtOperationPayload):
    """
    Update operation config
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    is_multi_input_op = payload.op_type in ["join", "unionall"]

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    # make sure the orgdbt here is the one we create locally
    orgdbt = OrgDbt.objects.filter(org=org, gitrepo_url=None).first()
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    check_canvas_locked(orguser, payload.canvas_lock_id)

    try:
        uuid.UUID(str(operation_uuid))
    except ValueError:
        raise HttpError(400, "operation not found")

    dbt_operation = OrgDbtOperation.objects.filter(uuid=operation_uuid).first()
    if not dbt_operation:
        raise HttpError(404, "operation not found")

    # if dbt_operation.dbtmodel.under_construction is False:
    #     raise HttpError(403, "model is locked")

    # allow edit of only leaf operation nodes - disabled for now
    # if (
    #     OrgDbtOperation.objects.filter(
    #         dbtmodel=dbt_operation.dbtmodel, seq__gt=dbt_operation.seq
    #     ).count()
    #     >= 1
    # ):
    #     raise HttpError(403, "operation is locked; cannot edit")

    target_model = dbt_operation.dbtmodel

    all_ops = OrgDbtOperation.objects.filter(dbtmodel=target_model).all()
    operation_chained_before = sum(1 for op in all_ops if op.seq < dbt_operation.seq)

    final_config, all_input_models = validate_operation_config(
        payload, target_model, is_multi_input_op, operation_chained_before, edit=True
    )

    # create edges only with tables/models if not present
    for source in all_input_models:
        edge = DbtEdge.objects.filter(from_node=source, to_node=target_model).first()
        if not edge:
            DbtEdge.objects.create(
                from_node=source,
                to_node=target_model,
            )

    output_cols = dbtautomation_service.get_output_cols_for_operation(
        org_warehouse, payload.op_type, final_config["config"].copy()
    )

    dbt_operation.config = final_config
    dbt_operation.output_cols = output_cols
    dbt_operation.save()

    logger.info("updated operation")

    # save the output cols of the latest operation to the dbt model
    target_model.output_cols = dbt_operation.output_cols
    target_model.save()

    if not target_model.under_construction:
        dbtautomation_service.update_dbt_model_in_project(org_warehouse, target_model)

    # propogate the udpates down the chain
    dbtautomation_service.propagate_changes_to_downstream_operations(
        target_model, dbt_operation, depth=1
    )

    logger.info("updated output cols for the target model")

    return from_orgdbtoperation(dbt_operation, chain_length=len(all_ops))


@transform_router.get("/dbt_project/model/operations/{operation_uuid}/")
@has_permission(["can_view_dbt_operation"])
def get_operation(request, operation_uuid: str):
    """
    Fetch config of operation
    """

    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    # make sure the orgdbt here is the one we create locally
    orgdbt = OrgDbt.objects.filter(org=org, gitrepo_url=None).first()
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    try:
        uuid.UUID(str(operation_uuid))
    except ValueError:
        raise HttpError(400, "operation not found")

    dbt_operation = OrgDbtOperation.objects.filter(uuid=operation_uuid).first()
    if not dbt_operation:
        raise HttpError(404, "operation not found")

    prev_source_columns = []
    if dbt_operation.seq > 1:
        prev_dbt_op = OrgDbtOperation.objects.filter(
            dbtmodel=dbt_operation.dbtmodel, seq=dbt_operation.seq - 1
        ).first()
        prev_source_columns = prev_dbt_op.output_cols
    else:
        config = dbt_operation.config
        if "input_models" in config and len(config["input_models"]) >= 1:
            model = OrgDbtModel.objects.filter(uuid=config["input_models"][0]["uuid"]).first()
            if model:
                for col_data in get_warehouse_data(
                    request,
                    "table_columns",
                    schema_name=model.schema,
                    table_name=model.name,
                ):
                    prev_source_columns.append(col_data["name"])

    return from_orgdbtoperation(dbt_operation, prev_source_columns=prev_source_columns)


@transform_router.post("/dbt_project/model/{model_uuid}/save/")
@has_permission(["can_edit_dbt_model"])
def post_save_model(request, model_uuid: str, payload: CompleteDbtModelPayload):
    """Complete the model; create the dbt model on disk"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    # make sure the orgdbt here is the one we create locally
    orgdbt = OrgDbt.objects.filter(org=org, gitrepo_url=None).first()
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    orgdbt_model = OrgDbtModel.objects.filter(uuid=model_uuid).first()
    if not orgdbt_model:
        raise HttpError(404, "model not found")

    # prevent duplicate models
    if (
        OrgDbtModel.objects.filter(orgdbt=orgdbt, name=payload.name)
        .exclude(uuid=orgdbt_model.uuid)
        .exists()
    ):
        raise HttpError(422, "model with this name already exists")

    # when you are overwriting the existing model with same name but different schema; which again leads to duplicate models
    if (
        payload.name == orgdbt_model.name
        and payload.dest_schema != orgdbt_model.schema
        and not orgdbt_model.under_construction
    ):
        raise HttpError(422, "model with this name already exists in the schema")

    check_canvas_locked(orguser, payload.canvas_lock_id)

    payload.name = slugify(payload.name)

    model_sql_path, output_cols = dbtautomation_service.create_dbt_model_in_project(
        org_warehouse, orgdbt_model, payload
    )

    orgdbt_model.output_cols = output_cols
    orgdbt_model.sql_path = str(model_sql_path)
    orgdbt_model.under_construction = False
    orgdbt_model.name = payload.name
    orgdbt_model.display_name = payload.display_name
    orgdbt_model.schema = payload.dest_schema
    orgdbt_model.save()

    return from_orgdbtmodel(orgdbt_model)


@transform_router.get("/dbt_project/sources_models/")
@has_permission(["can_view_dbt_models"])
def get_input_sources_and_models(request, schema_name: str = None):
    """
    Fetches all sources and models in a dbt project
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    # make sure the orgdbt here is the one we create locally
    orgdbt = OrgDbt.objects.filter(org=org, gitrepo_url=None).first()
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    query = OrgDbtModel.objects.filter(orgdbt=orgdbt)

    if schema_name:
        query = query.filter(schema=schema_name)

    res = []
    for orgdbt_model in query.all():
        if not orgdbt_model.under_construction:
            res.append(from_orgdbtmodel(orgdbt_model))

    return res


@transform_router.get("/dbt_project/graph/")
@has_permission(["can_view_dbt_workspace"])
def get_dbt_project_DAG(request):
    """
    Returns the DAG of the dbt project; including the nodes and edges
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    # make sure the orgdbt here is the one we create locally
    orgdbt = OrgDbt.objects.filter(org=org, gitrepo_url=None).first()
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    model_nodes: list[OrgDbtModel] = []
    operation_nodes: list[OrgDbtOperation] = []
    res_edges = []  # will go directly in the res

    edges = DbtEdge.objects.filter(
        Q(from_node__orgdbt=orgdbt) | Q(to_node__orgdbt=orgdbt)
    ).select_related("from_node", "to_node")

    seen_model_node_ids = set()
    for edge in edges:
        if edge.from_node.id not in seen_model_node_ids:
            model_nodes.append(edge.from_node)
        seen_model_node_ids.add(edge.from_node.id)
        if edge.to_node.id not in seen_model_node_ids:
            model_nodes.append(edge.to_node)
        seen_model_node_ids.add(edge.to_node.id)

    all_operations = OrgDbtOperation.objects.filter(dbtmodel_id__in=list(seen_model_node_ids)).all()

    # fetch all the source nodes that can be in the operation.config["input_models"]
    uuids = []
    for operation in all_operations:
        if "input_models" in operation.config and len(operation.config["input_models"]) > 0:
            uuids.extend([model["uuid"] for model in operation.config["input_models"]])
    op_src_nodes = OrgDbtModel.objects.filter(uuid__in=uuids).all()

    # push operation nodes and edges if any
    for target_node in model_nodes:
        # src_node -> op1 -> op2 -> op3 -> op4
        # start building edges from the source
        prev_op = None
        operations = [op for op in all_operations if op.dbtmodel.id == target_node.id]
        sorted_operations = sorted(operations, key=lambda op: op.seq)
        for operation in sorted_operations:
            operation_nodes.append(operation)
            if "input_models" in operation.config and len(operation.config["input_models"]) > 0:
                input_models = operation.config["input_models"]
                src_uuids = [model["uuid"] for model in input_models]
                # edge(s) between the node(s) and other sources involved that are tables (OrgDbtModel)
                for op_src_node in [
                    src_node for src_node in op_src_nodes if str(src_node.uuid) in src_uuids
                ]:
                    model_nodes.append(op_src_node)
                    res_edges.append(
                        {
                            "id": str(op_src_node.uuid) + "_" + str(operation.uuid),
                            "source": op_src_node.uuid,
                            "target": operation.uuid,
                        }
                    )
            if operation.seq >= 2:
                # for chained operations for seq >= 2
                res_edges.append(
                    {
                        "id": str(prev_op.uuid) + "_" + str(operation.uuid),
                        "source": prev_op.uuid,
                        "target": operation.uuid,
                    }
                )

            prev_op = operation

        # -> op4 -> target_model
        if not target_node.under_construction and prev_op:
            # edge between the last operation and the target model
            res_edges.append(
                {
                    "id": str(prev_op.uuid) + "_" + str(target_node.uuid),
                    "source": prev_op.uuid,
                    "target": target_node.uuid,
                }
            )

    res_nodes = []
    for node in model_nodes:
        if not node.under_construction:
            res_nodes.append(from_orgdbtmodel(node))

    for node in operation_nodes:
        res_nodes.append(from_orgdbtoperation(node))

    # set to remove duplicates
    seen = set()
    res = {}
    res["nodes"] = [nn for nn in res_nodes if not (nn["id"] in seen or seen.add(nn["id"]))]
    seen = set()
    res["edges"] = [edg for edg in res_edges if not (edg["id"] in seen or seen.add(edg["id"]))]

    return res


@transform_router.delete("/dbt_project/model/{model_uuid}/")
@has_permission(["can_delete_dbt_model"])
def delete_model(request, model_uuid, canvas_lock_id: str = None, cascade: bool = False):
    """
    Delete a model if it does not have any operations chained
    Convert the model to "under_construction if its has atleast 1 operation chained"
    Cascade will be implemented when we re-haul the ui4t architecture
    """
    if cascade:
        raise NotImplementedError()

    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    # make sure the orgdbt here is the one we create locally
    orgdbt = OrgDbt.objects.filter(org=org, transform_type=TransformType.UI).first()
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    check_canvas_locked(orguser, canvas_lock_id)

    orgdbt_model = OrgDbtModel.objects.filter(uuid=model_uuid, type=OrgDbtModelType.MODEL).first()
    if not orgdbt_model:
        raise HttpError(404, "model not found")

    if orgdbt_model.type == OrgDbtModelType.SOURCE:
        raise HttpError(422, "Cannot delete source model")

    dbtautomation_service.delete_org_dbt_model(orgdbt_model, cascade)

    return {"success": 1}


@transform_router.delete("/dbt_project/source/{model_uuid}/")
@has_permission(["can_delete_dbt_model"])
def delete_source(request, model_uuid, canvas_lock_id: str = None, cascade: bool = False):
    """
    Delete a source from dbt project
    Cascade will be implemented when we re-haul the ui4t architecture
    """
    if cascade:
        raise NotImplementedError()

    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    # make sure the orgdbt here is the one we create locally
    orgdbt = OrgDbt.objects.filter(org=org, transform_type=TransformType.UI).first()
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    check_canvas_locked(orguser, canvas_lock_id)

    orgdbt_model = OrgDbtModel.objects.filter(uuid=model_uuid, type=OrgDbtModelType.SOURCE).first()
    if not orgdbt_model:
        raise HttpError(404, "source not found")

    if DbtEdge.objects.filter(Q(from_node=orgdbt_model) | Q(to_node=orgdbt_model)).count() > 0:
        raise HttpError(422, "Cannot delete source model as it is connected to other models")

    dbtautomation_service.delete_org_dbt_source(orgdbt_model, cascade)

    return {"success": 1}


@transform_router.delete("/dbt_project/model/operations/{operation_uuid}/")
@has_permission(["can_delete_dbt_operation"])
def delete_operation(request, operation_uuid, canvas_lock_id: str = None):
    """
    Delete an operation;
    Delete the model if its the last operation left in the chain
    """
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

    dbt_operation = OrgDbtOperation.objects.filter(uuid=operation_uuid).first()
    if not dbt_operation:
        raise HttpError(404, "operation not found")

    if OrgDbtOperation.objects.filter(dbtmodel=dbt_operation.dbtmodel).count() == 1:
        # delete the model file
        dbt_operation.dbtmodel.delete()
    else:
        dbt_operation.delete()

    # delete the model file is present
    dbtautomation_service.delete_dbt_model_in_project(dbt_operation.dbtmodel)

    return {"success": 1}


@transform_router.get("/dbt_project/data_type/")
@has_permission(["can_view_warehouse_data"])
def get_warehouse_datatypes(request):
    """Get the datatypes of a table in a warehouse"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    data_types = dbtautomation_service.warehouse_datatypes(org_warehouse)
    return data_types


@transform_router.post(
    "/dbt_project/canvas/lock/",
    response=LockCanvasResponseSchema,
)
@has_permission(["can_edit_dbt_model"])
def post_lock_canvas(request, payload: LockCanvasRequestSchema):
    """
    Lock the canvas for the org
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    canvas_lock = CanvasLock.objects.filter(locked_by__org=org).first()

    if canvas_lock:
        # locked, but not by the requestor
        if canvas_lock.locked_by != orguser:
            # no lock_id => didn't acquire the lock
            return LockCanvasResponseSchema(
                locked_by=canvas_lock.locked_by.user.email,
                locked_at=canvas_lock.locked_at.isoformat(),
            )

        # locked by the requestor
        else:
            # only if this is the right session do we refresh the lock
            if payload.lock_id == canvas_lock.lock_id:
                canvas_lock.locked_at = datetime.now()
                canvas_lock.save()
            else:
                # no lock_id => didn't acquire the lock
                return LockCanvasResponseSchema(
                    locked_by=canvas_lock.locked_by.user.email,
                    locked_at=canvas_lock.locked_at.isoformat(),
                )

    # no lock, acquire
    else:
        canvas_lock = CanvasLock.objects.create(
            locked_by=orguser, locked_at=datetime.now(), lock_id=uuid.uuid4()
        )

    return LockCanvasResponseSchema(
        locked_by=canvas_lock.locked_by.user.email,
        locked_at=canvas_lock.locked_at.isoformat(),
        lock_id=str(canvas_lock.lock_id),
    )


@transform_router.post("/dbt_project/canvas/unlock/")
@has_permission(["can_edit_dbt_model"])
def post_unlock_canvas(request, payload: LockCanvasRequestSchema):
    """
    Unlock the canvas for the org
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    canvas_lock = CanvasLock.objects.filter(locked_by__org=org).first()

    if canvas_lock is None:
        raise HttpError(404, "no lock found")

    if canvas_lock.locked_by != orguser:
        raise HttpError(403, "not allowed")

    if str(canvas_lock.lock_id) != payload.lock_id:
        raise HttpError(422, "wrong lock id")

    canvas_lock.delete()

    return {"success": 1}


# ==============================================================================
# UI4T V2 API - New unified architecture using CanvasNode and CanvasEdge
# ==============================================================================


@transform_router.get("/v2/dbt_project/sources_models/")
@has_permission(["can_view_dbt_models"])
def get_input_sources_and_models(request, schema_name: str = None):
    """
    Fetches all sources and models in a dbt project
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    # make sure the orgdbt here is the one we create locally
    orgdbt = OrgDbt.objects.filter(org=org, gitrepo_url=None).first()
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    query = OrgDbtModel.objects.filter(orgdbt=orgdbt)

    if schema_name:
        query = query.filter(schema=schema_name)

    res = []
    for orgdbt_model in query.all():
        if not orgdbt_model.under_construction:
            orgdbt_model.uuid = str(orgdbt_model.uuid)
            res.append(
                {
                    "schema": orgdbt_model.schema,
                    "sql_path": orgdbt_model.sql_path,
                    "name": orgdbt_model.name,
                    "display_name": orgdbt_model.display_name,
                    "uuid": str(orgdbt_model.uuid),
                    "type": orgdbt_model.type,
                    "source_name": orgdbt_model.source_name,
                    "output_cols": orgdbt_model.output_cols,
                }
            )

    return res


@transform_router.get("/v2/dbt_project/graph/")
@has_permission(["can_view_dbt_workspace"])
def get_dbt_project_DAG_v2(request):
    """
    V2 API: Simplified DAG generation using unified CanvasNode and CanvasEdge models.

    This replaces the complex get_dbt_project_DAG function with a much simpler approach:
    1. Get all canvas nodes for the org
    2. Get all canvas edges for the org
    3. Convert to frontend format

    Returns same format as v1 API for backward compatibility.
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    # Validate org has warehouse setup
    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(422, "please setup your warehouse first")

    # Validate org has dbt workspace
    orgdbt = OrgDbt.objects.filter(org=org, gitrepo_url=None).first()
    if not orgdbt:
        raise HttpError(422, "dbt workspace not setup")

    try:
        # Get all canvas nodes for this org with related dbtmodel - much simpler than before!
        canvas_nodes = CanvasNode.objects.filter(orgdbt=orgdbt).select_related("dbtmodel")

        # Get all canvas edges for this org
        canvas_edges = CanvasEdge.objects.filter(from_node__orgdbt=orgdbt).select_related(
            "from_node", "to_node"
        )

        # Convert nodes to frontend format (backward compatible)
        nodes = []
        for canvas_node in canvas_nodes:
            frontend_node = convert_canvas_node_to_frontend_format(canvas_node)
            nodes.append(frontend_node)

        # Convert edges to frontend format (same as before)
        edges = []
        for canvas_edge in canvas_edges:
            edges.append(
                {
                    "id": f"{canvas_edge.from_node.uuid}_{canvas_edge.to_node.uuid}",
                    "source": str(canvas_edge.from_node.uuid),
                    "target": str(canvas_edge.to_node.uuid),
                }
            )

        logger.info(
            f"DAG generated successfully for org {org.slug}: {len(nodes)} nodes, {len(edges)} edges"
        )

        return {"nodes": nodes, "edges": edges}

    except HttpError:
        # propagate API errors unchanged
        raise
    except Exception as e:
        logger.error(f"Error generating DAG for org {org.slug}: {str(e)}")
        raise HttpError(500, f"Failed to generate DAG: {str(e)}")


# V2 CRUD operations for CanvasNode
@transform_router.post("/v2/dbt_project/models/{dbtmodel_uuid}/nodes/")
@has_permission(["can_create_dbt_model"])
def post_create_src_model_node(request, dbtmodel_uuid: str):
    """
    Create a new CanvasNode representing a source/model node.

    Simplifies the complex source/model creation by using unified CanvasNode.
    """
    orguser: OrgUser = request.orguser

    warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not warehouse:
        raise HttpError(404, "please setup your warehouse first")

    orgdbt = orguser.org.dbt
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    try:
        # TODO: apply canvas locking logic

        org_dbt_model = OrgDbtModel.objects.filter(uuid=dbtmodel_uuid, orgdbt=orgdbt).first()
        if not org_dbt_model:
            raise HttpError(404, "model not found")

        # create only if the canvas node for the model doesn't already exist
        existing_node = CanvasNode.objects.filter(dbtmodel=org_dbt_model, orgdbt=orgdbt).first()
        if existing_node:
            logger.info(
                f"source/model node already exists: {existing_node.uuid} for model {dbtmodel_uuid}"
            )
            return convert_canvas_node_to_frontend_format(existing_node)

        output_cols = dbtautomation_service.update_output_cols_of_dbt_model(
            warehouse, org_dbt_model
        )
        org_dbt_model.output_cols = output_cols
        org_dbt_model.save()

        canvas_node = CanvasNode.objects.create(
            orgdbt=orgdbt,
            node_type=(
                CanvasNodeType.SOURCE
                if org_dbt_model.type == OrgDbtModelType.SOURCE
                else CanvasNodeType.MODEL
            ),
            name=f"{org_dbt_model.schema}.{org_dbt_model.name}",
            dbtmodel=org_dbt_model,
            output_cols=org_dbt_model.output_cols,
        )

        logger.info(f"source/model node created successfully: {canvas_node.uuid}")
        return convert_canvas_node_to_frontend_format(canvas_node)

    except HttpError:
        # propagate API errors unchanged
        raise

    except Exception as e:
        logger.error(f"Failed to create source/model node: {str(e)}")
        # Transaction will automatically rollback due to the exception
        raise HttpError(500, f"Failed to create node: {str(e)}")


@transform_router.post("/v2/dbt_project/operations/nodes/")
@has_permission(["can_create_dbt_model"])
def post_add_operation_node(request, payload: CreateOperationNodePayload):
    """
    V2 API: Create a new CanvasNode representing an operation on the node with node_uuid.
    The node_uuid can be any CanvasNode (source/model/operation).

    Simplifies the complex operation creation by using unified CanvasNode model.
    No more sequence tracking or implicit relationships - everything is explicit via edges.
    """
    orguser: OrgUser = request.orguser

    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    orgdbt = orguser.org.dbt
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    logger.info(f"creating operation: {payload.op_type}")

    # TODO: apply canvas locking logic

    try:
        main_input_node: CanvasNode = CanvasNode.objects.select_related("dbtmodel").get(
            uuid=payload.input_node_uuid, orgdbt=orgdbt
        )

        is_multi_input_op = payload.op_type in ["join", "unionall"]

        # validate operation config
        validate_operation_config_v2(payload.op_type, payload.config)

        # validate other inputs for the multi input operation
        other_input_models: list[ModelSrcInputsForMultiInputOp] = []
        if is_multi_input_op:
            other_input_models = validate_and_return_inputs_for_multi_input_op(
                payload.other_inputs, orgdbt
            )

        final_op_config = {}
        final_op_config["config"] = payload.config
        final_op_config["config"]["source_columns"] = payload.source_columns
        if is_multi_input_op:
            # only needed for dbt automation package to compute the output columns
            final_op_config["config"]["other_inputs"] = [
                {
                    "input": {
                        "input_type": dbtmodel_input.src_model.type,
                        "input_name": (
                            dbtmodel_input.src_model.name
                            if dbtmodel_input.src_model.type == "model"
                            else dbtmodel_input.src_model.display_name
                        ),
                        "source_name": dbtmodel_input.src_model.source_name,
                    },
                    "source_columns": dbtmodel_input.src_model.output_cols,
                    "seq": dbtmodel_input.seq,
                }
                for dbtmodel_input in other_input_models
            ]
        final_op_config["type"] = payload.op_type

        output_cols = dbtautomation_service.get_output_cols_for_operation(
            org_warehouse, payload.op_type, final_op_config["config"].copy()
        )

        if "other_inputs" in final_op_config["config"]:
            del final_op_config["config"]["other_inputs"]  # clean up before saving

        with transaction.atomic():
            # Create the operation canvas node
            canvas_node = CanvasNode.objects.create(
                orgdbt=orgdbt,
                operation_config=final_op_config,
                node_type=CanvasNodeType.OPERATION,
                name=payload.op_type,
                output_cols=output_cols,
            )

            # Create edge from the main input node to the newly created operation node
            CanvasEdge.objects.create(from_node=main_input_node, to_node=canvas_node, seq=1)

            # Create edges from all other input nodes in case of multi-input operations
            if is_multi_input_op:
                for dbtmodel_input in other_input_models:
                    # check if corresponding canvas node is already created
                    # create if its not there
                    src_model_node = CanvasNode.objects.filter(
                        dbtmodel=dbtmodel_input.src_model, orgdbt=orgdbt
                    ).first()
                    if not src_model_node:
                        src_model_node = CanvasNode.objects.create(
                            dbtmodel=dbtmodel_input.src_model,
                            orgdbt=orgdbt,
                            node_type=(
                                CanvasNodeType.SOURCE
                                if dbtmodel_input.src_model.type == OrgDbtModelType.SOURCE
                                else CanvasNodeType.MODEL
                            ),
                            output_cols=dbtmodel_input.src_model.output_cols,
                            name=f"{dbtmodel_input.src_model.schema}.{dbtmodel_input.src_model.name}",
                        )

                    CanvasEdge.objects.create(
                        from_node=src_model_node, to_node=canvas_node, seq=dbtmodel_input.seq
                    )

            logger.info(f"operation created successfully: {canvas_node.uuid}")
            return convert_canvas_node_to_frontend_format(canvas_node)

    except CanvasNode.DoesNotExist:
        logger.error(f"input node {payload.input_node_uuid} not found")
        raise HttpError(404, "input node not found")
    except HttpError:
        # let domain errors propagate (422/404 from validation helpers)
        raise
    except ValueError as err:
        logger.error(f"Validation error while creating operation: {str(err)}")
        raise HttpError(422, str(err)) from err
    except Exception as err:
        logger.error(f"Failed to create operation: {str(err)}")
        # Transaction will automatically rollback due to the exception
        raise HttpError(500, f"Failed to create operation: {str(err)}") from err


@transform_router.put("/v2/dbt_project/operations/nodes/{node_uuid}/")
@has_permission(["can_create_dbt_model"])
def put_operation_node(request, node_uuid: str, payload: EditOperationNodePayload):
    """
    V2 API: Create a new CanvasNode representing an operation on the node with node_uuid.
    The node_uuid can be any CanvasNode (source/model/operation).

    The base node & the edge for this operation node will not change.

    Simplifies the complex operation creation by using unified CanvasNode model.
    No more sequence tracking or implicit relationships - everything is explicit via edges.

    """
    orguser: OrgUser = request.orguser

    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    orgdbt = orguser.org.dbt
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    logger.info(f"updating operation: {payload.op_type}")

    # TODO: apply canvas locking logic

    try:
        curr_operation_node: CanvasNode = CanvasNode.objects.select_related("dbtmodel").get(
            uuid=node_uuid, orgdbt=orgdbt, node_type=CanvasNodeType.OPERATION
        )

        is_multi_input_op = payload.op_type in ["join", "unionall"]
        # validate operation config
        validate_operation_config_v2(payload.op_type, payload.config)

        # validate other inputs for the multi input operation
        other_input_models: list[ModelSrcInputsForMultiInputOp] = []
        if is_multi_input_op:
            other_input_models = validate_and_return_inputs_for_multi_input_op(
                payload.other_inputs, orgdbt
            )

        final_op_config = {}
        final_op_config["config"] = payload.config
        final_op_config["config"]["source_columns"] = payload.source_columns
        if is_multi_input_op:
            # only needed for dbt automation package to compute the output columns
            final_op_config["config"]["other_inputs"] = [
                {
                    "input": {
                        "input_type": dbtmodel_input.src_model.type,
                        "input_name": (
                            dbtmodel_input.src_model.name
                            if dbtmodel_input.src_model.type == "model"
                            else dbtmodel_input.src_model.display_name
                        ),
                        "source_name": dbtmodel_input.src_model.source_name,
                    },
                    "source_columns": dbtmodel_input.src_model.output_cols,
                    "seq": dbtmodel_input.seq,
                }
                for dbtmodel_input in other_input_models
            ]
        final_op_config["type"] = payload.op_type

        output_cols = dbtautomation_service.get_output_cols_for_operation(
            org_warehouse, payload.op_type, final_op_config["config"].copy()
        )

        if "other_inputs" in final_op_config["config"]:
            del final_op_config["config"]["other_inputs"]  # clean up before saving

        with transaction.atomic():
            # Create the operation canvas node
            curr_operation_node.operation_config = final_op_config
            curr_operation_node.name = payload.op_type
            curr_operation_node.output_cols = output_cols
            curr_operation_node.save()

            # remove the edges from other input nodes first
            if is_multi_input_op:
                CanvasEdge.objects.filter(to_node=curr_operation_node).exclude(seq=1).delete()

                for dbtmodel_input in other_input_models:
                    # check if corresponding canvas node is already created
                    # create if its not there
                    src_model_node = CanvasNode.objects.filter(
                        dbtmodel=dbtmodel_input.src_model, orgdbt=orgdbt
                    ).first()
                    if not src_model_node:
                        src_model_node = CanvasNode.objects.create(
                            dbtmodel=dbtmodel_input.src_model,
                            orgdbt=orgdbt,
                            node_type=(
                                CanvasNodeType.SOURCE
                                if dbtmodel_input.src_model.type == OrgDbtModelType.SOURCE
                                else CanvasNodeType.MODEL
                            ),
                            output_cols=dbtmodel_input.src_model.output_cols,
                            name=f"{dbtmodel_input.src_model.schema}.{dbtmodel_input.src_model.name}",
                        )

                    CanvasEdge.objects.create(
                        from_node=src_model_node,
                        to_node=curr_operation_node,
                        seq=dbtmodel_input.seq,
                    )

            logger.info(f"operation created successfully: {curr_operation_node.uuid}")
            return convert_canvas_node_to_frontend_format(curr_operation_node)

    except CanvasNode.DoesNotExist:
        logger.error(f"Operation node {node_uuid} not found")
        raise HttpError(404, "input node not found")
    except Exception as e:
        logger.error(f"Failed to update operation: {str(e)}")
        # Transaction will automatically rollback due to the exception
        raise HttpError(500, f"Failed to create operation: {str(e)}")


@transform_router.post("/v2/dbt_project/operations/nodes/{node_uuid}/terminate/")
@has_permission(["can_create_dbt_model"])
def post_terminate_operation_node(
    request, node_uuid: str, payload: TerminateChainAndCreateModelPayload
):
    """
    Terminate the chain at this operation node by adding a model node at the end.
    Basically materialize the chain into a model.
    """
    orguser: OrgUser = request.orguser

    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    orgdbt = orguser.org.dbt
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    # TODO: apply canvas locking logic

    try:
        terminal_node = CanvasNode.objects.get(
            uuid=node_uuid, orgdbt=orgdbt, node_type=CanvasNodeType.OPERATION
        )

        # get all operations upstream from this node
        operations_list = tranverse_graph_and_return_operations_list(terminal_node)

        merge_config = {
            "operations": operations_list,
            "dest_schema": payload.dest_schema,
            "output_name": payload.name,
        }

        # create the dbt model on disk by merging all the operations
        model_sql_path, output_cols = create_or_update_dbt_model_in_project_v2(
            org_warehouse, merge_config, orgdbt
        )

        with transaction.atomic():
            # create the dbt model in django db
            # name should be unique since dbt doesn't allow 2 models using the name in ref()
            dbtmodel = OrgDbtModel.objects.filter(orgdbt=orgdbt, name=payload.name).first()

            if dbtmodel:
                logger.info(f"dbt model already exists, updating: {dbtmodel.uuid}")
                dbtmodel.display_name = payload.display_name
                dbtmodel.schema = payload.dest_schema
                dbtmodel.sql_path = str(model_sql_path)
                dbtmodel.save()
            else:
                logger.info(f"Creating new dbt model: {payload.dest_schema}.{payload.name}")
                dbtmodel = OrgDbtModel.objects.create(
                    orgdbt=orgdbt,
                    name=payload.name,
                    display_name=payload.display_name,
                    schema=payload.dest_schema,
                    sql_path=str(model_sql_path),
                    uuid=uuid.uuid4(),
                )

            # create the node representing the model
            model_node = CanvasNode.objects.create(
                orgdbt=orgdbt,
                node_type=CanvasNodeType.MODEL,
                name=f"{dbtmodel.schema}.{dbtmodel.name}",
                output_cols=output_cols,
                dbtmodel=dbtmodel,
            )

            # create the edge from the terminal operation node to the model node
            CanvasEdge.objects.create(from_node=terminal_node, to_node=model_node, seq=1)

        logger.info(f"V2 operation node terminated successfully: {node_uuid}")
        return convert_canvas_node_to_frontend_format(model_node)

    except CanvasNode.DoesNotExist:
        logger.error(f"Operation node {node_uuid} not found")
        raise HttpError(404, "operation node not found")
    except Exception as e:
        logger.error(f"Failed to terminate operation node: {str(e)}")
        # Transaction will automatically rollback due to the exception
        raise HttpError(500, f"Failed to terminate operation node: {str(e)}")


@transform_router.delete("/v2/dbt_project/nodes/{node_uuid}/")
@has_permission(["can_create_dbt_model"])
def delete_canvas_node(request, node_uuid: str):
    """
    Delete a CanvasNode by uuid.
    This will also delete all associated edges.

    This operation is atomic - if any step fails, all changes are rolled back.
    """
    orguser: OrgUser = request.orguser
    orgdbt = orguser.org.dbt

    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    try:
        canvas_node = CanvasNode.objects.get(uuid=node_uuid, orgdbt=orgdbt)

        if canvas_node.dbtmodel:
            dbtautomation_service.delete_dbt_model_in_project(canvas_node.dbtmodel)

        canvas_node.delete()

        logger.info(f"canvas node deleted successfully: {node_uuid}")
        return {"success": 1}
    except CanvasNode.DoesNotExist:
        logger.error(f"Canvas node {node_uuid} not found")
        raise HttpError(404, "canvas node not found")
    except Exception as e:
        logger.error(f"Failed to delete canvas node: {str(e)}")
        # Transaction will automatically rollback due to the exception
        raise HttpError(500, f"Failed to delete canvas node: {str(e)}")


@transform_router.get("/v2/dbt_project/nodes/{node_uuid}/")
@has_permission(["can_view_dbt_workspace"])
def get_canvas_node(request, node_uuid: str):
    """
    Fetch a single CanvasNode by UUID (v2 API).
    """
    orguser: OrgUser = request.orguser
    orgdbt = orguser.org.dbt

    # ensure dbt workspace exists for org
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    try:
        canvas_node = CanvasNode.objects.select_related("dbtmodel").get(
            uuid=node_uuid, orgdbt=orgdbt
        )

        return_canvas_node = convert_canvas_node_to_frontend_format(canvas_node)

        # get the input nodes of type model or source
        if canvas_node.node_type == CanvasNodeType.OPERATION and canvas_node.operation_config.get(
            "type", ""
        ):
            incoming_edges = (
                CanvasEdge.objects.filter(
                    to_node=canvas_node,
                    from_node__node_type__in=[CanvasNodeType.MODEL, CanvasNodeType.SOURCE],
                )
                .order_by("seq")
                .select_related("from_node")
            )
            other_inputs = []
            for edge in incoming_edges:
                nn = convert_canvas_node_to_frontend_format(edge.from_node)
                nn["seq"] = edge.seq
                other_inputs.append(nn)

            if other_inputs:
                return_canvas_node["input_nodes"] = other_inputs

        return return_canvas_node
    except CanvasNode.DoesNotExist:
        logger.error(f"Canvas node {node_uuid} not found")
        raise HttpError(404, "canvas node not found")
    except Exception as e:
        logger.error(f"Failed to fetch canvas node {node_uuid}: {str(e)}")
        raise HttpError(500, f"Failed to fetch canvas node: {str(e)}")
