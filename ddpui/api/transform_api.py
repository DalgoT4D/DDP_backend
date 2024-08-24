import os
import uuid
import shutil
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from django.db.models import Q
from django.forms import model_to_dict
from django.utils.text import slugify
from django.db.models import Prefetch
from ninja import NinjaAPI
from ninja.errors import ValidationError, HttpError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError

from ddpui import auth
from ddpui.ddpdbt.dbt_service import setup_local_dbt_workspace
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgDbt, OrgWarehouse
from ddpui.models.dbt_workflow import OrgDbtModel, DbtEdge, OrgDbtOperation
from ddpui.models.canvaslock import CanvasLock

from ddpui.schemas.org_task_schema import DbtProjectSchema
from ddpui.schemas.dbt_workflow_schema import (
    CreateDbtModelPayload,
    CompleteDbtModelPayload,
    EditDbtOperationPayload,
    LockCanvasRequestSchema,
    LockCanvasResponseSchema,
)
from ddpui.core.transformfunctions import validate_operation_config, check_canvas_locked
from ddpui.api.warehouse_api import get_warehouse_data

from ddpui.core import dbtautomation_service
from ddpui.core.dbtautomation_service import sync_sources_for_warehouse
from ddpui.auth import has_permission

from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.transform_workflow_helpers import (
    from_orgdbtoperation,
    from_orgdbtmodel,
)

transformapi = NinjaAPI(urls_namespace="transform")

load_dotenv()

logger = CustomLogger("ddpui")


@transformapi.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """
    Handle any ninja validation errors raised in the apis
    These are raised during request payload validation
    exc.errors is correct
    """
    return Response({"detail": exc.errors}, status=422)


@transformapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """
    Handle any pydantic errors raised in the apis
    These are raised during response payload validation
    exc.errors() is correct
    """
    return Response({"detail": exc.errors()}, status=500)


@transformapi.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception
):  # pylint: disable=unused-argument # skipcq PYL-W0613
    """Handle any other exception raised in the apis"""
    print(exc)
    return Response({"detail": "something went wrong"}, status=500)


@transformapi.exception_handler(ValueError)
def handle_value_error(request, exc):
    """
    Handle ValueError exceptions.
    """
    return Response({"detail": str(exc)}, status=400)


@transformapi.post("/dbt_project/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_create_dbt_workspace"])
def create_dbt_project(request, payload: DbtProjectSchema):
    """
    Create a new dbt project.
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug
    project_dir.mkdir(parents=True, exist_ok=True)

    # Call the post_dbt_workspace function
    _, error = setup_local_dbt_workspace(
        org, project_name="dbtrepo", default_schema=payload.default_schema
    )
    if error:
        raise HttpError(422, error)

    return {"message": f"Project {org.slug} created successfully"}


@transformapi.delete("/dbt_project/{project_name}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_delete_dbt_workspace"])
def delete_dbt_project(request, project_name: str):
    """
    Delete a dbt project in this org
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug

    if not project_dir.exists():
        raise HttpError(404, f"Organization {org.slug} does not have any projects")

    dbtrepo_dir: Path = project_dir / project_name

    if not dbtrepo_dir.exists():
        raise HttpError(
            422, f"Project {project_name} does not exist in organization {org.slug}"
        )

    if org.dbt:
        dbt = org.dbt
        org.dbt = None
        org.save()

        dbt.delete()

    shutil.rmtree(dbtrepo_dir)

    return {"message": f"Project {project_name} deleted successfully"}


@transformapi.post("/dbt_project/sync_sources/", auth=auth.CustomAuthMiddleware())
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

    task = sync_sources_for_warehouse.delay(
        orgdbt.id, org_warehouse.id, org_warehouse.org.slug
    )

    return {"task_id": task.id}


########################## Models & Sources #############################################


@transformapi.post("/dbt_project/model/", auth=auth.CustomAuthMiddleware())
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
        target_model = OrgDbtModel.objects.filter(
            uuid=payload.target_model_uuid
        ).first()

    if not target_model:
        target_model = OrgDbtModel.objects.create(
            uuid=uuid.uuid4(),
            orgdbt=orgdbt,
            under_construction=True,
        )

    # only under construction models can be modified
    if not target_model.under_construction:
        raise HttpError(422, "model is locked")

    current_operations_chained = OrgDbtOperation.objects.filter(
        dbtmodel=target_model
    ).count()

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
        org_warehouse, payload.op_type, final_config["config"].copy()
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


@transformapi.put(
    "/dbt_project/model/operations/{operation_uuid}/", auth=auth.CustomAuthMiddleware()
)
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

    dbtautomation_service.update_dbt_model_in_project(org_warehouse, target_model)

    # propogate the udpates down the chain
    dbtautomation_service.propagate_changes_to_downstream_operations(
        target_model, dbt_operation, depth=1
    )

    logger.info("updated output cols for the target model")

    return from_orgdbtoperation(dbt_operation, chain_length=len(all_ops))


@transformapi.get(
    "/dbt_project/model/operations/{operation_uuid}/", auth=auth.CustomAuthMiddleware()
)
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
            model = OrgDbtModel.objects.filter(
                uuid=config["input_models"][0]["uuid"]
            ).first()
            if model:
                for col_data in get_warehouse_data(
                    request,
                    "table_columns",
                    schema_name=model.schema,
                    table_name=model.name,
                ):
                    prev_source_columns.append(col_data["name"])

    return from_orgdbtoperation(dbt_operation, prev_source_columns=prev_source_columns)


@transformapi.post(
    "/dbt_project/model/{model_uuid}/save/", auth=auth.CustomAuthMiddleware()
)
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


@transformapi.get("/dbt_project/sources_models/", auth=auth.CustomAuthMiddleware())
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


@transformapi.get("/dbt_project/graph/", auth=auth.CustomAuthMiddleware())
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

    edges = (
        DbtEdge.objects.filter(Q(from_node__orgdbt=orgdbt) | Q(to_node__orgdbt=orgdbt))
        .select_related("from_node", "to_node")
        .prefetch_related(
            Prefetch(
                "from_node__operations",
                queryset=OrgDbtOperation.objects.order_by("seq"),
            ),
            Prefetch(
                "to_node__operations",
                queryset=OrgDbtOperation.objects.order_by("seq"),
            ),
        )
        .all()
    )

    seen_model_node_ids = set()
    for edge in edges:
        if edge.from_node.id not in seen_model_node_ids:
            model_nodes.append(edge.from_node)
        seen_model_node_ids.add(edge.from_node.id)
        if edge.to_node.id not in seen_model_node_ids:
            model_nodes.append(edge.to_node)
        seen_model_node_ids.add(edge.to_node.id)

    # push operation nodes and edges if any
    for target_node in model_nodes:
        # src_node -> op1 -> op2 -> op3 -> op4
        # start building edges from the source
        prev_op = None
        for operation in target_node.operations.order_by("seq").all():
            operation_nodes.append(operation)
            if (
                "input_models" in operation.config
                and len(operation.config["input_models"]) > 0
            ):
                input_models = operation.config["input_models"]
                # edge(s) between the node(s) and other sources involved that are tables (OrgDbtModel)
                for op_src_node in OrgDbtModel.objects.filter(
                    uuid__in=[model["uuid"] for model in input_models]
                ).all():
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
    res["nodes"] = [
        nn for nn in res_nodes if not (nn["id"] in seen or seen.add(nn["id"]))
    ]
    seen = set()
    res["edges"] = [
        edg for edg in res_edges if not (edg["id"] in seen or seen.add(edg["id"]))
    ]

    return res


@transformapi.delete(
    "/dbt_project/model/{model_uuid}/", auth=auth.CustomAuthMiddleware()
)
@has_permission(["can_delete_dbt_model"])
def delete_model(request, model_uuid, canvas_lock_id: str = None):
    """
    Delete a model if it does not have any operations chained
    Convert the model to "under_construction if its has atleast 1 operation chained"
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

    orgdbt_model = OrgDbtModel.objects.filter(uuid=model_uuid).first()
    if not orgdbt_model:
        raise HttpError(404, "model not found")

    if orgdbt_model.type == "source":
        return {"success": 1}

    operations = OrgDbtOperation.objects.filter(dbtmodel=orgdbt_model).count()

    if operations > 0:
        orgdbt_model.under_construction = True
        orgdbt_model.save()

        # delete the model file is present
        dbtautomation_service.delete_dbt_model_in_project(orgdbt_model)
    else:
        # make sure this is not linked to any other model
        # delete if there are no edges coming or going out of this model
        if (
            DbtEdge.objects.filter(
                Q(from_node=orgdbt_model) | Q(to_node=orgdbt_model)
            ).count()
            == 0
        ):
            orgdbt_model.delete()

    return {"success": 1}


@transformapi.delete(
    "/dbt_project/model/operations/{operation_uuid}/", auth=auth.CustomAuthMiddleware()
)
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


@transformapi.get("/dbt_project/data_type/", auth=auth.CustomAuthMiddleware())
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


@transformapi.post(
    "/dbt_project/canvas/lock/",
    auth=auth.CustomAuthMiddleware(),
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


@transformapi.post(
    "/dbt_project/canvas/unlock/",
    auth=auth.CustomAuthMiddleware(),
)
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
