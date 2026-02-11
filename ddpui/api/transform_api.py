import uuid
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

from ninja import Router
from ninja.errors import HttpError

from django.db.models import Q
from django.db import transaction
from django.utils import timezone

from ddpui.datainsights import warehouse
from ddpui.ddpdbt.dbt_service import setup_local_dbt_workspace
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgDbt, OrgWarehouse, TransformType, OrgDataFlowv1
from ddpui.models.dbt_workflow import OrgDbtModel, OrgDbtModelType
from ddpui.models.canvaslock import CanvasLock
from ddpui.models.canvas_models import CanvasNode, CanvasEdge, CanvasNodeType
from ddpui.models.tasks import DataflowOrgTask, OrgTask

from ddpui.schemas.org_task_schema import DbtProjectSchema
from ddpui.schemas.dbt_workflow_schema import (
    CreateOperationNodePayload,
    LockCanvasResponseSchema,
    EditOperationNodePayload,
    ModelSrcInputsForMultiInputOp,
    validate_operation_config_v2,
    TerminateChainAndCreateModelPayload,
)
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.utils.taskprogress import TaskProgress
from ddpui.models.tasks import TaskProgressHashPrefix, TaskProgressStatus

from ddpui.core import dbtautomation_service
from ddpui.core.dbtautomation_service import (
    create_or_update_dbt_model_in_project_v2,
    sync_sources_for_warehouse_v2,
    convert_canvas_node_to_frontend_format,
    tranverse_graph_and_return_operations_list,
    validate_and_return_inputs_for_multi_input_op,
)
from ddpui.auth import has_permission
from ddpui.core.git_manager import GitManager
from ddpui.ddpdbt import dbt_service

from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.transform_workflow_helpers import (
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
    setup_local_dbt_workspace(org, project_name="dbtrepo", default_schema=payload.default_schema)

    return {"message": f"Project {org.slug} created successfully"}


@transform_router.delete("/dbt_project/{project_name}")
@has_permission(["can_delete_dbt_workspace"])
def delete_dbt_project(request, project_name: str, force_delete: bool = False):
    """
    Delete a dbt project in this org

    CRITICAL SAFETY CHECKS: This endpoint performs destructive operations.
    It should only be called with explicit user confirmation.
    """
    orguser: OrgUser = request.orguser
    org = orguser.org
    orgdbt = org.dbt

    logger.warning(
        f"DANGEROUS OPERATION: Delete dbt project requested by {orguser.user.email} for org {org.slug}, project {project_name}"
    )

    org_dir = Path(DbtProjectManager.get_org_dir(org))

    if not org_dir.exists():
        raise HttpError(404, f"Organization {org.slug} does not have any projects")

    project_dir: Path = org_dir / project_name

    if not project_dir.exists():
        raise HttpError(422, f"Project {project_name} does not exist in organization {org.slug}")

    # SAFETY CHECK: Prevent accidental deletion from canvas operations
    if not force_delete and orgdbt:
        # Check if there are active canvas nodes - prevent deletion if canvas is in use
        active_canvas_nodes = CanvasNode.objects.filter(orgdbt=orgdbt).count()
        if active_canvas_nodes > 0:
            logger.error(
                f"BLOCKED DELETION: {active_canvas_nodes} active canvas nodes found for org {org.slug}"
            )
            raise HttpError(
                422,
                f"Cannot delete dbt workspace: {active_canvas_nodes} active workflow elements found. "
                f"This would result in data loss. Please contact support if you need to delete this workspace.",
            )

    # Log the deletion before performing it
    logger.warning(
        f"PROCEEDING WITH DELETION: org={org.slug}, project={project_name}, force_delete={force_delete}, user={orguser.user.email}"
    )

    if orgdbt:
        # Before deleting, log the current state for recovery
        logger.warning(
            f"DELETING ORGDBT: id={orgdbt.id}, cli_profile_block_id={orgdbt.cli_profile_block_id}, project_dir={orgdbt.project_dir}"
        )

        org.dbt = None
        org.save()

        orgdbt.delete()

        logger.warning(f"DELETING DIRECTORY: {project_dir}")
        shutil.rmtree(project_dir)

    logger.warning(f"DELETION COMPLETED: org={org.slug}, project={project_name}")
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

    orgdbt = OrgDbt.objects.filter(org=org).first()
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
            "status": TaskProgressStatus.RUNNING,
        }
    )

    sync_sources_for_warehouse_v2.delay(orgdbt.id, org_warehouse.id, task_id, hashkey)

    return {"task_id": task_id, "hashkey": hashkey}


########################## Models & Sources #############################################


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


# Canvas Lock endpoints (matching dashboard pattern)
@transform_router.post("/dbt_project/canvas/lock/", response=LockCanvasResponseSchema)
@has_permission(["can_edit_dbt_model"])
def lock_canvas(request):
    """Lock canvas for editing"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    # Get the dbt project via org.dbt relationship
    orgdbt = getattr(org, "dbt", None)
    if orgdbt is None:
        raise HttpError(404, "dbt workspace not setup")

    # Check if already locked
    try:
        lock: CanvasLock = orgdbt.canvas_lock
        if not lock.is_expired():
            if lock.locked_by == orguser:
                # Refresh lock with 2-minute duration
                lock.expires_at = timezone.now() + timedelta(minutes=2)
                lock.save()
                return LockCanvasResponseSchema(
                    lock_token=lock.lock_token,
                    expires_at=lock.expires_at.isoformat(),
                    locked_by=lock.locked_by.user.email,
                )
            else:
                raise HttpError(423, f"Canvas is already locked by {lock.locked_by.user.email}")
        else:
            # Delete expired lock
            lock.delete()
    except CanvasLock.DoesNotExist:
        pass

    # Create new lock with 2-minute duration
    lock = CanvasLock.objects.create(
        dbt=orgdbt,
        locked_by=orguser,
        lock_token=str(uuid.uuid4()),
        expires_at=timezone.now() + timedelta(minutes=2),
    )

    return LockCanvasResponseSchema(
        lock_token=lock.lock_token,
        expires_at=lock.expires_at.isoformat(),
        locked_by=lock.locked_by.user.email,
    )


@transform_router.put("/dbt_project/canvas/lock/refresh/")
@has_permission(["can_edit_dbt_model"])
def refresh_canvas_lock(request):
    """Refresh canvas lock to extend expiry"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    # Get the dbt project via org.dbt relationship
    orgdbt = org.dbt
    if orgdbt is None:
        raise HttpError(404, "dbt workspace not setup")

    try:
        lock: CanvasLock = orgdbt.canvas_lock
        if lock.is_expired():
            raise HttpError(410, "Lock has expired")
        if lock.locked_by != orguser:
            raise HttpError(403, "You can only refresh your own locks")

        # Refresh lock with 2-minute duration
        lock.expires_at = timezone.now() + timedelta(minutes=2)
        lock.save()

        logger.info(f"Refreshed lock for canvas")

        return LockCanvasResponseSchema(
            lock_token=lock.lock_token,
            expires_at=lock.expires_at.isoformat(),
            locked_by=lock.locked_by.user.email,
        )
    except CanvasLock.DoesNotExist:
        raise HttpError(404, "No active lock found")


@transform_router.delete("/dbt_project/canvas/lock/")
@has_permission(["can_edit_dbt_model"])
def unlock_canvas(request):
    """Unlock canvas"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    # Get the dbt project via org.dbt relationship
    orgdbt = getattr(org, "dbt", None)
    if orgdbt is None:
        raise HttpError(404, "dbt workspace not setup")

    try:
        lock = orgdbt.canvas_lock
        if lock.locked_by != orguser:
            raise HttpError(403, "You can only unlock your own locks")
        lock.delete()
    except CanvasLock.DoesNotExist:
        pass  # Already unlocked

    return {"success": True}


# Canvas Lock Helper Function
def validate_canvas_lock(orguser: OrgUser, orgdbt):
    """
    Validate that the canvas is properly locked by the requesting user.
    Similar to dashboard lock validation but for canvas operations.
    Also refreshes the lock expiry on successful validation.

    Args:
        orguser: The user making the request
        orgdbt: The OrgDbt object to check lock for

    Raises:
        HttpError: 423 if canvas is locked by another user
        HttpError: 410 if canvas lock has expired
    """
    # Check canvas lock status
    try:
        lock: CanvasLock = orgdbt.canvas_lock
        if lock.is_expired():
            # Clean up expired lock
            lock.delete()
            raise HttpError(410, "Canvas lock has expired. Please acquire a new lock.")
        elif lock.locked_by != orguser:
            raise HttpError(423, f"Canvas is locked by {lock.locked_by.user.email}")
        # Lock is valid and owned by the user - refresh expiry and proceed
        lock.expires_at = timezone.now() + timedelta(minutes=2)
        lock.save()
    except CanvasLock.DoesNotExist:
        raise HttpError(423, "Canvas is not locked. Please acquire a lock before making changes.")


# ==============================================================================
# UI4T V2 API - New unified architecture using CanvasNode and CanvasEdge
# ==============================================================================


@transform_router.get("/v2/dbt_project/sources_models/")
@has_permission(["can_view_dbt_models"])
def get_input_sources_and_models_v2(request, schema_name: str = None):
    """
    Fetches all sources and models in a dbt project
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    # make sure the orgdbt here is the one we create locally
    orgdbt = org.dbt
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


@transform_router.delete("/v2/dbt_project/model/{model_uuid}/")
@has_permission(["can_delete_dbt_model"])
def delete_orgdbtmodel(request, model_uuid, canvas_lock_id: str = None, cascade: bool = False):
    """
    Delete a model if it does not have any operations chained
    """
    if cascade:
        raise NotImplementedError()

    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    orgdbt = org.dbt
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    validate_canvas_lock(orguser, orgdbt)

    orgdbt_model = OrgDbtModel.objects.filter(uuid=model_uuid).first()
    if not orgdbt_model:
        raise HttpError(404, "model not found")

    # Check if the model is being used in any canvas nodes
    canvas_nodes = CanvasNode.objects.filter(dbtmodel=orgdbt_model, orgdbt=orgdbt)
    if canvas_nodes.exists():
        node_names = [node.name for node in canvas_nodes[:3]]  # Show first 3
        raise HttpError(422, f"Cannot delete: being used in the workflow ({', '.join(node_names)})")

    if orgdbt_model.type == OrgDbtModelType.SOURCE:
        # For sources, ensure that the source definition is removed from the project files
        dbtautomation_service.delete_dbt_source_in_project(orgdbt_model)
    elif orgdbt_model.type == OrgDbtModelType.MODEL:
        # Delete the model file from disk
        dbtautomation_service.delete_dbt_model_in_project(orgdbt_model)

    # Delete the database record
    orgdbt_model.delete()

    return {"success": 1}


@transform_router.get("/v2/dbt_project/graph/")
@has_permission(["can_view_dbt_workspace"])
def get_dbt_project_DAG_v2(request):
    """
    V2 API: Simplified DAG generation using unified CanvasNode and CanvasEdge models.

    This replaces the complex get_dbt_project_DAG function with a much simpler approach:
    1. Get all canvas nodes for the org
    2. Get all canvas edges for the org
    3. Convert to frontend format
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    # Validate org has warehouse setup
    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(422, "please setup your warehouse first")

    # Validate org has dbt workspace
    orgdbt = OrgDbt.objects.filter(org=org).first()
    if not orgdbt:
        raise HttpError(422, "dbt workspace not setup")

    try:
        # Get all canvas nodes for this org with related dbtmodel - much simpler than before!
        canvas_nodes = CanvasNode.objects.filter(orgdbt=orgdbt).select_related("dbtmodel")

        # Get all canvas edges for this org
        canvas_edges = CanvasEdge.objects.filter(from_node__orgdbt=orgdbt).select_related(
            "from_node", "to_node"
        )

        # Get changed files list using GitManager (if git is initialized)
        project_dir = Path(DbtProjectManager.get_dbt_project_dir(orgdbt))
        if not project_dir.exists():
            raise HttpError(500, "dbt project directory does not exist")

        changed_files = []
        if orgdbt.gitrepo_url:
            try:
                git_manager = GitManager(repo_local_path=project_dir)
                if git_manager.is_git_initialized():
                    changed_files = git_manager.get_changed_files_list()
            except Exception as e:
                # Log the error but continue without git status
                logger.warning(f"Failed to get git status for org {org.slug}: {e}")

        # Convert nodes to frontend format (backward compatible)
        nodes = []
        for canvas_node in canvas_nodes:
            frontend_node = convert_canvas_node_to_frontend_format(canvas_node, changed_files)
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

    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "please setup your warehouse first")

    orgdbt = orguser.org.dbt
    if not orgdbt:
        raise HttpError(404, "dbt workspace not setup")

    try:
        # TODO: apply canvas locking logic

        org_dbt_model = OrgDbtModel.objects.filter(uuid=dbtmodel_uuid, orgdbt=orgdbt).first()
        if not org_dbt_model:
            raise HttpError(404, "model not found")

        # handle source definitions better
        # if its a source that we are adding to the canvas; make sure it exists in the yml deifnition
        # then update the sql_path to point to the source definition file
        if org_dbt_model.type == OrgDbtModelType.SOURCE:
            source_yml_def: dbtautomation_service.SourceYmlDefinition = (
                dbtautomation_service.ensure_source_yml_definition_in_project(
                    orgdbt, org_dbt_model.schema, org_dbt_model.name
                )
            )
            org_dbt_model.sql_path = source_yml_def.sql_path
            org_dbt_model.source_name = source_yml_def.source_name
            org_dbt_model.name = source_yml_def.table
            org_dbt_model.schema = source_yml_def.source_schema
            org_dbt_model.save()

        # create only if the canvas node for the model doesn't already exist
        existing_node = CanvasNode.objects.filter(dbtmodel=org_dbt_model, orgdbt=orgdbt).first()
        if existing_node:
            logger.info(
                f"source/model node already exists: {existing_node.uuid} for model {dbtmodel_uuid}"
            )
            return convert_canvas_node_to_frontend_format(existing_node)

        output_cols = dbtautomation_service.update_output_cols_of_dbt_model(
            org_warehouse, org_dbt_model
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

    # Fetch the operation node first, outside the main try block
    try:
        curr_operation_node: CanvasNode = CanvasNode.objects.select_related("dbtmodel").get(
            uuid=node_uuid, orgdbt=orgdbt, node_type=CanvasNodeType.OPERATION
        )
    except CanvasNode.DoesNotExist:
        logger.error(f"Operation node {node_uuid} not found")
        raise HttpError(404, "operation node not found")

    try:
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

                    # do an update or create for the edge
                    CanvasEdge.objects.update_or_create(
                        from_node=src_model_node,
                        to_node=curr_operation_node,
                        defaults={"seq": dbtmodel_input.seq},
                    )

            logger.info(f"operation updated successfully: {curr_operation_node.uuid}")
            return convert_canvas_node_to_frontend_format(curr_operation_node)

    except HttpError:
        # let domain errors propagate (422/404 from validation helpers)
        raise
    except ValueError as e:
        logger.error(f"Validation error while updating operation: {str(e)}")
        raise HttpError(422, str(e)) from e
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
            "rel_dir_to_models": payload.rel_dir_to_models,
        }

        # create the dbt model on disk by merging all the operations
        model_sql_path, output_cols = create_or_update_dbt_model_in_project_v2(
            org_warehouse, merge_config, orgdbt
        )

        with transaction.atomic():
            # create the dbt model in django db
            # name should be unique since dbt doesn't allow 2 models using the name in ref()
            dbtmodel = OrgDbtModel.objects.filter(
                orgdbt=orgdbt, name=payload.name, type=OrgDbtModelType.MODEL
            ).first()

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
            model_node, created = CanvasNode.objects.get_or_create(
                dbtmodel=dbtmodel,
                orgdbt=orgdbt,
                defaults={
                    "node_type": CanvasNodeType.MODEL,
                    "name": f"{dbtmodel.schema}.{dbtmodel.name}",
                    "output_cols": output_cols,
                },
            )
            if not created:
                model_node.output_cols = output_cols
                model_node.save()

            # create the edge from the terminal operation node to the model node
            # do an update or create for the edge
            CanvasEdge.objects.update_or_create(
                from_node=terminal_node,
                to_node=model_node,
                defaults={"seq": 1},
            )

        logger.info(f"V2 operation node terminated successfully: {node_uuid}")
        return convert_canvas_node_to_frontend_format(model_node)

    except CanvasNode.DoesNotExist:
        logger.error(f"Operation node {node_uuid} not found")
        raise HttpError(404, "operation node not found")
    except HttpError:
        # let domain errors propagate (422/404 from validation helpers)
        raise
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
        dbtmodel = canvas_node.dbtmodel

        if dbtmodel:
            if dbtmodel.type == OrgDbtModelType.MODEL:
                dbtautomation_service.delete_dbt_model_in_project(dbtmodel)
            elif dbtmodel.type == OrgDbtModelType.SOURCE:
                dbtautomation_service.delete_dbt_source_in_project(dbtmodel)

        canvas_node.delete()

        logger.info(f"canvas node deleted successfully: {node_uuid}")
        return {"success": 1}
    except CanvasNode.DoesNotExist:
        logger.error(f"Canvas node {node_uuid} not found")
        raise HttpError(404, "canvas node not found")
    except HttpError:
        # let domain errors propagate (422/404 from validation helpers)
        raise
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


@transform_router.post("/v2/dbt_project/sync_remote_dbtproject_to_canvas/")
@has_permission(["can_create_dbt_model"])
def sync_remote_dbtproject_to_canvas(request):
    """
    Sync a remote dbt project to canvas (create/update CanvasNodes and CanvasEdges)
    """
    orguser: OrgUser = request.orguser
    org = orguser.org
    orgdbt = org.dbt

    if not orgdbt:
        raise HttpError(400, "Organization does not have a dbt workspace configured")

    if orgdbt.transform_type != TransformType.GIT:
        logger.info(f"Org {org.slug} dbt workspace is not of GIT type, skipping sync to canvas")
        return {
            "success": True,
            "message": "dbt workspace is not of GIT type, skipping sync to canvas",
        }

    # Get warehouse
    try:
        warehouse_obj = OrgWarehouse.objects.get(org=org)
    except OrgWarehouse.DoesNotExist:
        raise HttpError(400, "Organization does not have a warehouse configured")

    return dbt_service.sync_remote_dbtproject_to_canvas(org, orgdbt, warehouse_obj)


@transform_router.get("/v2/dbt_project/models_directories/")
@has_permission(["can_view_dbt_workspace"])
def get_models_directories(request):
    """
    Get all directories (and subdirectories) under the models folder
    """
    orguser: OrgUser = request.orguser
    org = orguser.org
    orgdbt = org.dbt
    if not orgdbt:
        raise HttpError(400, "Organization does not have a dbt workspace configured")

    try:
        project_dir = Path(DbtProjectManager.get_dbt_project_dir(orgdbt))
        if not project_dir.exists():
            raise HttpError(500, "dbt project directory does not exist")

        models_dir = project_dir / "models"
        if not models_dir.exists():
            return {"directories": []}

        # Get all directories under models
        directories = []
        for item in models_dir.rglob("*"):
            if item.is_dir():
                # Get relative path from models directory
                relative_path = item.relative_to(models_dir)
                directories.append(str(relative_path))

        # Add root models directory and sort
        directories.insert(0, "")  # Empty string represents models root
        directories.sort(key=lambda x: (x.count("/"), x))  # Sort by depth then alphabetically

        return {"directories": directories}

    except Exception as e:
        logger.exception(f"Error getting models directories for org {org.slug}: {str(e)}")
        raise HttpError(500, f"Failed to get models directories: {str(e)}")
