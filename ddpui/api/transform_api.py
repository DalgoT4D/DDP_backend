import os
import uuid
import shutil
from pathlib import Path

from dotenv import load_dotenv
from django.db.models import Q
from django.utils.text import slugify
from ninja import NinjaAPI
from ninja.errors import ValidationError, HttpError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError

from ddpui import auth
from ddpui.ddpdbt.dbt_service import setup_local_dbt_workspace
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgDbt, OrgWarehouse
from ddpui.models.dbt_workflow import OrgDbtModel, DbtEdge, OrgDbtOperation
from ddpui.utils.custom_logger import CustomLogger

from ddpui.schemas.org_task_schema import DbtProjectSchema
from ddpui.schemas.dbt_workflow_schema import (
    CreateDbtModelPayload,
    CompleteDbtModelPayload,
)

from ddpui.core import dbtautomation_service
from ddpui.core.dbtautomation_service import sync_sources_for_warehouse

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


@transformapi.post("/dbt_project/", auth=auth.CanManagePipelines())
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


@transformapi.delete("/dbt_project/{project_name}", auth=auth.CanManagePipelines())
def delete_dbt_project(request, project_name: str):
    """
    Delete a dbt project in this org
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug

    if not project_dir.exists():
        return {"error": f"Organization {org.slug} does not have any projects"}

    dbtrepo_dir: Path = project_dir / project_name

    if not dbtrepo_dir.exists():
        return {
            "error": f"Project {project_name} does not exist in organization {org.slug}"
        }

    if org.dbt:
        dbt = org.dbt
        org.dbt = None
        org.save()

        dbt.delete()

    shutil.rmtree(dbtrepo_dir)

    return {"message": f"Project {project_name} deleted successfully"}


@transformapi.post("/dbt_project/sync_sources/", auth=auth.CanManagePipelines())
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

    task = sync_sources_for_warehouse.delay(orgdbt.id, org_warehouse.id)

    return {"task_id": task.id}


########################## Models & Sources #############################################


@transformapi.post("/dbt_project/model/", auth=auth.CanManagePipelines())
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

    if payload.op_type not in dbtautomation_service.OPERATIONS_DICT.keys():
        raise HttpError(422, "Operation not supported")

    is_multi_input_op = payload.op_type == "join"

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

    input_models: list[OrgDbtModel] = []
    seq: list[int] = []
    other_input_columns: list[list[str]] = []

    logger.info(
        f"Operations chained for the target model {target_model.uuid} : {current_operations_chained}"
    )

    if current_operations_chained == 0:
        if not payload.input_uuid:
            raise HttpError(422, "input is required")

        model = OrgDbtModel.objects.filter(uuid=payload.input_uuid).first()
        if not model:
            raise HttpError(404, "input not found")

        input_models.append(model)

    if is_multi_input_op:  # multi input operation
        if len(payload.other_inputs) == 0:
            raise HttpError(422, "atleast 2 inputs are required for this operation")

        payload.other_inputs.sort(key=lambda x: x.seq)

        for other_input in payload.other_inputs:
            model = OrgDbtModel.objects.filter(uuid=other_input.uuid).first()
            if not model:
                raise HttpError(404, "input not found")
            seq.append(other_input.seq)
            other_input_columns.append(other_input.columns)
            input_models.append(model)

    # we create edges only with tables/models at the start of the chain & not operation nodes
    if current_operations_chained == 0:
        for source in input_models:
            edge = DbtEdge.objects.filter(
                from_node=source, to_node=target_model
            ).first()
            if not edge:
                DbtEdge.objects.create(
                    from_node=source,
                    to_node=target_model,
                )

    logger.info("passed all validation; moving to create operation")

    # source columns or selected columns
    # there will be atleast one input
    OP_CONFIG = payload.config
    OP_CONFIG["source_columns"] = payload.source_columns
    OP_CONFIG["other_inputs"] = []

    # in case of mutli input; send the rest of the inputs in the config; dbt_automation will handle the rest
    for dbtmodel, seq, columns in zip(input_models, seq, other_input_columns):
        OP_CONFIG["other_inputs"].append(
            {
                "input": {
                    "input_type": dbtmodel.type,
                    "input_name": dbtmodel.name,
                    "source_name": dbtmodel.source_name,
                },
                "source_columns": columns,
                "seq": seq,
            }
        )

    input_config = {
        "config": OP_CONFIG,
        "type": payload.op_type,
        "input_models": [
            {
                "uuid": str(model.uuid),
                "name": model.name,
                "display_name": model.display_name,
                "source_name": model.source_name,
                "schema": model.schema,
                "type": model.type,
            }
            for model in input_models
        ],
    }
    output_cols = dbtautomation_service.get_output_cols_for_operation(
        org_warehouse, payload.op_type, OP_CONFIG.copy()
    )
    logger.info("creating operation")

    dbt_op = OrgDbtOperation.objects.create(
        dbtmodel=target_model,
        uuid=uuid.uuid4(),
        seq=current_operations_chained + 1,
        config=input_config,
        output_cols=output_cols,
    )

    logger.info("created operation")

    # save the output cols of the latest operation to the dbt model
    target_model.output_cols = dbt_op.output_cols
    target_model.save()

    logger.info("updated output cols for the model")

    return {
        "id": target_model.uuid,
        "input_type": target_model.type,
        "source_name": target_model.source_name,
        "input_name": target_model.name,
        "schema": target_model.schema,
    }


@transformapi.post(
    "/dbt_project/model/{model_uuid}/save/", auth=auth.CanManagePipelines()
)
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

    model_sql_path, output_cols = dbtautomation_service.create_dbt_model_in_project(
        org_warehouse, orgdbt_model, payload
    )

    orgdbt_model.output_cols = output_cols
    orgdbt_model.sql_path = str(model_sql_path)
    orgdbt_model.under_construction = False
    orgdbt_model.name = slugify(payload.name)
    orgdbt_model.display_name = payload.display_name
    orgdbt_model.schema = payload.dest_schema
    orgdbt_model.save()

    return {
        "id": orgdbt_model.uuid,
        "input_type": orgdbt_model.type,
        "source_name": orgdbt_model.source_name,
        "input_name": orgdbt_model.name,
        "schema": orgdbt_model.schema,
    }


@transformapi.get("/dbt_project/sources_models/", auth=auth.CanManagePipelines())
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
            res.append(
                {
                    "id": orgdbt_model.uuid,
                    "source_name": orgdbt_model.source_name,
                    "input_name": orgdbt_model.name,
                    "input_type": orgdbt_model.type,
                    "schema": orgdbt_model.schema,
                }
            )

    return res


@transformapi.get("/dbt_project/graph/", auth=auth.CanManagePipelines())
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

    for edge in DbtEdge.objects.filter(
        Q(from_node__orgdbt=orgdbt) | Q(to_node__orgdbt=orgdbt)
    ).all():

        model_nodes.append(edge.from_node)
        model_nodes.append(edge.to_node)

    # push operation nodes and edges if any
    for target_node in model_nodes:
        # src_node -> op1 -> op2 -> op3 -> op4
        # start building edges fromt the source
        prev_op = None
        for operation in (
            OrgDbtOperation.objects.filter(dbtmodel=target_node).order_by("seq").all()
        ):
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
            res_nodes.append(
                {
                    "id": node.uuid,
                    "source_name": node.source_name,
                    "input_name": node.name,
                    "input_type": node.type,
                    "schema": node.schema,
                    "type": "src_model_node",
                }
            )

    for node in operation_nodes:
        res_nodes.append(
            {
                "id": node.uuid,
                "output_cols": node.output_cols,
                "config": node.config,
                "type": "operation_node",
                "target_model_id": node.dbtmodel.uuid,
            }
        )

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


@transformapi.delete("/dbt_project/model/{model_uuid}/", auth=auth.CanManagePipelines())
def delete_model(request, model_uuid):
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

    orgdbt_model = OrgDbtModel.objects.filter(uuid=model_uuid).first()
    if not orgdbt_model:
        raise HttpError(404, "model not found")

    operations = OrgDbtOperation.objects.filter(dbtmodel=orgdbt_model).count()

    if operations > 0:
        orgdbt_model.under_construction = True
        orgdbt_model.save()

        # delete the model file is present
        dbtautomation_service.delete_dbt_model_in_project(orgdbt_model)

    return {"success": 1}


@transformapi.delete(
    "/dbt_project/model/operations/{operation_uuid}/", auth=auth.CanManagePipelines()
)
def delete_operation(request, operation_uuid):
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
