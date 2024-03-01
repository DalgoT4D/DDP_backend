import os, uuid
import shutil
from pathlib import Path

from dotenv import load_dotenv
from django.forms.models import model_to_dict
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
    SyncSourcesSchema,
    CompleteDbtModelPayload,
)

from ddpui.core import dbtautomation_service

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
def sync_sources(request, payload: SyncSourcesSchema):
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

    sources_file_path, error = dbtautomation_service.sync_sources_to_dbt(
        payload.schema_name, payload.source_name, org, org_warehouse
    )

    if error:
        raise HttpError(422, error)

    # sync sources to django db
    logger.info("synced sources in dbt, saving to db now")
    sources = dbtautomation_service.read_dbt_sources_in_project(orgdbt)
    for source in sources:
        orgdbt_source = OrgDbtModel.objects.filter(
            source_name=source["source_name"], name=source["input_name"], type="source"
        ).first()

        if not orgdbt_source:
            orgdbt_source = OrgDbtModel(
                uuid=uuid.uuid4(),
                orgdbt=orgdbt,
                source_name=source["source_name"],
                name=source["input_name"],
                display_name=source["input_name"],
                type="source",
            )

        orgdbt_source.schema = source["schema"]
        orgdbt_source.sql_path = sources_file_path

        orgdbt_source.save()

    return {"sources_file_path": str(sources_file_path)}


########################## Models & Sources #############################################


@transformapi.post("/dbt_project/model/", auth=auth.CanManagePipelines())
def post_construct_dbt_model_operation(request, payload: CreateDbtModelPayload):
    """
    Construct a model, operation and the edge in django db
    """
    OP_CONFIG = payload.config

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

    if len(payload.input_uuids) == 0:
        raise HttpError(422, "no input provided")

    input_models = OrgDbtModel.objects.filter(uuid__in=payload.input_uuids).all()
    if len(input_models) != len(payload.input_uuids):
        raise HttpError(404, "input not found")

    input_arr = [
        {
            "input_name": input.name,
            "input_type": input.type,
            "source_name": input.source_name,
        }
        for input in input_models
    ]

    # input according to dbt_automation packag
    if len(input_arr) == 1:  # single input operation
        payload.config["input"] = input_arr[0]
    else:  # multi inputs operation
        payload.config["input"] = input_arr

    logger.info("passed all validation; moving to create operation")
    logger.info(f"no of inputs for the operation {len(input_arr)}")

    orgdbt_model = None
    if payload.model_uuid:
        orgdbt_model = OrgDbtModel.objects.filter(uuid=payload.model_uuid).first()

    # only under construction models can be modified
    if orgdbt_model and not orgdbt_model.under_construction:
        raise HttpError(422, "model is locked")

    if not orgdbt_model:
        orgdbt_model = OrgDbtModel.objects.create(
            uuid=uuid.uuid4(),
            orgdbt=orgdbt,
            under_construction=True,
        )

    # source columns or selected columns
    OP_CONFIG["source_columns"] = payload.select_columns
    input_config = {
        "config": OP_CONFIG,
        "type": payload.op_type,
        "input_uuids": [payload.input_uuids],
    }
    output_cols = dbtautomation_service.get_output_cols_for_operation(
        org_warehouse, payload.op_type, OP_CONFIG
    )

    logger.info("creating operation")

    dbt_op = OrgDbtOperation.objects.create(
        dbtmodel=orgdbt_model,
        uuid=uuid.uuid4(),
        seq=OrgDbtOperation.objects.filter(dbtmodel=orgdbt_model).count() + 1,
        config=input_config,
        output_cols=output_cols,
    )

    logger.info("created operation")

    # save the output cols of the latest operation to the dbt model
    orgdbt_model.output_cols = dbt_op.output_cols
    orgdbt_model.save()

    logger.info("updated output cols for the model")

    # create edge if it doesn't exist
    for source in input_models:
        edge = DbtEdge.objects.filter(from_node=source, to_node=orgdbt_model).first()
        if not edge:
            DbtEdge.objects.create(
                from_node=source,
                to_node=orgdbt_model,
            )

    return {
        "id": orgdbt_model.uuid,
        "input_type": orgdbt_model.type,
        "source_name": orgdbt_model.source_name,
        "input_name": orgdbt_model.name,
        "schema": orgdbt_model.schema,
    }


@transformapi.post(
    "/dbt_project/model/{model_uuid}/save", auth=auth.CanManagePipelines()
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

    sql_path, error = dbtautomation_service.create_dbt_model_in_project(
        orgdbt, org_warehouse, payload
    )
    if error:
        raise HttpError(422, error)

    orgdbt_model.sql_path = sql_path
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

    edges = DbtEdge.objects.filter(
        Q(from_node__orgdbt=orgdbt) | Q(to_node__orgdbt=orgdbt)
    ).all()

    res = {"nodes": [], "edges": []}

    for edge in edges:
        # append related nodes
        res["nodes"].extend(
            [
                {
                    "id": edge.from_node.uuid,
                    "source_name": edge.from_node.source_name,
                    "input_name": edge.from_node.name,
                    "input_type": edge.from_node.type,
                    "schema": edge.from_node.schema,
                },
                {
                    "id": edge.to_node.uuid,
                    "source_name": edge.to_node.source_name,
                    "input_name": edge.to_node.name,
                    "input_type": edge.to_node.type,
                    "schema": edge.to_node.schema,
                },
            ]
        )
        res["edges"].append(
            {
                "id": edge.id,
                "source": edge.from_node.uuid,
                "target": edge.to_node.uuid,
            }
        )

    # set to remove duplicates
    seen = set()
    res["nodes"] = [
        nn for nn in res["nodes"] if not (nn["id"] in seen or seen.add(nn["id"]))
    ]

    return res
