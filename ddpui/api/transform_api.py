import os
import shutil
from pathlib import Path
import json

import yaml
from dotenv import load_dotenv
from django.forms.models import model_to_dict
from django.utils.text import slugify
from ninja import NinjaAPI
from ninja.errors import ValidationError, HttpError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError

from ddpui import auth
from ddpui.ddpdbt.dbt_service import setup_local_dbt_workspace
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgDbt, OrgWarehouse
from ddpui.models.dbt_workflow import OrgDbtModel
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager
from ddpui.schemas.org_task_schema import DbtProjectSchema
from ddpui.schemas.dbt_workflow_schema import CreateDbtModelPayload
from dbt_automation.utils import warehouseclient
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


@transformapi.get("/dbt_project/", auth=auth.CanManagePipelines())
def get_dbt_project(request):
    """
    Get information about the dbt project in this org.
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    if not org.dbt:
        return {"message": f"No dbt project found in organization {org.slug}"}

    dbt_info = {
        "target_type": org.dbt.target_type,
        "default_schema": org.dbt.default_schema,
        "gitrepo_url": org.dbt.gitrepo_url,
    }

    return dbt_info


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


########################## Models #############################################


@transformapi.post("/model", auth=auth.CanManagePipelines())
def post_dbt_model(request, payload: CreateDbtModelPayload):
    """
    Create a model on local disk and save configuration to django db
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

    sql_path, error = dbtautomation_service.create_dbt_model_in_project(
        orgdbt, org_warehouse, payload.op_type, payload.config
    )
    if error:
        raise HttpError(422, error)

    orgdbt_model = OrgDbtModel.objects.create(
        orgdbt=orgdbt,
        name=slugify(payload.name),
        display_name=payload.display_name,
        sql_path=sql_path,
        config=payload.config,
    )

    return model_to_dict(orgdbt_model, exclude=["orgdbt", "id"])
