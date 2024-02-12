import os
import shutil
import subprocess
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
from ddpui.ddpprefect.schema import DBTProjectSchema
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgDbt, OrgWarehouse
from ddpui.models.dbt_workflow import OrgDbtModel
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager
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


@transformapi.post("/v1/dbt_project/", auth=auth.CanManagePipelines())
def create_dbt_project(request, payload: DBTProjectSchema):
    """
    Create a new dbt project.
    """
    orguser: OrgUser = request.orguser
    org = orguser.org
    project_name = payload.project_name
    adapter = payload.adapter

    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug
    project_dir.mkdir(parents=True, exist_ok=True)

    existing_projects = [p.stem for p in project_dir.iterdir() if p.is_dir()]
    if project_name in existing_projects:
        raise ValueError(f"A project called {project_name} already exists here.")

    os.chdir(project_dir)

    profile_path = os.path.expanduser("~/.dbt/profiles.yml")
    if os.path.exists(profile_path):
        with open(profile_path, "r") as file:
            profiles = yaml.safe_load(file)
            if profiles is None:
                profiles = {}
    else:
        profiles = {}

    profiles[project_name] = {
        "outputs": {
            "dev": {
                "type": adapter,
                "threads": 1,
                "host": payload.host,
                "port": payload.port,
                "user": payload.user,
                "pass": payload.password,
                "dbname": payload.dbname,
                "schema": payload.schema_,
            }
        },
        "target": "dev",
    }

    with open(profile_path, "w") as file:
        yaml.safe_dump(profiles, file)

    result = subprocess.run(
        ["dbt", "init", project_name, "--skip-profile-setup"], capture_output=True
    )

    if result.returncode != 0:
        raise Exception(f"dbt init command failed with return code {result.returncode}")

    return {"message": f"Project {project_name} created successfully"}


@transformapi.get("/v1/dbt_project/{project_name}", auth=auth.CanManagePipelines())
def get_dbt_project(request, project_name: str):
    """
    Get the details of a dbt project.
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug
    if not project_dir.exists():
        raise ValueError(f"No projects found for organization {org.slug}.")

    existing_projects = [p.stem for p in project_dir.iterdir() if p.is_dir()]
    if project_name not in existing_projects:
        raise ValueError(f"A project called {project_name} does not exist.")

    profile_path = os.path.expanduser("~/.dbt/profiles.yml")
    if not os.path.exists(profile_path):
        raise ValueError("No dbt profiles found.")

    with open(profile_path, "r") as file:
        profiles = yaml.safe_load(file)

    if project_name not in profiles:
        raise ValueError(f"No profile found for project {project_name}.")

    project_profile = profiles[project_name]

    return {
        "message": f"Project {project_name} details retrieved successfully",
        "profile": project_profile,
    }


@transformapi.delete("/v1/dbt_project/{project_name}", auth=auth.CanManagePipelines())
def delete_dbt_project(request, project_name: str):
    """
    Delete a dbt project.
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug
    if not project_dir.exists():
        raise ValueError(f"No projects found for organization {org.slug}.")

    existing_projects = [p.stem for p in project_dir.iterdir() if p.is_dir()]
    if project_name not in existing_projects:
        raise ValueError(f"A project called {project_name} does not exist.")

    profile_path = os.path.expanduser("~/.dbt/profiles.yml")
    if not os.path.exists(profile_path):
        raise ValueError("No dbt profiles found.")

    with open(profile_path, "r") as file:
        profiles = yaml.safe_load(file)

    if project_name not in profiles:
        raise ValueError(f"No profile found for project {project_name}.")

    shutil.rmtree(project_dir / project_name)

    del profiles[project_name]

    with open(profile_path, "w") as file:
        yaml.safe_dump(profiles, file)

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
    )

    return model_to_dict(orgdbt_model, exclude=["orgdbt", "id"])
