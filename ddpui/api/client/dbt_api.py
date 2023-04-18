import os
import shutil
from pathlib import Path
import json

from django.utils.text import slugify
from ninja import NinjaAPI
from ninja.errors import HttpError, ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError

from ddpui import auth
from ddpui.ddpprefect.schema import OrgDbtSchema
from ddpui.models.org import OrgDbt
from ddpui.models.org_user import OrgUserResponse
from ddpui.utils.helpers import runcmd
from ddpui.utils import secretsmanager

dbtapi = NinjaAPI(urls_namespace="dbt")


@dbtapi.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """Handle any ninja validation errors raised in the apis"""
    return Response({"error": exc.errors}, status=422)


@dbtapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """Handle any pydantic errors raised in the apis"""
    return Response({"error": exc.errors()}, status=422)


@dbtapi.exception_handler(HttpError)
def ninja_http_error_handler(
    request, exc: HttpError
):  # pylint: disable=unused-argument
    """Handle any http errors raised in the apis"""
    return Response({"error": " ".join(exc.args)}, status=exc.status_code)


@dbtapi.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception
):  # pylint: disable=unused-argument
    """Handle any other exception raised in the apis"""
    return Response({"error": " ".join(exc.args)}, status=500)


@dbtapi.post("/workspace/", auth=auth.CanManagePipelines())
def post_dbt_workspace(request, payload: OrgDbtSchema):
    """Setup the client git repo and install a virtual env inside it to run dbt"""
    orguser = request.orguser
    if orguser.org.dbt is not None:
        raise HttpError(400, "dbt is already configured for this client")

    if orguser.org.slug is None:
        orguser.org.slug = slugify(orguser.org.name)
        orguser.org.save()

    # this client'a dbt setup happens here
    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / orguser.org.slug
    if project_dir.exists():
        shutil.rmtree(str(project_dir))
    project_dir.mkdir()

    # clone the client's dbt repo into "dbtrepo/" under the project_dir
    # if we have an access token with the "contents" and "metadata" permissions then
    #   git clone https://oauth2:[TOKEN]@github.com/[REPO-OWNER]/[REPO-NAME]
    if payload.gitrepoAccessToken is not None:
        gitrepo_url = payload.gitrepoUrl.replace(
            "github.com", "oauth2:" + payload.gitrepoAccessToken + "@github.com"
        )
        process = runcmd(f"git clone {gitrepo_url} dbtrepo", project_dir)
    else:
        process = runcmd(f"git clone {payload.gitrepoUrl} dbtrepo", project_dir)
    if process.wait() != 0:
        raise HttpError(500, "git clone failed")

    # install a dbt venv
    process = runcmd("python -m venv venv", project_dir)
    if process.wait() != 0:
        raise HttpError(500, "make venv failed")
    pip = project_dir / "venv/bin/pip"
    process = runcmd(f"{pip} install --upgrade pip", project_dir)
    if process.wait() != 0:
        raise HttpError(500, "pip --upgrade failed")
    # install dbt in the new env
    process = runcmd(f"{pip} install dbt-core=={payload.dbtVersion}", project_dir)
    if process.wait() != 0:
        raise HttpError(500, f"pip install dbt-core=={payload.dbtVersion} failed")

    if payload.profile.target_configs_type == "postgres":
        process = runcmd(f"{pip} install dbt-postgres==1.4.5", project_dir)
        if process.wait() != 0:
            raise HttpError(500, "pip install dbt-postgres==1.4.5 failed")
        credentials = json.dumps(
            {
                "host": payload.credentials.host,
                "port": payload.credentials.port,
                "username": payload.credentials.username,
                "password": payload.credentials.password,
                "database": payload.credentials.database,
            }
        )
    elif payload.profile.target_configs_type == "bigquery":
        process = runcmd(f"{pip} install dbt-bigquery==1.4.3", project_dir)
        if process.wait() != 0:
            raise HttpError(500, "pip install dbt-bigquery==1.4.3 failed")
        credentials = ""

    else:
        raise Exception("what warehouse is this")

    dbt = OrgDbt(
        gitrepo_url=payload.gitrepoUrl,
        project_dir=str(project_dir),
        dbt_version=payload.dbtVersion,
        target_name=payload.profile.target,
        target_type=payload.profile.target_configs_type,
        target_schema=payload.profile.target_configs_schema,
        credentials=credentials,
    )
    dbt.save()
    orguser.org.dbt = dbt
    orguser.org.save()

    if payload.gitrepoAccessToken is not None:
        secretsmanager.delete_github_token(orguser.org)
        secretsmanager.save_github_token(orguser.org, payload.gitrepoAccessToken)

    return {"success": 1}


@dbtapi.delete("/workspace/", response=OrgUserResponse, auth=auth.CanManagePipelines())
def dbt_delete(request):
    """Delete the dbt workspace and project repo created"""
    orguser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")
    if orguser.org.dbt is None:
        raise HttpError(400, "dbt is not configured for this client")

    dbt = orguser.org.dbt
    orguser.org.dbt = None
    orguser.org.save()

    shutil.rmtree(dbt.project_dir)
    dbt.delete()

    secretsmanager.delete_github_token(orguser.org)

    return OrgUserResponse.from_orguser(orguser)


@dbtapi.post("/git_pull/", auth=auth.CanManagePipelines())
def post_dbt_git_pull(request):
    """Pull the dbt repo from github for the organization"""
    orguser = request.orguser
    if orguser.org.dbt is None:
        raise HttpError(400, "dbt is not configured for this client")

    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / orguser.org.slug
    if not os.path.exists(project_dir):
        raise HttpError(400, "create the dbt env first")

    process = runcmd("git pull", project_dir / "dbtrepo")
    if process.wait() != 0:
        raise HttpError(500, f"git pull failed in {str(project_dir / 'dbtrepo')}")

    return {"success": True}
