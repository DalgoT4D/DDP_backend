import os
import shutil
import subprocess
import shlex
from uuid import uuid4
from pathlib import Path
from typing import List
from datetime import datetime
from ninja import NinjaAPI
from ninja.errors import HttpError, ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError
from django.utils.text import slugify
from django.contrib.auth.models import User
from rest_framework.authtoken import views

from ddpui.utils.timezone import IST
from ddpui.utils.ddp_logger import logger
from ddpui.auth import AuthBearer

from ddpui.models.org_user import OrgUser, OrgUserCreate, OrgUserUpdate, OrgUserResponse
from ddpui.models.org_user import InvitationSchema, Invitation, AcceptInvitationSchema
from ddpui.models.org import Org, OrgSchema, OrgDbt
from ddpui.ddpairbyte.schema import AirbyteWorkspaceCreate, AirbyteWorkspace
from ddpui.ddpairbyte.schema import (
    AirbyteSourceCreate,
    AirbyteDestinationCreate,
    AirbyteConnectionCreate,
)
from ddpui.ddpprefect.schema import PrefectDbtRun, OrgDbtSchema

from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpprefect import prefect_service

from ddpui.ddpprefect.schema import (
    PrefectAirbyteSync,
    PrefectDbtCore,
    PrefectDbtCoreSetup,
    # DbtProfile,
)

from ddpui.ddpprefect.org_prefect_block import OrgPrefectBlock

clientapi = NinjaAPI()
# http://127.0.0.1:8000/api/docs


@clientapi.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):
    """Docstring"""
    return Response({"error": exc.errors}, status=422)


@clientapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(request, exc: PydanticValidationError):
    """Docstring"""
    return Response({"error": exc.errors()}, status=422)


@clientapi.exception_handler(HttpError)
def ninja_http_error_handler(request, exc: HttpError):
    """Docstring"""
    return Response({"error": " ".join(exc.args)}, status=exc.status_code)


@clientapi.exception_handler(Exception)
def ninja_default_error_handler(request, exc: Exception):
    """Docstring"""
    return Response({"error": " ".join(exc.args)}, status=500)


def runcmd(cmd, cwd):
    """runs a shell command in a specified working directory"""
    return subprocess.Popen(shlex.split(cmd), cwd=str(cwd))


@clientapi.get("/currentuser", response=OrgUserResponse, auth=AuthBearer())
def get_current_user(request):
    """return the OrgUser making this request"""
    orguser = OrgUser.objects.filter(user=request.user).first()
    if orguser is not None:
        return OrgUserResponse.from_orguser(orguser)
    raise HttpError(400, "requestor is not an OrgUser")


@clientapi.post("/organizations/users/", response=OrgUserResponse)
def post_organization_user(
    request, payload: OrgUserCreate
):  # pylint: disable=unused-argument
    """creates a new OrgUser having specified email + password. no Org is created or attached at this time"""
    if OrgUser.objects.filter(user__email=payload.email).exists():
        raise HttpError(400, f"user having email {payload.email} exists")
    user = User.objects.create_user(
        username=payload.email, email=payload.email, password=payload.password
    )
    orguser = OrgUser.objects.create(user=user)
    logger.info(f"created user {orguser.user.email}")
    return OrgUserResponse.from_orguser(orguser)


@clientapi.post("/login/")
def post_login(request):
    """Uses the username and password in the request to return an auth token"""
    token = views.obtain_auth_token(request)
    return token


@clientapi.get(
    "/organizations/users", response=List[OrgUserResponse], auth=AuthBearer()
)
def get_organization_users(request):
    """list all OrgUsers in the requestor's org. this will be used by admin OrgUsers once we implement roles"""
    orguser = OrgUser.objects.filter(user=request.user).first()
    if orguser is None:
        raise HttpError(400, "no orguser")
    if orguser.org is None:
        raise HttpError(400, "no associated org")
    query = OrgUser.objects.filter(org=orguser.org)
    return [
        OrgUserResponse(email=orguser.user.email, active=orguser.user.is_active)
        for orguser in query
    ]


@clientapi.put("/organizations/users", response=OrgUserResponse, auth=AuthBearer())
def put_organization_user(request, payload: OrgUserUpdate):
    """update an OrgUser"""
    assert request.auth
    orguser = OrgUser.objects.filter(user=request.user).first()
    if orguser is None:
        raise HttpError(400, "no orguser")

    if payload.email:
        orguser.user.email = payload.email
    if payload.active is not None:
        orguser.user.is_active = payload.active
    orguser.user.save()

    logger.info(f"updated user {orguser.user.email}")
    return OrgUserResponse(email=orguser.user.email, active=orguser.user.is_active)


@clientapi.post("/organizations/", response=OrgSchema, auth=AuthBearer())
def post_organization(request, payload: OrgSchema):
    """creates a new org and attaches it to the requestor"""
    orguser = OrgUser.objects.filter(user=request.user).first()
    if orguser is None:
        raise HttpError(400, "no orguser")
    if orguser.org:
        raise HttpError(400, "orguser already has an associated org")
    org = Org.objects.filter(name=payload.name).first()
    if org:
        raise HttpError(400, "client org already exists")
    org = Org.objects.create(**payload.dict())
    org.slug = slugify(org.name)
    orguser.org = org
    orguser.save()
    return OrgUserResponse(email=orguser.user.email, active=orguser.user.is_active)


@clientapi.post(
    "/organizations/users/invite/", response=InvitationSchema, auth=AuthBearer()
)
def post_organization_user_invite(request, payload: InvitationSchema):
    """Docstring"""
    if request.auth.org is None:
        raise HttpError(400, "an associated organization is required")
    invitation = Invitation.objects.filter(invited_email=payload.invited_email).first()
    if invitation:
        logger.error(
            f"{payload.invited_email} has already been invited by {invitation.invited_by} on {invitation.invited_on.strftime('%Y-%m-%d')}"
        )
        raise HttpError(400, f"{payload.invited_email} has already been invited")

    payload.invited_by = OrgUserResponse(
        email=request.auth.email, org=request.auth.org, active=request.auth.active
    )
    payload.invited_on = datetime.now(IST)
    payload.invite_code = str(uuid4())
    invitation = Invitation.objects.create(
        invited_email=payload.invited_email,
        invited_by=request.auth,
        invited_on=payload.invited_on,
        invite_code=payload.invite_code,
    )
    logger.info("created Invitation")
    return payload


# the invitee will get a hyperlink via email, clicking will take them to \
# the UI where they will choose
# a password, then click a button POSTing to this endpoint
@clientapi.get(
    "/organizations/users/invite/{invite_code}",
    response=InvitationSchema,
    auth=AuthBearer(),
)
def get_organization_user_invite(request, invite_code):
    """Docstring"""
    invitation = Invitation.objects.filter(invite_code=invite_code).first()
    if invitation is None:
        raise HttpError(400, "invalid invite code")
    return InvitationSchema.from_invitation(invitation)


@clientapi.post(
    "/organizations/users/invite/accept/", response=OrgUserResponse, auth=AuthBearer()
)
def post_organization_user_accept_invite(request, payload: AcceptInvitationSchema):
    """Docstring"""
    invitation = Invitation.objects.filter(invite_code=payload.invite_code).first()
    if invitation is None:
        raise HttpError(400, "invalid invite code")
    orguser = OrgUser.objects.filter(
        email=invitation.invited_email, org=invitation.invited_by.org
    ).first()
    if not orguser:
        logger.info(
            f"creating invited user {invitation.invited_email} for {invitation.invited_by.org.name}"
        )
        orguser = OrgUser.objects.create(
            email=invitation.invited_email, org=invitation.invited_by.org
        )
    return orguser


@clientapi.post("/airbyte/workspace/detatch", auth=AuthBearer())
def post_airbyte_detatch_workspace(request):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "org already has no workspace")

    user.org.airbyte_workspace_id = None
    user.org.save()

    return {"success": 1}


@clientapi.post("/airbyte/workspace/", response=AirbyteWorkspace, auth=AuthBearer())
def post_airbyte_workspace(request, payload: AirbyteWorkspaceCreate):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is not None:
        raise HttpError(400, "org already has a workspace")

    workspace = airbyte_service.create_workspace(payload.name)

    user.org.airbyte_workspace_id = workspace["workspaceId"]
    user.org.save()

    return AirbyteWorkspace(
        name=workspace["name"],
        workspaceId=workspace["workspaceId"],
        initialSetupComplete=workspace["initialSetupComplete"],
    )


@clientapi.get("/airbyte/source_definitions", auth=AuthBearer())
def get_airbyte_source_definitions(request):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source_definitions(user.org.airbyte_workspace_id)
    logger.debug(res)
    return res


@clientapi.get(
    "/airbyte/source_definitions/{sourcedef_id}/specifications", auth=AuthBearer()
)
def get_airbyte_source_definition_specifications(request, sourcedef_id):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source_definition_specification(
        user.org.airbyte_workspace_id, sourcedef_id
    )
    logger.debug(res)
    return res


@clientapi.post("/airbyte/sources/", auth=AuthBearer())
def post_airbyte_source(request, payload: AirbyteSourceCreate):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    source = airbyte_service.create_source(
        user.org.airbyte_workspace_id,
        payload.name,
        payload.sourcedef_id,
        payload.config,
    )
    logger.info("created source having id " + source["sourceId"])
    return {"source_id": source["sourceId"]}


@clientapi.post("/airbyte/sources/{source_id}/check", auth=AuthBearer())
def post_airbyte_check_source(request, source_id):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.check_source_connection(
        user.org.airbyte_workspace_id, source_id
    )
    logger.debug(res)
    return res


@clientapi.get("/airbyte/sources", auth=AuthBearer())
def get_airbyte_sources(request):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_sources(user.org.airbyte_workspace_id)
    logger.debug(res)
    return res


@clientapi.get("/airbyte/sources/{source_id}", auth=AuthBearer())
def get_airbyte_source(request, source_id):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source(user.org.airbyte_workspace_id, source_id)
    logger.debug(res)
    return res


@clientapi.get("/airbyte/sources/{source_id}/schema_catalog", auth=AuthBearer())
def get_airbyte_source_schema_catalog(request, source_id):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source_schema_catalog(
        user.org.airbyte_workspace_id, source_id
    )
    logger.debug(res)
    return res


@clientapi.get("/airbyte/destination_definitions", auth=AuthBearer())
def get_airbyte_destination_definitions(request):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_destination_definitions(user.org.airbyte_workspace_id)
    logger.debug(res)
    return res


@clientapi.get(
    "/airbyte/destination_definitions/{destinationdef_id}/specifications/",
    auth=AuthBearer(),
)
def get_airbyte_destination_definition_specifications(request, destinationdef_id):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_destination_definition_specification(
        user.org.airbyte_workspace_id, destinationdef_id
    )
    logger.debug(res)
    return res


@clientapi.post("/airbyte/destinations/", auth=AuthBearer())
def post_airbyte_destination(request, payload: AirbyteDestinationCreate):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    destination = airbyte_service.create_destination(
        user.org.airbyte_workspace_id,
        payload.name,
        payload.destinationdef_id,
        payload.config,
    )
    logger.info("created destination having id " + destination["destinationId"])
    return {"destination_id": destination["destinationId"]}


@clientapi.post("/airbyte/destinations/{destination_id}/check", auth=AuthBearer())
def post_airbyte_check_destination(request, destination_id):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.check_destination_connection(
        user.org.airbyte_workspace_id, destination_id
    )
    logger.debug(res)
    return res


@clientapi.get("/airbyte/destinations", auth=AuthBearer())
def get_airbyte_destinations(request):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_destinations(user.org.airbyte_workspace_id)
    logger.debug(res)
    return res


@clientapi.get("/airbyte/destinations/{destination_id}", auth=AuthBearer())
def get_airbyte_destination(request, destination_id):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_destination(user.org.airbyte_workspace_id, destination_id)
    logger.debug(res)
    return res


@clientapi.get("/airbyte/connections", auth=AuthBearer())
def get_airbyte_connections(request):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_connections(user.org.airbyte_workspace_id)
    logger.debug(res)
    return res


@clientapi.get("/airbyte/connections/{connection_id}", auth=AuthBearer())
def get_airbyte_connection(request, connection_id):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_connection(user.org.airbyte_workspace_id, connection_id)
    logger.debug(res)
    return res


@clientapi.post("/airbyte/connections/", auth=AuthBearer())
def post_airbyte_connection(request, payload: AirbyteConnectionCreate):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    if len(payload.streamnames) == 0:
        raise HttpError(400, "must specify stream names")

    res = airbyte_service.create_connection(user.org.airbyte_workspace_id, payload)
    logger.debug(res)
    return res


@clientapi.post("/airbyte/connections/{connection_id}/sync/", auth=AuthBearer())
def post_airbyte_sync_connection(request, connection_id):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    airbyte_service.sync_connection(user.org.airbyte_workspace_id, connection_id)


@clientapi.post("/dbt/workspace/", auth=AuthBearer())
def post_dbt_workspace(request, payload: OrgDbtSchema):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.dbt is not None:
        raise HttpError(400, "dbt is already configured for this client")

    if user.org.slug is None:
        user.org.slug = slugify(user.org.name)
        user.org.save()

    # this client'a dbt setup happens here
    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / user.org.slug
    if project_dir.exists():
        shutil.rmtree(str(project_dir))
    project_dir.mkdir()

    # clone the client's dbt repo into "dbtrepo/" under the project_dir
    process = runcmd(f"git clone {payload.gitrepo_url} dbtrepo", project_dir)
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
    process = runcmd(f"{pip} install dbt-core=={payload.dbtversion}", project_dir)
    if process.wait() != 0:
        raise HttpError(500, f"pip install dbt-core=={payload.dbtversion} failed")
    process = runcmd(f"{pip} install dbt-postgres==1.4.5", project_dir)
    if process.wait() != 0:
        raise HttpError(500, "pip install dbt-postgres==1.4.5 failed")

    dbt = OrgDbt(
        gitrepo_url=payload.gitrepo_url,
        project_dir=str(project_dir),
        dbtversion=payload.dbtversion,
        targetname=payload.profile.target,
        targettype=payload.profile.target_configs_type,
        targetschema=payload.profile.target_configs_schema,
        host=payload.credentials.host,
        port=payload.credentials.port,
        username=payload.credentials.username,
        password=payload.credentials.password,  # todo: encrypt with kms
        database=payload.credentials.database,
    )
    dbt.save()
    user.org.dbt = dbt
    user.org.save()

    return {"success": 1}


@clientapi.delete("/dbt/workspace/", response=OrgUserResponse, auth=AuthBearer())
def dbt_delete(request):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.dbt is None:
        raise HttpError(400, "dbt is not configured for this client")

    dbt = user.org.dbt
    user.org.dbt = None
    user.org.save()

    shutil.rmtree(dbt.project_dir)
    dbt.delete()

    return user


@clientapi.post("/dbt/git_pull/", auth=AuthBearer())
def post_dbt_git_pull(request):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.dbt is None:
        raise HttpError(400, "dbt is not configured for this client")

    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / user.org.slug
    if not os.path.exists(project_dir):
        raise HttpError(400, "create the dbt env first")

    process = runcmd("git pull", project_dir / "dbtrepo")
    if process.wait() != 0:
        raise HttpError(500, f"git pull failed in {str(project_dir / 'dbtrepo')}")

    return {"success": True}


@clientapi.post("/prefect/flows/airbyte_sync/", auth=AuthBearer())
def post_prefect_airbyte_sync_flow(request, payload: PrefectAirbyteSync):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    return prefect_service.run_airbyte_connection_prefect_flow(payload.blockname)


@clientapi.post("/prefect/flows/dbt_run/", auth=AuthBearer())
def post_prefect_dbt_core_run_flow(request, payload: PrefectDbtCore):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")

    return prefect_service.run_dbtcore_prefect_flow(payload.blockname)


@clientapi.post("/prefect/blocks/dbt_run/", auth=AuthBearer())
def post_prefect_dbt_core_block(request, payload: PrefectDbtRun):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.dbt is None:
        raise HttpError(400, "create a dbt workspace first")

    dbt_env_dir = Path(os.getenv("CLIENTDBT_ROOT")) / user.org.slug
    if not os.path.exists(dbt_env_dir):
        raise HttpError(400, "create the dbt env first")

    dbt_binary = str(dbt_env_dir / "venv/bin/dbt")
    project_dir = str(dbt_env_dir / "dbtrepo")

    block_data = PrefectDbtCoreSetup(
        blockname=payload.dbt_blockname,
        profiles_dir=f"{project_dir}/profiles/",
        project_dir=project_dir,
        working_dir=project_dir,
        env={},
        commands=[f"{dbt_binary} run --target {payload.profile.target}"],
    )

    block = prefect_service.create_dbt_core_block(
        block_data, payload.profile, payload.credentials
    )

    cpb = OrgPrefectBlock(
        org=user.org,
        blocktype=block["block_type"]["name"],
        blockid=block["id"],
        blockname=block["name"],
    )
    cpb.save()

    return block


@clientapi.get("/prefect/blocks/dbt_run/", auth=AuthBearer())
def get_prefect_dbt_run_blocks(request):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")

    return [
        {
            "blocktype": x.blocktype,
            "blockid": x.blockid,
            "blockname": x.blockname,
        }
        for x in OrgPrefectBlock.objects.filter(
            org=user.org, blocktype=prefect_service.DBTCORE
        )
    ]


@clientapi.delete("/prefect/blocks/dbt_run/{block_id}", auth=AuthBearer())
def delete_prefect_dbt_run_block(request, block_id):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    # don't bother checking for user.org.dbt

    prefect_service.delete_dbt_core_block(block_id)
    cpb = OrgPrefectBlock.objects.filter(org=user.org, blockid=block_id).first()
    if cpb:
        cpb.delete()

    return {"success": 1}


@clientapi.post("/prefect/blocks/dbt_test/", auth=AuthBearer())
def post_prefect_dbt_test_block(request, payload: PrefectDbtRun):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.dbt is None:
        raise HttpError(400, "create a dbt workspace first")

    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / user.org.slug
    if not os.path.exists(project_dir):
        raise HttpError(400, "create the dbt env first")

    project_dir = project_dir / "dbtrepo"
    dbt_binary = project_dir / "venv/bin/dbt"

    block_data = PrefectDbtCoreSetup(
        blockname=payload.dbt_blockname,
        profiles_dir=f"{project_dir}/profiles/",
        project_dir=project_dir,
        working_dir=project_dir,
        env={},
        commands=[f"{dbt_binary} test --target {payload.target}"],
    )

    block = prefect_service.create_dbt_core_block(
        block_data, payload.profile, payload.credentials
    )

    cpb = OrgPrefectBlock(
        org=user.org,
        blocktype=block["block_type"]["name"],
        blockid=block["id"],
        blockname=block["name"],
    )
    cpb.save()

    return block


@clientapi.delete("/prefect/blocks/dbt_test/{block_id}", auth=AuthBearer())
def delete_prefect_dbt_test_block(request, block_id):
    """Docstring"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    # don't bother checking for user.org.dbt

    prefect_service.delete_dbt_core_block(block_id)
    cpb = OrgPrefectBlock.objects.filter(org=user.org, blockid=block_id).first()
    if cpb:
        cpb.delete()

    return {"success": 1}
