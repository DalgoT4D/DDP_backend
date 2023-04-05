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

from ddpui.utils.timezone import IST
from ddpui.utils.ddp_logger import logger
from ddpui.auth import LoginData, UserAuthBearer

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
    """Handle any ninja validation errors raised in the apis"""
    return Response({"error": exc.errors}, status=422)


@clientapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(request, exc: PydanticValidationError):
    """Handle any pydantic errors raised in the apis"""
    return Response({"error": exc.errors()}, status=422)


@clientapi.exception_handler(HttpError)
def ninja_http_error_handler(request, exc: HttpError):
    """Handle any http errors raised in the apis"""
    return Response({"error": " ".join(exc.args)}, status=exc.status_code)


@clientapi.exception_handler(Exception)
def ninja_default_error_handler(request, exc: Exception):
    """Handle any other exception raised in the apis"""
    return Response({"error": " ".join(exc.args)}, status=500)


def runcmd(cmd, cwd):
    """Execute a child program in a new process"""
    return subprocess.Popen(shlex.split(cmd), cwd=str(cwd))


@clientapi.get("/currentuser", auth=UserAuthBearer(), response=OrgUserResponse)
def get_current_user(request):
    """Fetch the current logged in user"""
    return request.auth


@clientapi.post("/organizations/users", response=OrgUserResponse)
def post_organization_user(request, payload: OrgUserCreate):
    """Create an organization user"""
    if OrgUser.objects.filter(email=payload.email).exists():
        raise HttpError(400, f"user having email {payload.email} exists")
    user = OrgUser.objects.create(**payload.dict())
    logger.info(f"created user {payload.email}")
    return user


@clientapi.post("/login/")
def post_login(request, payload: LoginData):
    """Login a client"""
    print(payload)
    if payload.password == "password":
        user = OrgUser.objects.filter(email=payload.email).first()
        if user:
            token = f"fake-auth-token:{user.id}"
            logger.info("returning auth token " + token)
            return {"token": token}
    raise HttpError(400, "login denied")


@clientapi.get(
    "/organizations/users", response=List[OrgUserResponse], auth=UserAuthBearer()
)
def get_organization_users(request):
    """Fetch organization users"""
    assert request.auth
    user = request.auth
    if user.org is None:
        raise HttpError(400, "no associated org")
    return OrgUser.objects.filter(org=user.org)


@clientapi.put("/organizations/users", response=OrgUserResponse, auth=UserAuthBearer())
def put_organization_user(request, payload: OrgUserUpdate):
    """Update an organization user"""
    assert request.auth
    user = request.auth
    if payload.email:
        user.email = payload.email
    if payload.active is not None:
        user.active = payload.active
    user.save()
    logger.info(f"updated user {user.email}")
    return user


@clientapi.post("/organizations/", response=OrgSchema, auth=UserAuthBearer())
def post_organization(request, payload: OrgSchema):
    """Create an organization"""
    logger.info(payload)
    user = request.auth
    if user.org:
        raise HttpError(400, "user already has an associated client")
    org = Org.objects.filter(name=payload.name).first()
    if org:
        raise HttpError(400, "client org already exists")
    org = Org.objects.create(**payload.dict())
    org.slug = slugify(org.name)
    user.org = org
    user.save()
    return org


@clientapi.post(
    "/organizations/users/invite/", response=InvitationSchema, auth=UserAuthBearer()
)
def post_organization_user_invite(request, payload: InvitationSchema):
    """Send an invitation to a user to join platform"""
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
@clientapi.get("/organizations/users/invite/{invite_code}", response=InvitationSchema)
def get_organization_user_invite(request, invite_code):
    """Fetch the invite sent to user with a particular invite code"""
    invitation = Invitation.objects.filter(invite_code=invite_code).first()
    if invitation is None:
        raise HttpError(400, "invalid invite code")
    return InvitationSchema.from_invitation(invitation)


@clientapi.post("/organizations/users/invite/accept/", response=OrgUserResponse)
def post_organization_user_accept_invite(request, payload: AcceptInvitationSchema):
    """User accepting the invite sent with a valid invite code"""
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


@clientapi.post("/airbyte/workspace/detatch", auth=UserAuthBearer())
def post_airbyte_detatch_workspace(request):
    """Detach airbyte workspace from organization"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "org already has no workspace")

    user.org.airbyte_workspace_id = None
    user.org.save()

    return {"success": 1}


@clientapi.post("/airbyte/workspace/", response=AirbyteWorkspace, auth=UserAuthBearer())
def post_airbyte_workspace(request, payload: AirbyteWorkspaceCreate):
    """Create an airbyte workspace"""
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


@clientapi.get("/airbyte/source_definitions", auth=UserAuthBearer())
def get_airbyte_source_definitions(request):
    """Fetch airbyte source definitions in the user organization workspace"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source_definitions(user.org.airbyte_workspace_id)
    logger.debug(res)
    return res


@clientapi.get(
    "/airbyte/source_definitions/{sourcedef_id}/specifications", auth=UserAuthBearer()
)
def get_airbyte_source_definition_specifications(request, sourcedef_id):
    """Fetch definition specifications for a particular source definition in the user organization workspace"""
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


@clientapi.post("/airbyte/sources/", auth=UserAuthBearer())
def post_airbyte_source(request, payload: AirbyteSourceCreate):
    """Create airbyte source in the user organization workspace"""
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


@clientapi.post("/airbyte/sources/{source_id}/check", auth=UserAuthBearer())
def post_airbyte_check_source(request, source_id):
    """Test the source connection in the user organization workspace"""
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


@clientapi.get("/airbyte/sources", auth=UserAuthBearer())
def get_airbyte_sources(request):
    """Fetch all airbyte sources in the user organization workspace"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_sources(user.org.airbyte_workspace_id)
    logger.debug(res)
    return res


@clientapi.get("/airbyte/sources/{source_id}", auth=UserAuthBearer())
def get_airbyte_source(request, source_id):
    """Fetch a single airbyte source in the user organization workspace"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_source(user.org.airbyte_workspace_id, source_id)
    logger.debug(res)
    return res


@clientapi.get("/airbyte/sources/{source_id}/schema_catalog", auth=UserAuthBearer())
def get_airbyte_source_schema_catalog(request, source_id):
    """Fetch schema catalog for a source in the user organization workspace"""
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


@clientapi.get("/airbyte/destination_definitions", auth=UserAuthBearer())
def get_airbyte_destination_definitions(request):
    """Fetch destination definitions in the user organization workspace"""
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
    auth=UserAuthBearer(),
)
def get_airbyte_destination_definition_specifications(request, destinationdef_id):
    """Fetch specifications for a destination definition in the user organization workspace"""
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


@clientapi.post("/airbyte/destinations/", auth=UserAuthBearer())
def post_airbyte_destination(request, payload: AirbyteDestinationCreate):
    """Create an airbyte destination in the user organization workspace"""
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


@clientapi.post("/airbyte/destinations/{destination_id}/check", auth=UserAuthBearer())
def post_airbyte_check_destination(request, destination_id):
    """Test connection to destination in the user organization workspace"""
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


@clientapi.get("/airbyte/destinations", auth=UserAuthBearer())
def get_airbyte_destinations(request):
    """Fetch all airbyte destinations in the user organization workspace"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_destinations(user.org.airbyte_workspace_id)
    logger.debug(res)
    return res


@clientapi.get("/airbyte/destinations/{destination_id}", auth=UserAuthBearer())
def get_airbyte_destination(request, destination_id):
    """Fetch an airbyte destination in the user organization workspace"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_destination(user.org.airbyte_workspace_id, destination_id)
    logger.debug(res)
    return res


@clientapi.get("/airbyte/connections", auth=UserAuthBearer())
def get_airbyte_connections(request):
    """Fetch all airbyte connections in the user organization workspace"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_connections(user.org.airbyte_workspace_id)
    logger.debug(res)
    return res


@clientapi.get("/airbyte/connections/{connection_id}", auth=UserAuthBearer())
def get_airbyte_connection(request, connection_id):
    """Fetch a connection in the user organization workspace"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    res = airbyte_service.get_connection(user.org.airbyte_workspace_id, connection_id)
    logger.debug(res)
    return res


@clientapi.post("/airbyte/connections/", auth=UserAuthBearer())
def post_airbyte_connection(request, payload: AirbyteConnectionCreate):
    """Create an airbyte connection in the user organization workspace"""
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


@clientapi.post("/airbyte/connections/{connection_id}/sync/", auth=UserAuthBearer())
def post_airbyte_sync_connection(request, connection_id):
    """Sync an airbyte connection in the uer organization workspace"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    airbyte_service.sync_connection(user.org.airbyte_workspace_id, connection_id)


@clientapi.post("/dbt/workspace/", auth=UserAuthBearer())
def post_dbt_workspace(request, payload: OrgDbtSchema):
    """Setup the client git repo and install a virtual env inside it to run dbt"""
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


@clientapi.delete("/dbt/workspace/", auth=UserAuthBearer(), response=OrgUserResponse)
def dbt_delete(request):
    """Delete the dbt workspace and project repo created"""
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


@clientapi.post("/dbt/git_pull/", auth=UserAuthBearer())
def post_dbt_git_pull(request):
    """Pull the dbt repo from github for the organization"""
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


@clientapi.post("/prefect/flows/airbyte_sync/", auth=UserAuthBearer())
def post_prefect_airbyte_sync_flow(request, payload: PrefectAirbyteSync):
    """Run airbyte sync flow in prefect"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    if user.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    return prefect_service.run_airbyte_connection_prefect_flow(payload.blockname)


@clientapi.post("/prefect/flows/dbt_run/", auth=UserAuthBearer())
def post_prefect_dbt_core_run_flow(request, payload: PrefectDbtCore):
    """Run dbt flow in prefect"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")

    return prefect_service.run_dbtcore_prefect_flow(payload.blockname)


@clientapi.post("/prefect/blocks/dbt_run/", auth=UserAuthBearer())
def post_prefect_dbt_core_block(request, payload: PrefectDbtRun):
    """Create prefect dbt core block"""
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


@clientapi.get("/prefect/blocks/dbt_run/", auth=UserAuthBearer())
def get_prefect_dbt_run_blocks(request):
    """Fetch all prefect dbt run blocks for an organization"""
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


@clientapi.delete("/prefect/blocks/dbt_run/{block_id}", auth=UserAuthBearer())
def delete_prefect_dbt_run_block(request, block_id):
    """Delete prefect dbt run block for an organization"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    # don't bother checking for user.org.dbt

    prefect_service.delete_dbt_core_block(block_id)
    cpb = OrgPrefectBlock.objects.filter(org=user.org, blockid=block_id).first()
    if cpb:
        cpb.delete()

    return {"success": 1}


@clientapi.post("/prefect/blocks/dbt_test/", auth=UserAuthBearer())
def post_prefect_dbt_test_block(request, payload: PrefectDbtRun):
    """Create prefect dbt test block for an organization"""
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


@clientapi.delete("/prefect/blocks/dbt_test/{block_id}", auth=UserAuthBearer())
def delete_prefect_dbt_test_block(request, block_id):
    """Delete dbt test block for an organization"""
    user = request.auth
    if user.org is None:
        raise HttpError(400, "create an organization first")
    # don't bother checking for user.org.dbt

    prefect_service.delete_dbt_core_block(block_id)
    cpb = OrgPrefectBlock.objects.filter(org=user.org, blockid=block_id).first()
    if cpb:
        cpb.delete()

    return {"success": 1}
