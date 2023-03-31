import os
from ninja import NinjaAPI
from ninja.errors import HttpError
from datetime import datetime
from uuid import uuid4
from pathlib import Path
import shutil
from typing import List
import subprocess
import shlex
from django.utils.text import slugify

from ddpui.utils.timezone import IST
from ddpui.utils.ddplogger import logger
from ddpui.auth import LoginData, UserAuthBearer

clientapi = NinjaAPI()
# http://127.0.0.1:8000/api/docs

from ddpui.models.orguser import OrgUser, OrgUserCreate, OrgUserUpdate, OrgUserResponse
from ddpui.models.orguser import InvitationSchema, Invitation, AcceptInvitationSchema
from ddpui.models.org import Org, OrgSchema, OrgDbt

from ddpui.ddpairbyte.schemas import AirbyteWorkspaceCreate, AirbyteWorkspace
from ddpui.ddpairbyte.schemas import AirbyteSourceCreate, AirbyteDestinationCreate, AirbyteConnectionCreate

from ddpui.ddpairbyte import functions
from ddpui.ddpprefect import functions

from ddpui.ddpprefect.schemas import PrefectAirbyteSync, PrefectDbtCore, PrefectDbtCoreSetup, DbtProfile
from ddpui.ddpprefect.schemas import DbtCredentialsPostgres, PrefectDbtRun, OrgDbtSchema

from ddpui.ddpprefect.orgprefectblock import OrgPrefectBlock

# ====================================================================================================
def runcmd(cmd, cwd):
  return subprocess.Popen(shlex.split(cmd), cwd=str(cwd))

# ====================================================================================================
@clientapi.get("/currentuser", auth=UserAuthBearer(), response=OrgUserResponse)
def currentuser(request):
  return request.auth

# ====================================================================================================
@clientapi.post("/createuser/", response=OrgUserResponse)
def createuser(request, payload: OrgUserCreate):
  if OrgUser.objects.filter(email=payload.email).exists():
    raise HttpError(400, f"user having email {payload.email} exists")
  user = OrgUser.objects.create(**payload.dict())
  logger.info(f"created user {payload.email}")
  return user

# ====================================================================================================
@clientapi.post("/login/")
def login(request, payload: LoginData):
  print(payload)
  if payload.password == 'password':
    user = OrgUser.objects.filter(email=payload.email).first()
    if user:
      token = f"fake-auth-token:{user.id}"
      logger.info("returning auth token " + token)
      return {'token': token}
  raise HttpError(400, "login denied")

# ====================================================================================================
@clientapi.get("/users", response=List[OrgUserResponse], auth=UserAuthBearer())
def users(request):
  assert(request.auth)
  user = request.auth
  if user.org is None:
    raise HttpError(400, "no associated org")
  return OrgUser.objects.filter(org=user.org)

# ====================================================================================================
@clientapi.post("/updateuser/", response=OrgUserResponse, auth=UserAuthBearer())
def updateuser(request, payload: OrgUserUpdate):
  assert(request.auth)
  user = request.auth
  if payload.email:
    user.email = payload.email
  if payload.active is not None:
    user.active = payload.active
  user.save()
  logger.info(f"updated user {user.email}")
  return user

# ====================================================================================================
@clientapi.post('/client/create/', response=OrgSchema, auth=UserAuthBearer())
def createclient(request, payload: OrgSchema):
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

# ====================================================================================================
@clientapi.post('/user/invite/', response=InvitationSchema, auth=UserAuthBearer())
def inviteuser(request, payload: InvitationSchema):
  if request.auth.org is None:
    raise HttpError(400, "an associated organization is required")
  x = Invitation.objects.filter(invited_email=payload.invited_email).first()
  if x:
    logger.error(f"{payload.invited_email} has already been invited by {x.invited_by} on {x.invited_on.strftime('%Y-%m-%d')}")
    raise HttpError(400, f'{payload.invited_email} has already been invited')

  payload.invited_by = OrgUserResponse(email=request.auth.email, org=request.auth.org, active=request.auth.active)
  payload.invited_on = datetime.now(IST)
  payload.invite_code = str(uuid4())
  x = Invitation.objects.create(
    invited_email=payload.invited_email,
    invited_by=request.auth,
    invited_on=payload.invited_on,
    invite_code=payload.invite_code,
  )
  logger.info('created Invitation')
  return payload

# ====================================================================================================
# the invitee will get a hyperlink via email, clicking will take them to the UI where they will choose
# a password, then click a button POSTing to this endpoint
@clientapi.get('/user/getinvitedetails/{invite_code}', response=InvitationSchema)
def getinvitedetails(request, invite_code):
  x = Invitation.objects.filter(invite_code=invite_code).first()
  if x is None:
    raise HttpError(400, "invalid invite code")
  return InvitationSchema.from_invitation(x)

# ====================================================================================================
@clientapi.post('/user/acceptinvite/', response=OrgUserResponse)
def acceptinvite(request, payload: AcceptInvitationSchema):
  x = Invitation.objects.filter(invite_code=payload.invite_code).first()
  if x is None:
    raise HttpError(400, "invalid invite code")
  orguser = OrgUser.objects.filter(email=x.invited_email, org=x.invited_by.org).first()
  if not orguser:
    logger.info(f"creating invited user {x.invited_email} for {x.invited_by.org.name}")
    orguser = OrgUser.objects.create(email=x.invited_email, org=x.invited_by.org)
  return orguser
  
# ====================================================================================================
@clientapi.post('/airbyte/detatchworkspace/', auth=UserAuthBearer())
def airbyte_detatchworkspace(request):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "org already has no workspace")
  
  user.org.airbyte_workspace_id = None
  user.org.save()

  return {"success": 1}

# ====================================================================================================
@clientapi.post('/airbyte/createworkspace/', response=AirbyteWorkspace, auth=UserAuthBearer())
def airbyte_createworkspace(request, payload: AirbyteWorkspaceCreate):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is not None:
    raise HttpError(400, "org already has a workspace")

  workspace = functions.createworkspace(payload.name)

  user.org.airbyte_workspace_id = workspace['workspaceId']
  user.org.save()

  return AirbyteWorkspace(
    name=workspace['name'],
    workspaceId=workspace['workspaceId'],
    initialSetupComplete=workspace['initialSetupComplete']
  )

# ====================================================================================================
@clientapi.get('/airbyte/getsourcedefinitions', auth=UserAuthBearer())
def airbyte_getsources(request):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "create an airbyte workspace first")

  r = functions.getsourcedefinitions(user.org.airbyte_workspace_id)
  logger.debug(r)
  return r

# ====================================================================================================
@clientapi.get('/airbyte/getsourcedefinitionspecification/{sourcedef_id}', auth=UserAuthBearer())
def airbyte_getsourcedefinitionspecification(request, sourcedef_id):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "create an airbyte workspace first")

  r = functions.getsourcedefinitionspecification(user.org.airbyte_workspace_id, sourcedef_id)
  logger.debug(r)
  return r

# ====================================================================================================
@clientapi.post('/airbyte/createsource/', auth=UserAuthBearer())
def airbyte_createsource(request, payload: AirbyteSourceCreate):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "create an airbyte workspace first")

  source = functions.createsource(user.org.airbyte_workspace_id, payload.name, payload.sourcedef_id, payload.config)
  logger.info("created source having id " + source['sourceId'])
  return {'source_id': source['sourceId']}

# ====================================================================================================
@clientapi.post('/airbyte/checksource/{source_id}/', auth=UserAuthBearer())
def airbyte_checksource(request, source_id):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "create an airbyte workspace first")

  r = functions.checksourceconnection(user.org.airbyte_workspace_id, source_id)
  logger.debug(r)
  return r

# ====================================================================================================
@clientapi.get('/airbyte/getsources', auth=UserAuthBearer())
def airbyte_getsources(request):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "create an airbyte workspace first")

  r = functions.getsources(user.org.airbyte_workspace_id)
  logger.debug(r)
  return r

# ====================================================================================================
@clientapi.get('/airbyte/getsource/{source_id}', auth=UserAuthBearer())
def airbyte_getsources(request, source_id):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "create an airbyte workspace first")

  r = functions.getsource(user.org.airbyte_workspace_id, source_id)
  logger.debug(r)
  return r

# ====================================================================================================
@clientapi.get('/airbyte/getsourceschemacatalog/{source_id}', auth=UserAuthBearer())
def airbyte_getsourceschemacatalog(request, source_id):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "create an airbyte workspace first")

  r = functions.getsourceschemacatalog(user.org.airbyte_workspace_id, source_id)
  logger.debug(r)
  return r

# ====================================================================================================
@clientapi.get('/airbyte/getdestinationdefinitions', auth=UserAuthBearer())
def airbyte_getdestinations(request):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "create an airbyte workspace first")

  r = functions.getdestinationdefinitions(user.org.airbyte_workspace_id)
  logger.debug(r)
  return r

# ====================================================================================================
@clientapi.get('/airbyte/getdestinationdefinitionspecification/{destinationdef_id}', auth=UserAuthBearer())
def airbyte_getdestinationdefinitionspecification(request, destinationdef_id):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "create an airbyte workspace first")

  r = functions.getdestinationdefinitionspecification(user.org.airbyte_workspace_id, destinationdef_id)
  logger.debug(r)
  return r

# ====================================================================================================
@clientapi.post('/airbyte/createdestination/', auth=UserAuthBearer())
def airbyte_createsource(request, payload: AirbyteDestinationCreate):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "create an airbyte workspace first")

  destination = functions.createdestination(user.org.airbyte_workspace_id, payload.name, payload.destinationdef_id, payload.config)
  logger.info("created destination having id " + destination['destinationId'])
  return {'destination_id': destination['destinationId']}

# ====================================================================================================
@clientapi.post('/airbyte/checkdestination/{destination_id}/', auth=UserAuthBearer())
def airbyte_checkdestination(request, destination_id):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "create an airbyte workspace first")

  r = functions.checkdestinationconnection(user.org.airbyte_workspace_id, destination_id)
  logger.debug(r)
  return r

# ====================================================================================================
@clientapi.get('/airbyte/getdestinations', auth=UserAuthBearer())
def airbyte_getdestinations(request):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "create an airbyte workspace first")

  r = functions.getdestinations(user.org.airbyte_workspace_id)
  logger.debug(r)
  return r

# ====================================================================================================
@clientapi.get('/airbyte/getdestination/{destination_id}', auth=UserAuthBearer())
def airbyte_getdestinations(request, destination_id):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "create an airbyte workspace first")

  r = functions.getdestination(user.org.airbyte_workspace_id, destination_id)
  logger.debug(r)
  return r

# ====================================================================================================
@clientapi.get('/airbyte/getconnections', auth=UserAuthBearer())
def airbyte_getconnections(request):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "create an airbyte workspace first")

  r = functions.getconnections(user.org.airbyte_workspace_id)
  logger.debug(r)
  return r

# ====================================================================================================
@clientapi.get('/airbyte/getconnection/{connection_id}', auth=UserAuthBearer())
def airbyte_getconnections(request, connection_id):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "create an airbyte workspace first")

  r = functions.getconnection(user.org.airbyte_workspace_id, connection_id)
  logger.debug(r)
  return r

# ====================================================================================================
@clientapi.post('/airbyte/createconnection/', auth=UserAuthBearer())
def airbyte_createconnection(request, payload: AirbyteConnectionCreate):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "create an airbyte workspace first")
  
  if len(payload.streamnames) == 0:
    raise HttpError(400, "must specify stream names")

  r = functions.createconnection(user.org.airbyte_workspace_id, payload)
  logger.debug(r)
  return r

# ====================================================================================================
@clientapi.post('/airbyte/syncconnection/{connection_id}/', auth=UserAuthBearer())
def airbyte_syncconnection(request, connection_id):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "create an airbyte workspace first")
  
  functions.syncconnection(user.org.airbyte_workspace_id, connection_id)

# ====================================================================================================
@clientapi.post('/dbt/createworkspace/', auth=UserAuthBearer())
def dbt_create(request, payload: OrgDbtSchema):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.dbt is not None:
    raise HttpError(400, "dbt is already configured for this client")
  
  if user.org.slug is None:
    user.org.slug = slugify(user.org.name)
    user.org.save()

  # this client'a dbt setup happens here
  project_dir = Path(os.getenv('CLIENTDBT_ROOT')) / user.org.slug
  if project_dir.exists():
    shutil.rmtree(str(project_dir))
  project_dir.mkdir()

  # clone the client's dbt repo into "dbtrepo/" under the project_dir
  p = runcmd(f"git clone {payload.gitrepo_url} dbtrepo", project_dir)
  if p.wait() != 0:
    raise HttpError(500, "git clone failed")

  # install a dbt venv
  p = runcmd("python -m venv venv", project_dir)
  if p.wait() != 0:
    raise HttpError(500, "make venv failed")
  pip = project_dir / "venv/bin/pip"
  p = runcmd(f"{pip} install --upgrade pip", project_dir)
  if p.wait() != 0:
    raise HttpError(500, "pip --upgrade failed")
  # install dbt in the new env
  p = runcmd(f"{pip} install dbt-core=={payload.dbtversion}", project_dir)
  if p.wait() != 0:
    raise HttpError(500, f"pip install dbt-core=={payload.dbtversion} failed")
  p = runcmd(f"{pip} install dbt-postgres==1.4.5", project_dir)
  if p.wait() != 0:
    raise HttpError(500, f"pip install dbt-postgres==1.4.5 failed")

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
    password=payload.credentials.password, # todo: encrypt with kms
    database=payload.credentials.database,
  )
  dbt.save()
  user.org.dbt = dbt
  user.org.save()

  return {"success": 1}

# ====================================================================================================
@clientapi.post('/dbt/deleteworkspace/', auth=UserAuthBearer(), response=OrgUserResponse)
def dbt_delete(request):
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

# ====================================================================================================
@clientapi.post('/dbt/git-pull/', auth=UserAuthBearer())
def dbt_gitpull(request):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.dbt is None:
    raise HttpError(400, "dbt is not configured for this client")
  
  project_dir = Path(os.getenv('CLIENTDBT_ROOT')) / user.org.slug
  if not os.path.exists(project_dir):
    raise HttpError(400, "create the dbt env first")

  p = runcmd("git pull", project_dir / "dbtrepo")
  if p.wait() != 0:
    raise HttpError(500, f"git pull failed in {str(project_dir / 'dbtrepo')}")
  
  return {'success': True}
    
# ====================================================================================================
@clientapi.post('/prefect/createairbytesyncjob/', auth=UserAuthBearer())
def create_prefect_flow_to_run_airbyte_sync(request, payload: PrefectAirbyteSync):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.airbyte_workspace_id is None:
    raise HttpError(400, "create an airbyte workspace first")

  return functions.run_airbyte_connection_prefect_flow(payload.blockname)

# ====================================================================================================
@clientapi.post('/prefect/createdbtcorejob/', auth=UserAuthBearer())
def create_prefect_flow_to_run_dbtcore(request, payload: PrefectDbtCore):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")

  return functions.run_dbtcore_prefect_flow(payload.blockname)

# ====================================================================================================
@clientapi.post('/prefect/createdbtrunblock/', auth=UserAuthBearer())
def create_prefect_block_dbtrun(request, payload: PrefectDbtRun):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.dbt is None:
    raise HttpError(400, "create a dbt workspace first")

  dbtenvdir = Path(os.getenv('CLIENTDBT_ROOT')) / user.org.slug
  if not os.path.exists(dbtenvdir):
    raise HttpError(400, "create the dbt env first")
  
  dbt_binary = str(dbtenvdir / "venv/bin/dbt")
  project_dir = str(dbtenvdir / "dbtrepo")

  blockdata = PrefectDbtCoreSetup(
    blockname=payload.dbt_blockname,
    profiles_dir=f"{project_dir}/profiles/",
    project_dir=project_dir,
    working_dir=project_dir,
    env={},
    commands=[
      f"{dbt_binary} run --target {payload.profile.target}"
    ]
  )

  block = functions.create_dbtcore_block(blockdata, payload.profile, payload.credentials)

  cpb = OrgPrefectBlock(
    org=user.org, 
    blocktype=block['block_type']['name'], 
    blockid=block['id'],
    blockname=block['name'],
  )
  cpb.save()

  return block

# ====================================================================================================
@clientapi.get('/prefect/dbtrunblocks', auth=UserAuthBearer())
def get_prefect_dbtrun_blocks(request):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  
  return [{
    'blocktype': x.blocktype,
    'blockid': x.blockid,
    'blockname': x.blockname,
  } for x in OrgPrefectBlock.objects.filter(org=user.org, blocktype=functions.DBTCORE)]

# ====================================================================================================
@clientapi.delete('/prefect/dbtrunblock/{block_id}', auth=UserAuthBearer())
def create_prefect_block_dbtrun(request, block_id):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  # don't bother checking for user.org.dbt

  functions.delete_dbtcore_block(block_id)
  cpb = OrgPrefectBlock.objects.filter(org=user.org, blockid=block_id).first()
  if cpb:
    cpb.delete()

  return {"success": 1}

# ====================================================================================================
@clientapi.post('/prefect/createdbttestblock/', auth=UserAuthBearer())
def create_prefect_block_dbttest(request, payload: PrefectDbtRun):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  if user.org.dbt is None:
    raise HttpError(400, "create a dbt workspace first")

  project_dir = Path(os.getenv('CLIENTDBT_ROOT')) / user.org.slug
  if not os.path.exists(project_dir):
    raise HttpError(400, "create the dbt env first")
  
  project_dir = project_dir / "dbtrepo"
  dbt_binary = project_dir / "venv/bin/dbt"

  blockdata = PrefectDbtCoreSetup(
    blockname=payload.dbt_blockname,
    profiles_dir=f"{project_dir}/profiles/",
    project_dir=project_dir,
    working_dir=project_dir,
    env={},
    commands=[
      f"{dbt_binary} test --target {payload.target}"
    ]
  )

  block = functions.create_dbtcore_block(blockdata, payload.profile, payload.credentials)

  cpb = OrgPrefectBlock(
    org=user.org, 
    blocktype=block['block_type']['name'], 
    blockid=block['id'],
    blockname=block['name'],
  )
  cpb.save()

  return block

# ====================================================================================================
@clientapi.delete('/prefect/dbttestblock/{block_id}', auth=UserAuthBearer())
def create_prefect_block_dbttest(request, block_id):
  user = request.auth
  if user.org is None:
    raise HttpError(400, "create an organization first")
  # don't bother checking for user.org.dbt
  
  functions.delete_dbtcore_block(block_id)
  cpb = OrgPrefectBlock.objects.filter(org=user.org, blockid=block_id).first()
  if cpb:
    cpb.delete()

  return {"success": 1}

