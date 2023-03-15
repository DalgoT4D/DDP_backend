from ninja import NinjaAPI
from ninja.errors import HttpError
from datetime import datetime
from .timezone import IST
from uuid import uuid4

from typing import List

from .ddplogger import logger
from .auth import LoginData, ClientAuthBearer

clientapi = NinjaAPI()
# http://127.0.0.1:8000/api/docs

from .clientuser import ClientUser, ClientUserCreate, ClientUserUpdate, ClientUserResponse
from .clientuser import InvitationSchema, Invitation, AcceptInvitationSchema
from .clientorg import ClientOrg, ClientOrgSchema

# ====================================================================================================
@clientapi.get("/currentuser", auth=ClientAuthBearer(), response=ClientUserResponse)
def currentuser(request):
  return request.auth

# ====================================================================================================
@clientapi.post("/createuser/", response=ClientUserResponse)
def createuser(request, payload: ClientUserCreate):
  if ClientUser.objects.filter(email=payload.email).exists():
    raise HttpError(400, f"user having email {payload.email} exists")
  user = ClientUser.objects.create(**payload.dict())
  logger.info(f"created user {payload.email}")
  return user

# ====================================================================================================
@clientapi.post("/login/")
def login(request, payload: LoginData):
  if payload.password == 'password':
    user = ClientUser.objects.filter(email=payload.email).first()
    if user:
      token = f"fake-auth-token:{user.id}"
      logger.info("returning auth token " + token)
      return {'token': token}
  raise HttpError(400, "login denied")

# ====================================================================================================
@clientapi.get("/users", response=List[ClientUserResponse], auth=ClientAuthBearer())
def users(request):
  assert(request.auth)
  user = request.auth
  if user.clientorg is None:
    raise HttpError(400, "no associated org")
  return ClientUser.objects.filter(clientorg=user.clientorg)

# ====================================================================================================
@clientapi.post("/updateuser/", response=ClientUserResponse, auth=ClientAuthBearer())
def updateuser(request, payload: ClientUserUpdate):
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
@clientapi.post('/client/create/', response=ClientOrgSchema, auth=ClientAuthBearer())
def createclient(request, payload: ClientOrgSchema):
  logger.info(payload)
  user = request.auth
  if user.clientorg:
    raise HttpError(400, "user already has an associated client")
  clientorg = ClientOrg.objects.filter(name=payload.name).first()
  if clientorg:
    raise HttpError(400, "client org already exists")
  clientorg = ClientOrg.objects.create(**payload.dict())
  user.clientorg = clientorg
  user.save()
  return clientorg

# ====================================================================================================
@clientapi.post('/user/invite/', response=InvitationSchema, auth=ClientAuthBearer())
def inviteuser(request, payload: InvitationSchema):
  if request.auth.clientorg is None:
    raise HttpError(400, "an associated organization is required")
  x = Invitation.objects.filter(invited_email=payload.invited_email).first()
  if x:
    logger.error(f"{payload.invited_email} has already been invited by {x.invited_by} on {x.invited_on.strftime('%Y-%m-%d')}")
    raise HttpError(400, f'{payload.invited_email} has already been invited')

  payload.invited_by = ClientUserResponse(email=request.auth.email, clientorg=request.auth.clientorg, active=request.auth.active)
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
@clientapi.post('/user/acceptinvite/', response=ClientUserResponse)
def acceptinvite(request, payload: AcceptInvitationSchema):
  x = Invitation.objects.filter(invite_code=payload.invite_code).first()
  if x is None:
    raise HttpError(400, "invalid invite code")
  clientuser = ClientUser.objects.filter(email=x.invited_email, clientorg=x.invited_by.clientorg).first()
  if not clientuser:
    logger.info(f"creating invited user {x.invited_email} for {x.invited_by.clientorg.name}")
    clientuser = ClientUser.objects.create(email=x.invited_email, clientorg=x.invited_by.clientorg)
  return clientuser
  