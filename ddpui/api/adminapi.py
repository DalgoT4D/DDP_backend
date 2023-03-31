from ninja import NinjaAPI
from ninja.errors import HttpError

from typing import List

from .ddplogger import logger
from .auth import AdminAuthBearer, LoginData
from .clientorg import ClientOrg, ClientOrgSchema

from .clientuser import ClientUser, ClientUserResponse, ClientUserUpdate
from .adminuser import AdminUser, AdminUserResponse

adminapi = NinjaAPI(urls_namespace='admin')

# ====================================================================================================
@adminapi.post("/login/")
def login(request, payload: LoginData):
  if payload.password == 'password':
    user = AdminUser.objects.filter(email=payload.email).first()
    if user:
      token = f"fake-admin-auth-token:{user.id}"
      logger.info("returning auth token " + token)
      return {'token': token}
  raise HttpError(400, "login denied")

# ====================================================================================================
@adminapi.get('/getadminuser', response=AdminUserResponse, auth=AdminAuthBearer())
def getadminuser(request):
  return request.auth

# ====================================================================================================
@adminapi.get("/users", response=List[ClientUserResponse], auth=AdminAuthBearer())
def users(request, clientorg: str = None):
  assert(request.auth)
  q = ClientUser.objects.filter(active=True)
  if clientorg:
    q = q.filter(clientorg__name=clientorg)
  return q

# ====================================================================================================
@adminapi.post("/updateuser/{clientuserid}", response=ClientUserResponse, auth=AdminAuthBearer())
def updateuser(request, clientuserid: int, payload: ClientUserUpdate):
  assert(request.auth)
  user = ClientUser.objects.filter(id=clientuserid).first()
  if user is None:
    raise HttpError(400, "no such user id")
  if payload.email:
    user.email = payload.email
  if payload.active is not None:
    user.active = payload.active
  if payload.clientorg:
    clientorg = ClientOrg.objects.filter(name=payload.clientorg.name).first()
    user.clientorg = clientorg
  user.save()
  logger.info(f"updated user {user.email}")
  return user

# ====================================================================================================
@adminapi.post("/deleteorg/", auth=AdminAuthBearer())
def updateuser(request, payload: ClientOrgSchema):
  if request.headers.get('X-DDP-Confirmation') != 'yes':
    raise HttpError(400, "missing x-confirmation header")
  clientorg = ClientOrg.objects.filter(name=payload.name).first()
  if clientorg:
    for clientuser in ClientUser.objects.filter(clientorg=clientorg):
      logger.warn(f"deleting {clientorg.name} user {clientuser.email}")
      clientuser.delete()
    clientorg.delete()
    logger.warn(f"deleting {clientorg.name}")
  return {"success": 1}