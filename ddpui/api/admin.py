from ninja import NinjaAPI
from ninja.errors import HttpError

from typing import List

from ddpui.utils.ddplogger import logger
from ddpui.auth import AdminAuthBearer, LoginData
from ddpui.models.org import Org, OrgSchema

from ddpui.models.orguser import OrgUser, OrgUserResponse, OrgUserUpdate
from ddpui.models.adminuser import AdminUser, AdminUserResponse

adminapi = NinjaAPI(urls_namespace='admin')

# ====================================================================================================
@adminapi.post("/login/")
def postLogin(request, payload: LoginData):
  if payload.password == 'password':
    user = AdminUser.objects.filter(email=payload.email).first()
    if user:
      token = f"fake-admin-auth-token:{user.id}"
      logger.info("returning auth token " + token)
      return {'token': token}
  raise HttpError(400, "login denied")

# ====================================================================================================
@adminapi.get('/getadminuser', response=AdminUserResponse, auth=AdminAuthBearer())
def getAdminUser(request):
  return request.auth

# ====================================================================================================
@adminapi.get("/organizations", response=List[OrgUserResponse], auth=AdminAuthBearer())
def getUsers(request, org: str = None):
  assert(request.auth)
  q = OrgUser.objects.filter(active=True)
  if org:
    q = q.filter(org__name=org)
  return q

# ====================================================================================================
@adminapi.put("/organizations/{orguserid}", response=OrgUserResponse, auth=AdminAuthBearer())
def putUser(request, orguserid: int, payload: OrgUserUpdate):
  assert(request.auth)
  user = OrgUser.objects.filter(id=orguserid).first()
  if user is None:
    raise HttpError(400, "no such user id")
  if payload.email:
    user.email = payload.email
  if payload.active is not None:
    user.active = payload.active
  if payload.org:
    org = Org.objects.filter(name=payload.org.name).first()
    user.org = org
  user.save()
  logger.info(f"updated user {user.email}")
  return user

# ====================================================================================================
@adminapi.delete("/organizations/", auth=AdminAuthBearer())
def deleteUser(request, payload: OrgSchema):
  if request.headers.get('X-DDP-Confirmation') != 'yes':
    raise HttpError(400, "missing x-confirmation header")
  org = Org.objects.filter(name=payload.name).first()
  if org:
    for orguser in OrgUser.objects.filter(org=org):
      logger.warn(f"deleting {org.name} user {orguser.email}")
      orguser.delete()
    org.delete()
    logger.warn(f"deleting {org.name}")
  return {"success": 1}