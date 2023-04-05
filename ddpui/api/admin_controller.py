from typing import List
from ninja import NinjaAPI
from ninja.errors import HttpError

from rest_framework.authtoken import views

from ddpui.utils.ddp_logger import logger
from ddpui.auth import AuthBearer
from ddpui.models.org import Org, OrgSchema

from ddpui.models.org_user import OrgUser, OrgUserResponse, OrgUserUpdate
from ddpui.models.admin_user import AdminUserResponse

adminapi = NinjaAPI(urls_namespace="admin")


@adminapi.post("/login/")
def post_login(request):
    """Uses the username and password in the request to return an auth token"""
    token = views.obtain_auth_token(request)
    return token


@adminapi.get("/getadminuser", response=AdminUserResponse, auth=AuthBearer())
def get_admin_user(request):
    """return the admin user who made this request"""
    return AdminUserResponse(
        email=request.user.email,
        active=request.user.is_active,
    )


@adminapi.get("/organizations/users", response=List[OrgUserResponse], auth=AuthBearer())
def get_organization_users(request, orgname: str = None):
    """return all active orgusers in the database, optionally filtering by orgname"""
    assert request.auth
    query = OrgUser.objects.filter(user__is_active=True)
    if orgname:
        query = query.filter(org__name=orgname)
    return [
        OrgUserResponse(email=orguser.user.email, active=orguser.user.is_active)
        for orguser in query
    ]


@adminapi.put(
    "/organizations/users/{orguserid}", response=OrgUserResponse, auth=AuthBearer()
)
def put_organization_user(request, orguserid: int, payload: OrgUserUpdate):
    """update attributes of an orguser (or of the linked django user)"""
    assert request.auth
    orguser = OrgUser.objects.filter(id=orguserid).first()
    if orguser is None:
        raise HttpError(400, "no such orguser id")
    if payload.email:
        orguser.user.email = payload.email
    if payload.active is not None:
        orguser.user.is_active = payload.active
    orguser.user.save()
    logger.info(f"updated user {orguser.user.email}")
    return orguser


# ====================================================================================================
@adminapi.delete("/organizations/", auth=AuthBearer())
def delete_organization(request, payload: OrgSchema):
    """delete an organization and all associated org users"""
    if request.headers.get("X-DDP-Confirmation") != "yes":
        raise HttpError(400, "missing x-confirmation header")
    org = Org.objects.filter(name=payload.name).first()
    if org:
        for orguser in OrgUser.objects.filter(org=org):
            logger.warning(f"deleting {org.name} user {orguser.user.email}")
            orguser.delete()
        org.delete()
        logger.warning(f"deleting {org.name}")
    return {"success": 1}
