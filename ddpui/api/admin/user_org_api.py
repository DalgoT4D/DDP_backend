from typing import List
from ninja import NinjaAPI
from ninja.errors import HttpError

# from ninja.errors import ValidationError
# from ninja.responses import Response
# from pydantic.error_wrappers import ValidationError as PydanticValidationError
from rest_framework.authtoken import views

from ddpui.utils.custom_logger import CustomLogger
from ddpui import auth
from ddpui.models.org import Org, OrgSchema

from ddpui.models.org_user import OrgUser, OrgUserResponse, OrgUserUpdate
from ddpui.models.admin_user import AdminUserResponse
from ddpui.utils.orguserhelpers import from_orguser

adminapi = NinjaAPI(urls_namespace="admin")

logger = CustomLogger("ddpui")


# @adminapi.exception_handler(ValidationError)
# def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
#     """Handle any ninja validation errors raised in the apis"""
#     return Response({"error": exc.errors}, status=422)


# @adminapi.exception_handler(PydanticValidationError)
# def pydantic_validation_error_handler(
#     request, exc: PydanticValidationError
# ):  # pylint: disable=unused-argument
#     """Handle any pydantic errors raised in the apis"""
#     return Response({"error": exc.errors()}, status=422)


# @adminapi.exception_handler(HttpError)
# def ninja_http_error_handler(
#     request, exc: HttpError
# ):  # pylint: disable=unused-argument
#     """Handle any http errors raised in the apis"""
#     return Response({"error": " ".join(exc.args)}, status=exc.status_code)


# @adminapi.exception_handler(Exception)
# def ninja_default_error_handler(
#     request, exc: Exception
# ):  # pylint: disable=unused-argument
#     """Handle any other exception raised in the apis"""
#     return Response({"error": " ".join(exc.args)}, status=500)


@adminapi.post("/login/")
def post_login(request):
    """Uses the username and password in the request to return an auth token"""
    token = views.obtain_auth_token(request)
    return token


@adminapi.get("/currentuser", response=AdminUserResponse, auth=auth.PlatformAdmin())
def get_admin_user(request):
    """return the admin user who made this request"""
    return AdminUserResponse.fromadminuser(request.adminuser)


@adminapi.get(
    "/organizations/users", response=List[OrgUserResponse], auth=auth.PlatformAdmin()
)
def get_organization_users(
    request, orgname: str = None
):  # pylint: disable=unused-argument
    """Fetch all organization users"""
    query = OrgUser.objects.filter(user__is_active=True)
    if orgname:
        query = query.filter(org__name=orgname)
    return [from_orguser(orguser) for orguser in query]


@adminapi.put(
    "/organizations/users/{orguserid}",
    response=OrgUserResponse,
    auth=auth.PlatformAdmin(),
)
def put_organization_user(
    request, orguserid: int, payload: OrgUserUpdate
):  # pylint: disable=unused-argument
    """update attributes of an orguser (or of the linked django user)"""
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
@adminapi.delete("/organizations/", auth=auth.PlatformAdmin())
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
