from typing import List
from ninja import NinjaAPI
from ninja.errors import HttpError
from ninja.errors import ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError

from ddpui.utils.ddp_logger import logger
from ddpui.auth import AdminAuthBearer, LoginData
from ddpui.models.org import Org, OrgSchema

from ddpui.models.org_user import OrgUser, OrgUserResponse, OrgUserUpdate
from ddpui.models.admin_user import AdminUser, AdminUserResponse

adminapi = NinjaAPI(urls_namespace="admin")


@adminapi.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):
    """Docstring"""
    return Response({"error": exc.errors}, status=422)


@adminapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(request, exc: PydanticValidationError):
    """Docstring"""
    return Response({"error": exc.errors()}, status=422)


@adminapi.exception_handler(HttpError)
def ninja_http_error_handler(request, exc: HttpError):
    """Docstring"""
    return Response({"error": " ".join(exc.args)}, status=exc.status_code)


@adminapi.exception_handler(Exception)
def ninja_default_error_handler(request, exc: Exception):
    """Docstring"""
    return Response({"error": " ".join(exc.args)}, status=500)


@adminapi.post("/login/")
def post_login(request, payload: LoginData):
    """Docstring"""
    if payload.password == "password":
        user = AdminUser.objects.filter(email=payload.email).first()
        if user:
            token = f"fake-admin-auth-token:{user.id}"
            logger.info("returning auth token " + token)
            return {"token": token}
    raise HttpError(400, "login denied")


@adminapi.get("/getadminuser", response=AdminUserResponse, auth=AdminAuthBearer())
def get_admin_user(request):
    """Docstring"""
    return request.auth


@adminapi.get(
    "/organizations/users", response=List[OrgUserResponse], auth=AdminAuthBearer()
)
def get_organization_users(request, org: str = None):
    """Docstring"""
    assert request.auth
    query = OrgUser.objects.filter(active=True)
    if org:
        query = query.filter(org__name=org)
    return query


@adminapi.put(
    "/organizations/users/{orguserid}", response=OrgUserResponse, auth=AdminAuthBearer()
)
def put_organization_user(request, orguserid: int, payload: OrgUserUpdate):
    """Docstring"""
    assert request.auth
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


@adminapi.delete("/organizations/", auth=AdminAuthBearer())
def delete_organization(request, payload: OrgSchema):
    """Docstring"""
    if request.headers.get("X-DDP-Confirmation") != "yes":
        raise HttpError(400, "missing x-confirmation header")
    org = Org.objects.filter(name=payload.name).first()
    if org:
        for orguser in OrgUser.objects.filter(org=org):
            logger.warning(f"deleting {org.name} user {orguser.email}")
            orguser.delete()
        org.delete()
        logger.warning(f"deleting {org.name}")
    return {"success": 1}
