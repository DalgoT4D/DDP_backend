from datetime import datetime
from typing import List
from uuid import uuid4

from django.contrib.auth.models import User
from django.utils.text import slugify
from ninja import NinjaAPI
from ninja.errors import HttpError, ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError
from rest_framework.authtoken import views

from ddpui import auth
from ddpui.models.org import Org, OrgSchema
from ddpui.models.org_user import (
    AcceptInvitationSchema,
    Invitation,
    InvitationSchema,
    OrgUser,
    OrgUserCreate,
    OrgUserResponse,
    OrgUserRole,
    OrgUserUpdate,
)
from ddpui.utils.ddp_logger import logger
from ddpui.utils.timezone import IST

user_org_api = NinjaAPI(urls_namespace="userorg")
# http://127.0.0.1:8000/api/docs


@user_org_api.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """Handle any ninja validation errors raised in the apis"""
    return Response({"error": exc.errors}, status=422)


@user_org_api.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """Handle any pydantic errors raised in the apis"""
    return Response({"error": exc.errors()}, status=422)


@user_org_api.exception_handler(HttpError)
def ninja_http_error_handler(
    request, exc: HttpError
):  # pylint: disable=unused-argument
    """Handle any http errors raised in the apis"""
    return Response({"error": " ".join(exc.args)}, status=exc.status_code)


@user_org_api.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception
):  # pylint: disable=unused-argument
    """Handle any other exception raised in the apis"""
    return Response({"error": " ".join(exc.args)}, status=500)


@user_org_api.get("/currentuser", response=OrgUserResponse, auth=auth.AnyOrgUser())
def get_current_user(request):
    """return the OrgUser making this request"""
    orguser = request.orguser
    if orguser is not None:
        return OrgUserResponse.from_orguser(orguser)
    raise HttpError(400, "requestor is not an OrgUser")


@user_org_api.post("/organizations/users/", response=OrgUserResponse)
def post_organization_user(
    request, payload: OrgUserCreate
):  # pylint: disable=unused-argument
    """this is the "signup" action
    creates a new OrgUser having specified email + password.
    no Org is created or attached at this time
    """
    email = payload.email.lower().strip()
    if OrgUser.objects.filter(user__email=email).exists():
        raise HttpError(400, f"user having email {email} exists")
    user = User.objects.create_user(
        username=email, email=email, password=payload.password
    )
    orguser = OrgUser.objects.create(user=user, role=OrgUserRole.ACCOUNT_MANAGER)
    orguser.save()
    logger.info(
        f"created user [account-manager] {orguser.user.email} having userid {orguser.user.id}"
    )
    return OrgUserResponse.from_orguser(orguser)


@user_org_api.post("/login/")
def post_login(request):
    """Uses the username and password in the request to return an auth token"""
    token = views.obtain_auth_token(request)
    return token


@user_org_api.get(
    "/organizations/users", response=List[OrgUserResponse], auth=auth.CanManageUsers()
)
def get_organization_users(request):
    """list all OrgUsers in the requestor's org, including inactive"""
    orguser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")
    query = OrgUser.objects.filter(org=orguser.org)
    return [
        OrgUserResponse(email=orguser.user.email, active=orguser.user.is_active)
        for orguser in query
    ]


@user_org_api.put(
    "/organizations/user_self/", response=OrgUserResponse, auth=auth.AnyOrgUser()
)
def put_organization_user_self(request, payload: OrgUserUpdate):
    """update the requestor's OrgUser"""
    orguser = request.orguser

    if payload.email:
        orguser.user.email = payload.email
    if payload.active is not None:
        orguser.user.is_active = payload.active
    orguser.user.save()

    logger.info(f"updated self {orguser.user.email}")
    return OrgUserResponse(email=orguser.user.email, active=orguser.user.is_active)


@user_org_api.put(
    "/organizations/users/",
    response=OrgUserResponse,
    auth=auth.CanManageUsers(),
)
def put_organization_user(request, payload: OrgUserUpdate):
    """update another OrgUser"""
    orguser = OrgUser.objects.filter(
        email=payload.toupdate_email, org=request.orguser.org
    ).first()

    if payload.email:
        orguser.user.email = payload.email
    if payload.active is not None:
        orguser.user.is_active = payload.active
    orguser.user.save()

    logger.info(f"updated orguser {orguser.user.email}")
    return OrgUserResponse(email=orguser.user.email, active=orguser.user.is_active)


@user_org_api.post("/organizations/", response=OrgSchema, auth=auth.FullAccess())
def post_organization(request, payload: OrgSchema):
    """creates a new org and attaches it to the requestor"""
    orguser = request.orguser
    if orguser.org:
        raise HttpError(400, "orguser already has an associated org")
    org = Org.objects.filter(name=payload.name).first()
    if org:
        raise HttpError(400, "client org already exists")
    org = Org.objects.create(**payload.dict())
    org.slug = slugify(org.name)
    orguser.org = org
    orguser.save()
    logger.info(f"{orguser.user.email} created new org {org.name}")
    return OrgSchema(name=org.name, airbyte_workspace_id=None)


@user_org_api.post(
    "/organizations/users/invite/",
    response=InvitationSchema,
    auth=auth.CanManageUsers(),
)
def post_organization_user_invite(request, payload: InvitationSchema):
    """Send an invitation to a user to join platform"""
    orguser = request.orguser
    invitation = Invitation.objects.filter(invited_email=payload.invited_email).first()
    if invitation:
        logger.error(
            f"{payload.invited_email} has already been invited by {invitation.invited_by} on {invitation.invited_on.strftime('%Y-%m-%d')}"
        )
        raise HttpError(400, f"{payload.invited_email} has already been invited")

    payload.invited_by = OrgUserResponse.from_orguser(orguser)
    payload.invited_on = datetime.now(IST)
    payload.invite_code = str(uuid4())
    invitation = Invitation.objects.create(
        invited_email=payload.invited_email,
        invited_role=payload.invited_role,
        invited_by=orguser,
        invited_on=payload.invited_on,
        invite_code=payload.invite_code,
    )
    logger.info("created Invitation")
    return payload


# the invitee will get a hyperlink via email, clicking will take them to \
# the UI where they will choose
# a password, then click a button POSTing to this endpoint
@user_org_api.get(
    "/organizations/users/invite/{invite_code}",
    response=InvitationSchema,
)
def get_organization_user_invite(
    request, invite_code
):  # pylint: disable=unused-argument
    """Fetch the invite sent to user with a particular invite code"""
    invitation = Invitation.objects.filter(invite_code=invite_code).first()
    if invitation is None:
        raise HttpError(400, "invalid invite code")
    return InvitationSchema.from_invitation(invitation)


@user_org_api.post(
    "/organizations/users/invite/accept/",
    response=OrgUserResponse,
)
def post_organization_user_accept_invite(
    request, payload: AcceptInvitationSchema
):  # pylint: disable=unused-argument
    """User accepting the invite sent with a valid invite code"""
    invitation = Invitation.objects.filter(invite_code=payload.invite_code).first()
    if invitation is None:
        raise HttpError(400, "invalid invite code")
    orguser = OrgUser.objects.filter(
        user__email=invitation.invited_email, org=invitation.invited_by.org
    ).first()
    if not orguser:
        logger.info(
            f"creating invited user {invitation.invited_email} for {invitation.invited_by.org.name}"
        )
        user = User.objects.create_user(
            username=invitation.invited_email,
            email=invitation.invited_email,
            password=payload.password,
        )
        orguser = OrgUser.objects.create(
            user=user, org=invitation.invited_by.org, role=invitation.invited_role
        )
    return OrgUserResponse.from_orguser(orguser)
