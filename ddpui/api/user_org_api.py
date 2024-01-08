from typing import List
import json
from dotenv import load_dotenv


from ninja import NinjaAPI
from ninja.errors import HttpError

from ninja.errors import ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError
from rest_framework.authtoken import views

from ddpui import auth
from ddpui.models.org import (
    OrgSchema,
    OrgWarehouseSchema,
)
from ddpui.models.org_user import (
    AcceptInvitationSchema,
    Invitation,
    InvitationSchema,
    UserAttributes,
    OrgUser,
    OrgUserCreate,
    OrgUserNewOwner,
    OrgUserResponse,
    OrgUserRole,
    OrgUserUpdate,
    ForgotPasswordSchema,
    ResetPasswordSchema,
    VerifyEmailSchema,
    DeleteOrgUserPayload,
)


from ddpui.utils.custom_logger import CustomLogger

from ddpui.utils.deleteorg import delete_warehouse_v1
from ddpui.utils.orguserhelpers import from_orguser
from ddpui.core import orguserfunctions
from ddpui.core import orgfunctions

user_org_api = NinjaAPI(urls_namespace="userorg")
# http://127.0.0.1:8000/api/docs

load_dotenv()

logger = CustomLogger("ddpui")


@user_org_api.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """
    Handle any ninja validation errors raised in the apis
    These are raised during request payload validation
    exc.errors is correct
    """
    return Response({"detail": exc.errors}, status=422)


@user_org_api.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """
    Handle any pydantic errors raised in the apis
    These are raised during response payload validation
    exc.errors() is correct
    """
    return Response({"detail": exc.errors()}, status=500)


@user_org_api.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception
):  # pylint: disable=unused-argument # skipcq PYL-W0613
    """Handle any other exception raised in the apis"""
    print(exc)
    return Response({"detail": "something went wrong"}, status=500)


@user_org_api.get("/currentuser", response=OrgUserResponse, auth=auth.AnyOrgUser())
def get_current_user(request):
    """return the OrgUser making this request"""
    orguser: OrgUser = request.orguser
    if orguser is not None:
        return from_orguser(orguser)
    raise HttpError(400, "requestor is not an OrgUser")


@user_org_api.get(
    "/currentuserv2", response=List[OrgUserResponse], auth=auth.AnyOrgUser()
)
def get_current_user_v2(request):
    """return all the OrgUsers for the User making this request"""
    if request.orguser is None:
        raise HttpError(400, "requestor is not an OrgUser")
    user = request.orguser.user
    return [from_orguser(orguser) for orguser in OrgUser.objects.filter(user=user)]


@user_org_api.post("/organizations/users/", response=OrgUserResponse)
def post_organization_user(
    request, payload: OrgUserCreate
):  # pylint: disable=unused-argument
    """this is the "signup" action
    creates a new OrgUser having specified email + password.
    no Org is created or attached at this time
    """
    payload.email = payload.email.lower().strip()
    retval, error = orguserfunctions.signup_orguser(payload)
    if error:
        raise HttpError(400, error)
    return retval


@user_org_api.post("/login/")
def post_login(request):
    """Uses the username and password in the request to return an auth token"""
    request_obj = json.loads(request.body)
    token = views.obtain_auth_token(request)
    if "token" in token.data:
        retval = orguserfunctions.lookup_user(request_obj["username"])
        retval["token"] = token.data["token"]
        return retval

    return token


@user_org_api.get(
    "/organizations/users", response=List[OrgUserResponse], auth=auth.CanManageUsers()
)
def get_organization_users(request):
    """list all OrgUsers in the requestor's org, including inactive"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")
    query = OrgUser.objects.filter(org=orguser.org)
    return [from_orguser(orguser) for orguser in query]


@user_org_api.post("/organizations/users/delete", auth=auth.CanManageUsers())
def delete_organization_users(request, payload: DeleteOrgUserPayload):
    """delete the orguser posted"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")

    _, error = orguserfunctions.delete_orguser(orguser, payload)
    if error:
        raise HttpError(400, error)

    return {"success": 1}


@user_org_api.put(
    "/organizations/user_self/", response=OrgUserResponse, auth=auth.AnyOrgUser()
)
def put_organization_user_self(request, payload: OrgUserUpdate):
    """update the requestor's OrgUser"""
    orguser: OrgUser = request.orguser

    # not allowed to update own role
    payload.role = None
    return orguserfunctions.update_orguser(orguser, payload)


@user_org_api.put(
    "/organizations/users/",
    response=OrgUserResponse,
    auth=auth.CanManageUsers(),
)
def put_organization_user(request, payload: OrgUserUpdate):
    """update another OrgUser"""
    requestor_orguser: OrgUser = request.orguser

    if requestor_orguser.role not in [
        OrgUserRole.ACCOUNT_MANAGER,
        OrgUserRole.PIPELINE_MANAGER,
    ]:
        raise HttpError(400, "not authorized to update another user")

    orguser = OrgUser.objects.filter(
        user__email=payload.toupdate_email, org=request.orguser.org
    ).first()
    if orguser is None:
        raise HttpError(
            400, "could not find user having this email address in this org"
        )

    return orguserfunctions.update_orguser(orguser, payload)


@user_org_api.post(
    "/organizations/users/makeowner/",
    response=OrgUserResponse,
    auth=auth.FullAccess(),
)
def post_transfer_ownership(request, payload: OrgUserNewOwner):
    """update another OrgUser"""
    orguser: OrgUser = request.orguser
    retval, error = orguserfunctions.transfer_ownership(orguser, payload)
    if error:
        raise HttpError(400, error)
    return retval


@user_org_api.post("/organizations/warehouse/", auth=auth.CanManagePipelines())
def post_organization_warehouse(request, payload: OrgWarehouseSchema):
    """registers a data warehouse for the org"""
    orguser: OrgUser = request.orguser
    _, error = orgfunctions.create_warehouse(orguser.org, payload)
    if error:
        raise HttpError(400, error)

    return {"success": 1}


@user_org_api.get("/organizations/warehouses", auth=auth.CanManagePipelines())
def get_organizations_warehouses(request):
    """returns all warehouses associated with this org"""
    orguser: OrgUser = request.orguser
    result, error = orgfunctions.get_warehouses(orguser.org)
    if error:
        raise HttpError(400, error)
    return {"warehouses": result}


@user_org_api.post(
    "/organizations/users/invite/",
    response=InvitationSchema,
    auth=auth.CanManageUsers(),
)
def post_organization_user_invite(request, payload: InvitationSchema):
    """Send an invitation to a user to join platform"""
    orguser: OrgUser = request.orguser
    retval, error = orguserfunctions.invite_user(orguser, payload)
    if error:
        raise HttpError(400, error)
    return retval


@user_org_api.post(
    "/organizations/users/invite/accept/",
    response=OrgUserResponse,
)
def post_organization_user_accept_invite(
    request, payload: AcceptInvitationSchema
):  # pylint: disable=unused-argument
    """User accepting the invite sent with a valid invite code"""
    retval, error = orguserfunctions.accept_invitation(payload)
    if error:
        raise HttpError(400, error)
    return retval


@user_org_api.get("/users/invitations/", auth=auth.AnyOrgUser())
def get_invitations(request):
    """Get all invitations sent by the current user"""
    retval, error = orguserfunctions.get_invitations_from_orguser(request.orguser)
    if error:
        raise HttpError(400, error)
    return retval


@user_org_api.post("/users/invitations/resend/{invitation_id}", auth=auth.AnyOrgUser())
def post_resend_invitation(request, invitation_id):
    """Get all invitations sent by the current user"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    _, error = orguserfunctions.resend_invitation(invitation_id)
    if error:
        raise HttpError(400, error)

    return {"success": 1}


@user_org_api.delete(
    "/users/invitations/delete/{invitation_id}", auth=auth.AnyOrgUser()
)
def delete_invitation(request, invitation_id):
    """Get all invitations sent by the current user"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    invitation = Invitation.objects.filter(id=invitation_id).first()

    if invitation:
        invitation.delete()

    return {"success": 1}


@user_org_api.post(
    "/users/forgot_password/",
)
def post_forgot_password(
    request, payload: ForgotPasswordSchema
):  # pylint: disable=unused-argument
    """step 1 of the forgot-password flow"""
    _, error = orguserfunctions.request_reset_password(payload.email)
    if error:
        raise HttpError(400, error)
    return {"success": 1}


@user_org_api.post("/users/reset_password/")
def post_reset_password(
    request, payload: ResetPasswordSchema
):  # pylint: disable=unused-argument
    """step 2 of the forgot-password flow"""
    _, error = orguserfunctions.confirm_reset_password(payload)
    if error:
        raise HttpError(400, error)
    return {"success": 1}


@user_org_api.get("/users/verify_email/resend", auth=auth.AnyOrgUser())
def get_verify_email_resend(request):  # pylint: disable=unused-argument
    """this api is hit when the user is logged in but the email is still not verified"""
    _, error = orguserfunctions.resend_verification_email(
        request.orguser, request.user.email
    )
    if error:
        raise HttpError(400, error)
    return {"success": 1}


@user_org_api.post("/users/verify_email/")
def post_verify_email(
    request, payload: VerifyEmailSchema
):  # pylint: disable=unused-argument
    """step 2 of the verify-email flow"""
    _, error = orguserfunctions.verify_email(payload)
    if error:
        raise HttpError(400, error)
    return {"success": 1}


# ==============================================================================
# new apis to go away from the block architecture


@user_org_api.post("/v1/organizations/", response=OrgSchema, auth=auth.AnyOrgUser())
def post_organization_v1(request, payload: OrgSchema):
    """creates a new org & new orguser (if required) and attaches it to the requestor"""
    orguser: OrgUser = request.orguser

    userattributes = UserAttributes.objects.filter(user=orguser.user).first()
    if userattributes is None or userattributes.can_create_orgs is False:
        raise HttpError(403, "Insufficient permissions for this operation")

    org, error = orgfunctions.create_organization(payload)
    if error:
        raise HttpError(400, error)

    # create a new orguser if the org is already there
    orguserfunctions.ensure_orguser_for_org(orguser, org)

    logger.info(f"{orguser.user.email} created new org {org.name}")
    return OrgSchema(
        name=org.name, airbyte_workspace_id=org.airbyte_workspace_id, slug=org.slug
    )


@user_org_api.delete("/v1/organizations/warehouses/", auth=auth.CanManagePipelines())
def delete_organization_warehouses_v1(request):
    """deletes all (references to) data warehouses for the org"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    if orguser.org.is_demo:
        raise HttpError(403, "insufficient permissions")

    delete_warehouse_v1(orguser.org)

    return {"success": 1}


@user_org_api.post("/organizations/accept-tnc/", auth=auth.CanManagePipelines())
def post_organization_accept_tnc(request):
    """accept the terms and conditions"""
    orguser: OrgUser = request.orguser
    _, error = orguserfunctions.accept_tnc(orguser)
    if error:
        raise HttpError(400, error)
    return {"success": 1}
