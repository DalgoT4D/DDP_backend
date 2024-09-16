import json
from typing import List

from dotenv import load_dotenv
from ninja import NinjaAPI
from ninja.errors import HttpError, ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError
from rest_framework.authtoken import views
from flags.state import flag_enabled
from django.utils.text import slugify
from django.db.models import Prefetch
from django.contrib.auth.models import User
from django.db.models import F

from ddpui import auth
from ddpui.auth import has_permission
from ddpui.core import orgfunctions, orguserfunctions
from ddpui.models.org import OrgSchema, OrgWarehouseSchema
from ddpui.models.org_user import (
    AcceptInvitationSchema,
    DeleteOrgUserPayload,
    ForgotPasswordSchema,
    Invitation,
    InvitationSchema,
    NewInvitationSchema,
    OrgUser,
    OrgUserCreate,
    OrgUserNewOwner,
    OrgUserResponse,
    OrgUserRole,
    OrgUserUpdate,
    OrgUserUpdateNewRole,
    OrgUserUpdatev1,
    ResetPasswordSchema,
    UserAttributes,
    VerifyEmailSchema,
)
from ddpui.models.role_based_access import Role, RolePermission
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.deleteorg import delete_warehouse_v1
from ddpui.utils.orguserhelpers import from_orguser
from ddpui.models.org import OrgWarehouse, Org, OrgType
from ddpui.models.orgtnc import OrgTnC
from ddpui.ddpairbyte import airbytehelpers

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


@user_org_api.get(
    "/currentuserv2", response=List[OrgUserResponse], auth=auth.CustomAuthMiddleware()
)
@has_permission(["can_view_orgusers"])
def get_current_user_v2(request):
    """return all the OrgUsers for the User making this request"""
    if request.orguser is None:
        raise HttpError(400, "requestor is not an OrgUser")
    orguser: OrgUser = request.orguser
    user: User = request.orguser.user
    org: Org = orguser.org

    # warehouse
    warehouse = OrgWarehouse.objects.filter(org=org).first()

    res = []
    for curr_orguser in OrgUser.objects.filter(user=user).prefetch_related(
        Prefetch(
            "new_role",
            queryset=Role.objects.prefetch_related(
                Prefetch(
                    "rolepermissions",
                    queryset=RolePermission.objects.filter(
                        role_id=F("role__id")
                    ).select_related("permission"),
                )
            ),
        ),
        Prefetch(
            "org",
            queryset=Org.objects.prefetch_related(
                "orgtncs",  # Assuming 'orgtnc' is a related name from Org to its related model
            ),
        ),
    ):
        if curr_orguser.org.orgtncs.exists():
            curr_orguser.org.tnc_accepted = curr_orguser.org.orgtncs.exists()
        res.append(
            OrgUserResponse(
                email=user.email,
                org=curr_orguser.org,
                active=user.is_active,
                role=curr_orguser.role,
                role_slug=slugify(OrgUserRole(curr_orguser.role).name),
                new_role_slug=curr_orguser.new_role.slug,
                wtype=warehouse.wtype if warehouse else None,
                permissions=[
                    {"slug": rolep.permission.slug, "name": rolep.permission.name}
                    for rolep in curr_orguser.new_role.rolepermissions.all()
                ],
                is_demo=(
                    curr_orguser.org.type == OrgType.DEMO if curr_orguser.org else False
                ),
            )
        )

    return res


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
    "/organizations/users",
    response=List[OrgUserResponse],
    auth=auth.CustomAuthMiddleware(),
)
@has_permission(["can_view_orgusers"])
def get_organization_users(request):
    """list all OrgUsers in the requestor's org, including inactive"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")
    org: Org = orguser.org
    # warehouse
    warehouse = OrgWarehouse.objects.filter(org=org).first()

    res = []
    for curr_orguser in OrgUser.objects.filter(org=org).prefetch_related(
        Prefetch(
            "new_role",
            queryset=Role.objects.prefetch_related(
                Prefetch(
                    "rolepermissions",
                    queryset=RolePermission.objects.filter(
                        role_id=F("role__id")
                    ).select_related("permission"),
                )
            ),
        ),
        Prefetch(
            "org",
            queryset=Org.objects.prefetch_related(
                "orgtncs",  # Assuming 'orgtnc' is a related name from Org to its related model
            ),
        ),
        Prefetch(
            "user",
            queryset=User.objects.all(),
        ),
    ):
        if curr_orguser.org.orgtncs.exists():
            curr_orguser.org.tnc_accepted = curr_orguser.org.orgtncs.exists()
        res.append(
            OrgUserResponse(
                email=curr_orguser.user.email,
                org=curr_orguser.org,
                active=curr_orguser.user.is_active,
                role=curr_orguser.role,
                role_slug=slugify(OrgUserRole(curr_orguser.role).name),
                new_role_slug=curr_orguser.new_role.slug,
                wtype=warehouse.wtype if warehouse else None,
                permissions=[
                    {"slug": rolep.permission.slug, "name": rolep.permission.name}
                    for rolep in curr_orguser.new_role.rolepermissions.all()
                ],
                is_demo=(
                    curr_orguser.org.type == OrgType.DEMO if curr_orguser.org else False
                ),
            )
        )

    return res


@user_org_api.post("/organizations/users/delete", auth=auth.CustomAuthMiddleware())
@has_permission(["can_delete_orguser"])
def delete_organization_users(request, payload: DeleteOrgUserPayload):
    """delete the orguser posted"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")

    _, error = orguserfunctions.delete_orguser(orguser, payload)
    if error:
        raise HttpError(400, error)

    return {"success": 1}


@user_org_api.post("/v1/organizations/users/delete", auth=auth.CustomAuthMiddleware())
@has_permission(["can_delete_orguser"])
def delete_organization_users_v1(request, payload: DeleteOrgUserPayload):
    """delete the orguser posted"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")

    _, error = orguserfunctions.delete_orguser_v1(orguser, payload)
    if error:
        raise HttpError(400, error)

    return {"success": 1}


@user_org_api.put(
    "/organizations/user_self/",
    response=OrgUserResponse,
    auth=auth.CustomAuthMiddleware(),
    deprecated=True,
)
@has_permission(["can_edit_orguser"])
def put_organization_user_self(request, payload: OrgUserUpdate):
    """update the requestor's OrgUser"""
    orguser: OrgUser = request.orguser

    # not allowed to update own role
    payload.role = None
    return orguserfunctions.update_orguser(orguser, payload)


@user_org_api.put(
    "/v1/organizations/user_self/",
    response=OrgUserResponse,
    auth=auth.CustomAuthMiddleware(),
)
@has_permission(["can_edit_orguser"])
def put_organization_user_self_v1(request, payload: OrgUserUpdatev1):
    """update the requestor's OrgUser"""
    orguser: OrgUser = request.orguser

    # not allowed to update own role
    payload.role_uuid = None
    return orguserfunctions.update_orguser_v1(orguser, payload)


@user_org_api.put(
    "/organizations/users/",
    response=OrgUserResponse,
    auth=auth.CustomAuthMiddleware(),
    deprecated=True,
)
@has_permission(["can_edit_orguser"])
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


@user_org_api.put(
    "/v1/organizations/users/",
    response=OrgUserResponse,
    auth=auth.CustomAuthMiddleware(),
)
@has_permission(["can_edit_orguser"])
def put_organization_user_v1(request, payload: OrgUserUpdatev1):
    """update another OrgUser or themselves"""
    requestor_orguser: OrgUser = request.orguser

    orguser = OrgUser.objects.filter(
        user__email=payload.toupdate_email, org=request.orguser.org
    ).first()
    if orguser is None:
        raise HttpError(
            400, "could not find user having this email address in this org"
        )

    # one can only update the role of user less than or equal to their role
    if payload.role_uuid and orguser.new_role.level > requestor_orguser.new_role.level:
        raise HttpError(403, "Insufficient permissions")

    # not allowed to update own role
    if requestor_orguser.user.email == orguser.user.email:
        payload.role_uuid = None

    return orguserfunctions.update_orguser_v1(orguser, payload)


@user_org_api.post(
    "/organizations/users/makeowner/",
    response=OrgUserResponse,
    auth=auth.CustomAuthMiddleware(),
)
@has_permission(["can_edit_orguser_role"])
def post_transfer_ownership(request, payload: OrgUserNewOwner):
    """update another OrgUser"""
    orguser: OrgUser = request.orguser
    retval, error = orguserfunctions.transfer_ownership(orguser, payload)
    if error:
        raise HttpError(400, error)
    return retval


@user_org_api.post(
    "/organizations/user_role/modify/",
    auth=auth.CustomAuthMiddleware(),
)
@has_permission(["can_edit_orguser_role"])
def post_modify_orguser_role(request, payload: OrgUserUpdateNewRole):
    """update another OrgUser's role"""
    orguser: OrgUser = request.orguser

    if not orguser.new_role:
        raise HttpError(403, "Insufficient permissions")

    role_to_be_assgined = Role.objects.filter(uuid=payload.role_uuid).first()

    if not role_to_be_assgined:
        raise HttpError(400, "Invalid role")

    # you cannot assign a role that is higher than yours
    if role_to_be_assgined.level > orguser.new_role.level:
        raise HttpError(403, "Insufficient permissions")

    request_email = payload.toupdate_email.lower().strip()
    orguser_to_be_assigned = (
        OrgUser.objects.filter(user__email__iexact=request_email, org=orguser.org)
        .exclude(user__email__iexact=orguser.user.email)
        .first()
    )
    if not orguser_to_be_assigned:
        raise HttpError(400, "User does not exist")

    orguser_to_be_assigned.new_role = role_to_be_assgined
    orguser_to_be_assigned.save()

    return {"success": 1}


@user_org_api.post("/organizations/warehouse/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_create_warehouse"])
def post_organization_warehouse(request, payload: OrgWarehouseSchema):
    """registers a data warehouse for the org"""
    orguser: OrgUser = request.orguser
    _, error = airbytehelpers.create_warehouse(orguser.org, payload)
    if error:
        raise HttpError(400, error)

    return {"success": 1}


@user_org_api.get("/organizations/warehouses", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouses"])
def get_organizations_warehouses(request):
    """returns all warehouses associated with this org"""
    orguser: OrgUser = request.orguser
    result, error = airbytehelpers.get_warehouses(orguser.org)
    if error:
        raise HttpError(400, error)
    return {"warehouses": result}


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


@user_org_api.get("/users/verify_email/resend", auth=auth.CustomAuthMiddleware())
@has_permission(["can_resend_email_verification"])
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


# ====================== Invite users =========================================


@user_org_api.post(
    "/organizations/users/invite/",
    response=InvitationSchema,
    auth=auth.CustomAuthMiddleware(),
)
@has_permission(["can_create_invitation"])
def post_organization_user_invite(request, payload: InvitationSchema):
    """Send an invitation to a user to join platform"""
    orguser: OrgUser = request.orguser
    retval, error = orguserfunctions.invite_user(orguser, payload)
    if error:
        raise HttpError(400, error)
    return retval


@user_org_api.post(
    "/v1/organizations/users/invite/",
    response=NewInvitationSchema,
    auth=auth.CustomAuthMiddleware(),
)
@has_permission(["can_create_invitation"])
def post_organization_user_invite_v1(request, payload: NewInvitationSchema):
    """Send an invitation to a user to join platform"""
    orguser: OrgUser = request.orguser
    retval, error = orguserfunctions.invite_user_v1(orguser, payload)
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


@user_org_api.post(
    "/v1/organizations/users/invite/accept/",
    response=OrgUserResponse,
)
def post_organization_user_accept_invite_v1(
    request, payload: AcceptInvitationSchema
):  # pylint: disable=unused-argument
    """User accepting the invite sent with a valid invite code"""
    retval, error = orguserfunctions.accept_invitation_v1(payload)
    if error:
        raise HttpError(400, error)
    return retval


@user_org_api.get("/users/invitations/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_invitations"])
def get_invitations(request):
    """Get all invitations sent by the current user"""
    retval, error = orguserfunctions.get_invitations_from_orguser(request.orguser)
    if error:
        raise HttpError(400, error)
    return retval


@user_org_api.get("/v1/users/invitations/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_invitations"])
def get_invitations_v1(request):
    """Get all invitations sent by the current user"""
    retval, error = orguserfunctions.get_invitations_from_orguser_v1(request.orguser)
    if error:
        raise HttpError(400, error)
    return retval


@user_org_api.post(
    "/users/invitations/resend/{invitation_id}", auth=auth.CustomAuthMiddleware()
)
@has_permission(["can_edit_invitation"])
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
    "/users/invitations/delete/{invitation_id}", auth=auth.CustomAuthMiddleware()
)
@has_permission(["can_delete_invitation"])
def delete_invitation(request, invitation_id):
    """Get all invitations sent by the current user"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    invitation = Invitation.objects.filter(id=invitation_id).first()

    if invitation:
        invitation.delete()

    return {"success": 1}


# ==============================================================================
# new apis to go away from the block architecture


@user_org_api.post(
    "/v1/organizations/", response=OrgSchema, auth=auth.CustomAuthMiddleware()
)
@has_permission(["can_create_org"])
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


@user_org_api.delete("/v1/organizations/warehouses/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_delete_warehouses"])
def delete_organization_warehouses_v1(request):
    """deletes all (references to) data warehouses for the org"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    if orguser.org.type == OrgType.DEMO:
        raise HttpError(403, "insufficient permissions")

    delete_warehouse_v1(orguser.org)

    return {"success": 1}


@user_org_api.post("/organizations/accept-tnc/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_accept_tnc"])
def post_organization_accept_tnc(request):
    """accept the terms and conditions"""
    orguser: OrgUser = request.orguser
    _, error = orguserfunctions.accept_tnc(orguser)
    if error:
        raise HttpError(400, error)
    return {"success": 1}


@user_org_api.get("/organizations/flags", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_flags"])
def get_organization_feature_flags(request):
    """get"""
    return {"allowLogsSummary": True}
