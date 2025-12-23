import json
from typing import List
from datetime import timedelta

from dotenv import load_dotenv
from ninja import Router
from ninja.errors import HttpError
from rest_framework.authtoken import views
from django.utils.text import slugify
from django.db.models import Prefetch
from django.contrib.auth.models import User
from django.db.models import F
from django.http import JsonResponse
from django.conf import settings
from rest_framework_simplejwt.tokens import RefreshToken, TokenError

from ddpui.auth import has_permission, CustomTokenObtainSerializer, CustomTokenRefreshSerializer
from ddpui.core import orgfunctions, orguserfunctions
from ddpui.models.org import (
    OrgSchema,
    OrgWarehouseSchema,
    CreateOrgSchema,
    CreateFreeTrialOrgSchema,
)
from ddpui.models.org_user import (
    AcceptInvitationSchema,
    DeleteOrgUserPayload,
    ForgotPasswordSchema,
    Invitation,
    InvitationSchema,
    NewInvitationSchema,
    OrgUser,
    OrgUserCreate,
    OrgUserResponse,
    OrgUserUpdate,
    OrgUserUpdateNewRole,
    OrgUserUpdatev1,
    ResetPasswordSchema,
    ChangePasswordSchema,
    UserAttributes,
    VerifyEmailSchema,
    LoginPayload,
    TokenRefreshPayload,
    LogoutPayload,
)
from ddpui.models.org_plans import OrgPlanType
from ddpui.models.org_wren import OrgWren
from ddpui.models.role_based_access import Role, RolePermission
from ddpui.utils.custom_logger import CustomLogger
from ddpui.models.org import OrgWarehouse, Org, OrgType
from ddpui.ddpairbyte import airbytehelpers
from ddpui.models.org_preferences import OrgPreferences
from ddpui.celeryworkers.moretasks import create_free_trial_org_account
from ddpui.utils.feature_flags import get_all_feature_flags_for_org
from ddpui.services.org_cleanup_service import OrgCleanupService
from django.db import transaction

user_org_router = Router()
load_dotenv()
logger = CustomLogger("ddpui")


@user_org_router.get("/currentuserv2", response=List[OrgUserResponse])
@has_permission(["can_view_orgusers"])
def get_current_user_v2(request, org_slug: str = None):
    """return all the OrgUsers for the User making this request"""
    if request.orguser is None:
        raise HttpError(400, "requestor is not an OrgUser")
    orguser: OrgUser = request.orguser
    user: User = request.orguser.user
    org: Org = orguser.org
    # warehouse
    warehouse = OrgWarehouse.objects.filter(org=org).first()
    curr_orgusers = OrgUser.objects.filter(user=user)

    if org_slug:
        curr_orgusers = curr_orgusers.filter(org__slug=org_slug)

    org_preferences = OrgPreferences.objects.filter(org=org).first()
    if org_preferences is None:
        org_preferences = OrgPreferences.objects.create(org=org)

    # Get org default dashboard
    org_default_dashboard = None
    from ddpui.models.dashboard import Dashboard

    org_default_dashboard_obj = Dashboard.objects.filter(org=org, is_org_default=True).first()
    if org_default_dashboard_obj:
        org_default_dashboard = org_default_dashboard_obj.id

    res = []
    for curr_orguser in curr_orgusers.prefetch_related(
        Prefetch(
            "new_role",
            queryset=Role.objects.prefetch_related(
                Prefetch(
                    "rolepermissions",
                    queryset=RolePermission.objects.filter(role_id=F("role__id")).select_related(
                        "permission"
                    ),
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
                new_role_slug=curr_orguser.new_role.slug,
                wtype=warehouse.wtype if warehouse else None,
                permissions=[
                    {"slug": rolep.permission.slug, "name": rolep.permission.name}
                    for rolep in curr_orguser.new_role.rolepermissions.all()
                ],
                is_demo=(
                    curr_orguser.org.base_plan() == OrgType.DEMO if curr_orguser.org else False
                ),
                is_llm_active=org_preferences.llm_optin,
                landing_dashboard_id=curr_orguser.landing_dashboard_id,
                org_default_dashboard_id=org_default_dashboard,
            )
        )

    return res


@user_org_router.post("/organizations/users/", response=OrgUserResponse)
def post_organization_user(request, payload: OrgUserCreate):  # pylint: disable=unused-argument
    """this is the "signup" action
    creates a new OrgUser having specified email + password.
    no Org is created or attached at this time
    """
    payload.email = payload.email.lower().strip()
    retval, error = orguserfunctions.signup_orguser(payload)
    if error:
        raise HttpError(400, error)
    return retval


@user_org_router.post("/login/", auth=None)
def post_login(request, payload: LoginPayload):
    """Uses the username and password in the request to return a JWT auth token"""
    serializer = CustomTokenObtainSerializer(
        data={
            "username": payload.username,
            "password": payload.password,
        }
    )
    serializer.is_valid(raise_exception=True)
    token_data = serializer.validated_data
    retval = orguserfunctions.lookup_user(payload.username)
    retval["token"] = token_data["access"]
    retval["refresh_token"] = token_data["refresh"]
    return retval


@user_org_router.post("/login_token/")
def post_login_token(request):
    """
    Login user with token (used by embed-token provider).
    Invalidates the current short-lived iframe token and generates a new session token with longer expiry.
    """
    user: User = request.user
    if not user or not user.username:
        raise HttpError(401, "Invalid or missing token")

    # Generate new tokens with standard expiry for the session
    serializer = CustomTokenObtainSerializer.get_token(user)
    access_token = serializer.access_token

    # Get user data
    retval = orguserfunctions.lookup_user(user.username)
    retval["token"] = str(access_token)
    retval["refresh"] = str(serializer)

    return retval


@user_org_router.post("/logout/")
def post_logout(request):
    """
    Blacklists the refresh token on logout and clears httpOnly cookies.
    Gets refresh token from cookies for cookie-based authentication.
    """
    # Get refresh token from cookies
    refresh_token = request.COOKIES.get("refresh_token")

    # Try to blacklist the refresh token if we have one
    if refresh_token:
        try:
            token = RefreshToken(refresh_token)
            token_user_id = token.payload.get("user_id")
            if request.user and request.user.id == token_user_id:
                token.blacklist()
        except (TokenError, Exception):
            # Token is already invalid/expired or other error, continue with logout
            pass

    # Create response
    response = JsonResponse({"success": True})

    # Always try to clear cookies (harmless if they don't exist)
    # delete_cookie only accepts: key, path, domain, samesite
    response.delete_cookie("access_token", path="/")
    response.delete_cookie("refresh_token", path="/")

    return response


@user_org_router.post("/token/refresh", auth=None)
def post_token_refresh(request, payload: TokenRefreshPayload):
    """Refreshes the JWT token using the refresh token"""
    serializer = CustomTokenRefreshSerializer(data=payload.dict())
    serializer.is_valid(raise_exception=True)
    token_data = serializer.validated_data
    return {"token": token_data["access"]}


@user_org_router.get(
    "/organizations/users",
    response=List[OrgUserResponse],
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
                    queryset=RolePermission.objects.filter(role_id=F("role__id")).select_related(
                        "permission"
                    ),
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
                new_role_slug=curr_orguser.new_role.slug,
                wtype=warehouse.wtype if warehouse else None,
                permissions=[
                    {"slug": rolep.permission.slug, "name": rolep.permission.name}
                    for rolep in curr_orguser.new_role.rolepermissions.all()
                ],
                is_demo=(
                    curr_orguser.org.base_plan() == OrgType.DEMO if curr_orguser.org else False
                ),
            )
        )

    return res


@user_org_router.post("/v1/organizations/users/delete")
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


@user_org_router.put(
    "/v1/organizations/user_self/",
    response=OrgUserResponse,
)
def put_organization_user_self_v1(request, payload: OrgUserUpdatev1):
    """update the requestor's OrgUser"""
    orguser: OrgUser = request.orguser

    # not allowed to update own role
    payload.role_uuid = None
    return orguserfunctions.update_orguser_v1(orguser, payload)


@user_org_router.put(
    "/v1/organizations/users/",
    response=OrgUserResponse,
)
@has_permission(["can_edit_orguser"])
def put_organization_user_v1(request, payload: OrgUserUpdatev1):
    """update another OrgUser or themselves"""
    requestor_orguser: OrgUser = request.orguser

    orguser = OrgUser.objects.filter(
        user__email=payload.toupdate_email, org=request.orguser.org
    ).first()
    if orguser is None:
        raise HttpError(400, "could not find user having this email address in this org")

    # one can only update the role of user less than or equal to their role
    if payload.role_uuid and orguser.new_role.level > requestor_orguser.new_role.level:
        raise HttpError(403, "Insufficient permissions")

    # not allowed to update own role
    if requestor_orguser.user.email == orguser.user.email:
        payload.role_uuid = None

    return orguserfunctions.update_orguser_v1(orguser, payload)


@user_org_router.post(
    "/organizations/user_role/modify/",
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


@user_org_router.post("/organizations/warehouse/")
@has_permission(["can_create_warehouse"])
def post_organization_warehouse(request, payload: OrgWarehouseSchema):
    """registers a data warehouse for the org"""
    orguser: OrgUser = request.orguser
    _, error = airbytehelpers.create_warehouse(orguser.org, payload)
    if error:
        raise HttpError(400, error)

    return {"success": 1}


@user_org_router.get("/organizations/warehouses")
@has_permission(["can_view_warehouses"])
def get_organizations_warehouses(request):
    """returns all warehouses associated with this org"""
    orguser: OrgUser = request.orguser
    result, error = airbytehelpers.get_warehouses(orguser.org)
    if error:
        raise HttpError(400, error)
    return {"warehouses": result}


@user_org_router.post(
    "/users/forgot_password/",
    auth=None,
)
def post_forgot_password(request, payload: ForgotPasswordSchema):  # pylint: disable=unused-argument
    """step 1 of the forgot-password flow"""
    # Get the origin from request headers
    _, error = orguserfunctions.request_reset_password(payload.email, False)
    if error:
        raise HttpError(400, error)
    return {"success": 1}


@user_org_router.post(
    "/users/forgot_password_v2/",
    auth=None,
)
def post_forgot_password_v2(
    request, payload: ForgotPasswordSchema
):  # pylint: disable=unused-argument
    """step 1 of the forgot-password flow"""
    # Get the origin from request headers
    _, error = orguserfunctions.request_reset_password(payload.email, True)
    if error:
        raise HttpError(400, error)
    return {"success": 1}


@user_org_router.post("/users/reset_password/", auth=None)
def post_reset_password(request, payload: ResetPasswordSchema):  # pylint: disable=unused-argument
    """step 2 of the forgot-password flow"""
    _, error = orguserfunctions.confirm_reset_password(payload)
    if error:
        raise HttpError(400, error)
    return {"success": 1}


@user_org_router.post("/users/change_password/")  # from the settings panel
def change_password(request, payload: ChangePasswordSchema):  # pylint: disable=unused-argument
    """step 2 of the forgot-password flow"""
    orguser = request.orguser
    _, error = orguserfunctions.change_password(payload, orguser)
    if error:
        raise HttpError(400, error)
    return {"success": 1}


@user_org_router.get("/users/verify_email/resend")
@has_permission(["can_resend_email_verification"])
def get_verify_email_resend(request):  # pylint: disable=unused-argument
    """this api is hit when the user is logged in but the email is still not verified"""
    _, error = orguserfunctions.resend_verification_email(request.orguser, request.user.email)
    if error:
        raise HttpError(400, error)
    return {"success": 1}


@user_org_router.post("/users/verify_email/", auth=None)
def post_verify_email(request, payload: VerifyEmailSchema):  # pylint: disable=unused-argument
    """step 2 of the verify-email flow"""
    _, error = orguserfunctions.verify_email(payload)
    if error:
        raise HttpError(400, error)
    return {"success": 1}


# ====================== Invite users =========================================


@user_org_router.post(
    "/v1/organizations/users/invite/",
    response=NewInvitationSchema,
)
@has_permission(["can_create_invitation"])
def post_organization_user_invite_v1(request, payload: NewInvitationSchema):
    """Send an invitation to a user to join platform"""
    orguser: OrgUser = request.orguser
    retval, error = orguserfunctions.invite_user_v1(orguser, payload)
    if error:
        raise HttpError(400, error)
    return retval


@user_org_router.post("/v1/organizations/users/invite/accept/", response=OrgUserResponse, auth=None)
def post_organization_user_accept_invite_v1(
    request, payload: AcceptInvitationSchema
):  # pylint: disable=unused-argument
    """User accepting the invite sent with a valid invite code"""
    retval, error = orguserfunctions.accept_invitation_v1(payload)
    if error:
        raise HttpError(400, error)
    return retval


@user_org_router.get("/v1/users/invitations/")
@has_permission(["can_view_invitations"])
def get_invitations_v1(request):
    """Get all invitations sent by the current user"""
    retval, error = orguserfunctions.get_invitations_from_orguser_v1(request.orguser)
    if error:
        raise HttpError(400, error)
    return retval


@user_org_router.post("/users/invitations/resend/{invitation_id}")
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


@user_org_router.delete("/users/invitations/delete/{invitation_id}")
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


@user_org_router.post("/v1/organizations/", response=OrgSchema)
@has_permission(["can_create_org"])
@transaction.atomic
def post_organization_v1(request, payload: CreateOrgSchema):
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

    # create a new orgplan
    org_plan, error = orgfunctions.create_org_plan(payload, org)
    if error:
        raise HttpError(400, error)

    logger.info(f"{orguser.user.email} created new org {org.name}")
    return OrgSchema(name=org.name, airbyte_workspace_id=org.airbyte_workspace_id, slug=org.slug)


@user_org_router.post("/v1/organizations/free_trial")
@has_permission(["can_create_org"])
def post_organization_free_trial(request, payload: CreateFreeTrialOrgSchema):
    """create a new org with free trial plan and a warehouse/superset with it"""
    orguser: OrgUser = request.orguser

    userattributes = UserAttributes.objects.filter(user=orguser.user).first()
    if userattributes is None or userattributes.can_create_orgs is False:
        raise HttpError(403, "Insufficient permissions for this operation")

    if payload.base_plan != OrgPlanType.FREE_TRIAL:
        raise HttpError(403, "Only free trial orgs can be created")

    task = create_free_trial_org_account.delay(payload.dict())

    return {"task_id": task.id}


@user_org_router.delete("/v1/organizations/warehouses/")
@has_permission(["can_delete_warehouses"])
def delete_organization_warehouses_v1(request):
    """deletes all (references to) data warehouses for the org"""
    orguser: OrgUser = request.orguser
    org: Org = orguser.org
    if org is None:
        raise HttpError(400, "create an organization first")

    if org.base_plan() == OrgType.DEMO:
        raise HttpError(403, "insufficient permissions")

    cleanup_src = OrgCleanupService(org, dry_run=False)

    cleanup_src.delete_orchestrate_pipelines()
    cleanup_src.delete_warehouse()
    cleanup_src.delete_transformation_layer()

    return {"success": 1}


@user_org_router.post("/organizations/accept-tnc/")
@has_permission(["can_accept_tnc"])
def post_organization_accept_tnc(request):
    """accept the terms and conditions"""
    orguser: OrgUser = request.orguser
    _, error = orguserfunctions.accept_tnc(orguser)
    if error:
        raise HttpError(400, error)
    return {"success": 1}


@user_org_router.get("/organizations/flags")
@has_permission(["can_view_flags"])
def get_organization_feature_flags(request):
    """Get all feature flags for the current organization"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")

    # Get all feature flags for this organization (includes global + org overrides)
    feature_flags = get_all_feature_flags_for_org(orguser.org)

    return feature_flags


@user_org_router.get("/organizations/wren")
def get_organization_wren(request):
    """Fetch org_wren from the database and send to frontend"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")

    org_wren = OrgWren.objects.filter(org=orguser.org).first()
    if org_wren is None:
        raise HttpError(404, "org_wren not found")

    return {
        "wren_url": org_wren.wren_url,
    }


# ============================================
# Cookie-Based Auth Endpoints (v2)
# ============================================


@user_org_router.post("/v2/login/", auth=None)
def post_login_v2(request, payload: LoginPayload):
    """Login endpoint that sets httpOnly cookies instead of returning tokens in response"""
    serializer = CustomTokenObtainSerializer(
        data={
            "username": payload.username,
            "password": payload.password,
        }
    )
    serializer.is_valid(raise_exception=True)
    token_data = serializer.validated_data

    # Get user data (same as v1)
    retval = orguserfunctions.lookup_user(payload.username)

    # Create JsonResponse and set cookies
    response = JsonResponse(retval)

    # Set access token cookie
    response.set_cookie(
        "access_token",
        token_data["access"],
        httponly=settings.COOKIE_HTTPONLY,
        secure=settings.COOKIE_SECURE,
        samesite=settings.COOKIE_SAMESITE,
        path="/",
    )

    # Set refresh token cookie
    response.set_cookie(
        "refresh_token",
        token_data["refresh"],
        httponly=settings.COOKIE_HTTPONLY,
        secure=settings.COOKIE_SECURE,
        samesite=settings.COOKIE_SAMESITE,
        path="/",
    )

    return response


@user_org_router.post("/v2/token/refresh", auth=None, response={200: dict})
def post_token_refresh_v2(request):
    """Refresh token endpoint that reads refresh token from cookie and sets new access token in cookie"""
    # Get refresh token from cookie
    refresh_token = request.COOKIES.get("refresh_token")

    if not refresh_token:
        raise HttpError(401, "Refresh token not found")

    # Use the serializer to validate and get new access token
    serializer = CustomTokenRefreshSerializer(data={"refresh": refresh_token})
    serializer.is_valid(raise_exception=True)
    token_data = serializer.validated_data

    # Create response
    response = JsonResponse({"success": True})

    # Set new access token cookie
    response.set_cookie(
        "access_token",
        token_data["access"],
        httponly=settings.COOKIE_HTTPONLY,
        secure=settings.COOKIE_SECURE,
        samesite=settings.COOKIE_SAMESITE,
        path="/",
    )

    return response


@user_org_router.post("/v2/iframe-token/", response={200: dict})
@has_permission(["can_view_orgusers"])
def get_iframe_token(request):
    """
    Get a short-lived token for iframe communication.
    This endpoint validates the user's httpOnly cookie authentication
    and returns a temporary JWT token specifically for iframe use.
    """
    # Current auth middleware has already validated cookies and set request.user and request.orguser
    if request.orguser is None:
        raise HttpError(400, "requestor is not an OrgUser")

    orguser: OrgUser = request.orguser
    user: User = request.user

    # Use the same token generation logic as login to ensure all custom claims are included
    # This creates a refresh token with custom claims (like orguser_role_key) that the middleware expects
    refresh_token = CustomTokenObtainSerializer.get_token(user)
    access_token = refresh_token.access_token

    # Override access token expiration to 2 minutes for iframe use
    access_token.set_exp(lifetime=timedelta(minutes=2))

    return {
        "success": True,
        "iframe_token": str(access_token),
        "expires_in": 120,  # 2 minutes in seconds
        "org_slug": orguser.org.slug,
    }
