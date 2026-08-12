import json
from typing import List, Literal, Optional
from datetime import datetime, timedelta

from dotenv import load_dotenv
from ninja import Router, File, Schema
from ninja.errors import HttpError
from ninja.files import UploadedFile
from rest_framework.authtoken import views
from django.utils.text import slugify
from django.db.models import Prefetch
from django.contrib.auth.models import User
from django.db.models import F
from django.http import JsonResponse, HttpResponse
from django.conf import settings
from rest_framework_simplejwt.tokens import RefreshToken, AccessToken
from yaml import error

from ddpui.auth import (
    has_permission,
    CustomTokenObtainSerializer,
    CustomTokenRefreshSerializer,
    blacklist_jti_in_redis,
)
from ddpui.core import orgfunctions, orguserfunctions
from ddpui.core.access.ownership import can_delete_resource
from ddpui.core.audit_log_service import create_audit_log
from ddpui.models.audit_log import AuditLogResourceType, AuditLogAction
from ddpui.models.org_user import (
    AcceptInvitationSchema,
    DeleteOrgUserPayload,
    ForgotPasswordSchema,
    Invitation,
    InvitationSchema,
    NewInvitationSchema,
    OrgUser,
    OrgUserCreate,
    OrgUserGroup,
    OrgUserGroupMember,
    OrgUserResponse,
    OrgUserUpdate,
    OrgUserUpdateNewRole,
    OrgUserUpdatev1,
    ResetPasswordSchema,
    ChangePasswordSchema,
    UserAttributes,
    VerifyEmailSchema,
    LoginPayload,
    LogoutPayload,
)
from ddpui.models.org_plans import OrgPlanType
from ddpui.models.org_wren import OrgWren
from ddpui.models.role_based_access import Role, RolePermission
from ddpui.models.org import OrgWarehouse, Org, OrgType
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.resource_share import ResourceShare, ResourceSharePrincipalType

from ddpui.schemas.org_schema import OrgSchema, CreateOrgSchema, OrgLogoResponse, OrgLogoUrlPayload
from ddpui.schemas.org_warehouse_schema import OrgWarehouseSchema

from ddpui.services.org_cleanup_service import OrgCleanupService
from ddpui.ddpairbyte import airbytehelpers

from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.feature_flags import get_all_feature_flags_for_org
from ddpui.utils.response_wrapper import api_response, ApiResponse
from ddpui.core.org_logo.exceptions import (
    OrgLogoNotFoundError,
    OrgLogoValidationError,
    OrgLogoS3Error,
    OrgLogoFetchError,
)

from django.db import transaction

user_org_router = Router()
load_dotenv()
logger = CustomLogger("ddpui")


def _record_audit_log(
    orguser,
    resource_type,
    action,
    resource_id: str = "",
    resource_fields: dict | None = None,
):
    """Emit an audit log for the current org user when available."""
    if not orguser or not getattr(orguser, "org", None):
        return

    create_audit_log(
        org=orguser.org,
        orguser=orguser,
        resource_type=resource_type,
        resource_id=resource_id,
        action=action,
        resource_fields=resource_fields,
    )


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
                user_id=user.id,
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
                subscription_plan=(curr_orguser.org.base_plan() if curr_orguser.org else None),
                work_domain=curr_orguser.work_domain,
                has_seen_rbac_notice=curr_orguser.has_seen_rbac_notice,
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
    # Capture orguser before logout for audit logging
    orguser = getattr(request, "orguser", None)

    # Blacklist access token
    access_token_str = request.COOKIES.get("access_token")
    if access_token_str:
        blacklist_jti_in_redis(access_token_str, AccessToken)

    # Blacklist refresh token
    refresh_token_str = request.COOKIES.get("refresh_token")
    if refresh_token_str:
        blacklist_jti_in_redis(refresh_token_str, RefreshToken)

    response = JsonResponse({"success": True})
    response.delete_cookie("access_token", path="/")
    response.delete_cookie("refresh_token", path="/")

    # Audit log: user logged out
    _record_audit_log(
        orguser,
        AuditLogResourceType.AUTH,
        AuditLogAction.LOGOUT,
    )

    return response


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
                user_id=curr_orguser.user.id,
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
                subscription_plan=(curr_orguser.org.base_plan() if curr_orguser.org else None),
            )
        )

    return res


class PeopleRow(Schema):
    email: str
    role_slug: str
    role_name: str
    status: Literal["active", "pending"]
    created_by_email: Optional[str] = None
    orguser_id: Optional[int] = None
    invitation_id: Optional[int] = None
    created_at: Optional[datetime] = None


@user_org_router.get(
    "/v1/organizations/people",
    response=List[PeopleRow],
)
@has_permission(["can_view_orgusers", "can_view_invitations"])
def get_organization_people(request):
    """merged list of active orgusers + pending invitations for the org (People tab)"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")
    org: Org = orguser.org

    rows: List[PeopleRow] = []

    for curr_orguser in OrgUser.objects.filter(org=org).select_related("user", "new_role"):
        rows.append(
            PeopleRow(
                email=curr_orguser.user.email,
                role_slug=curr_orguser.new_role.slug,
                role_name=curr_orguser.new_role.name,
                status="active",
                created_by_email=None,
                orguser_id=curr_orguser.id,
                invitation_id=None,
                created_at=curr_orguser.user.date_joined,
            )
        )

    for invitation in Invitation.objects.filter(invited_by__org=org).select_related(
        "invited_by__user", "invited_new_role"
    ):
        rows.append(
            PeopleRow(
                email=invitation.invited_email,
                role_slug=invitation.invited_new_role.slug if invitation.invited_new_role else "",
                role_name=invitation.invited_new_role.name if invitation.invited_new_role else "",
                status="pending",
                created_by_email=invitation.invited_by.user.email,
                orguser_id=None,
                invitation_id=invitation.id,
                created_at=invitation.invited_on,
            )
        )

    return rows


# ==============================================================================
# User Groups (Access → Groups tab)


class GroupMemberSchema(Schema):
    member_id: int
    orguser_id: Optional[int] = None
    email: str
    role_name: Optional[str] = None
    status: Literal["active", "pending"]


class GroupListRow(Schema):
    id: int
    name: str
    member_count: int
    created_by_email: Optional[str] = None
    created_at: datetime


class GroupDetailSchema(Schema):
    id: int
    name: str
    created_by_email: Optional[str] = None
    created_at: datetime
    members: List[GroupMemberSchema]


class CreateGroupPayload(Schema):
    name: str
    orguser_ids: List[int] = []
    pending_emails: List[str] = []
    invite_role_uuid: Optional[str] = None


class UpdateGroupPayload(Schema):
    name: str


class AddMembersPayload(Schema):
    orguser_ids: List[int] = []
    pending_emails: List[str] = []
    invite_role_uuid: Optional[str] = None


def _serialize_group_row(group: OrgUserGroup) -> GroupListRow:
    created_by_email = (
        group.created_by.user.email if group.created_by and group.created_by.user else None
    )
    return GroupListRow(
        id=group.id,
        name=group.name,
        member_count=group.members.count(),
        created_by_email=created_by_email,
        created_at=group.created_at,
    )


def _serialize_group_detail(group: OrgUserGroup) -> GroupDetailSchema:
    created_by_email = (
        group.created_by.user.email if group.created_by and group.created_by.user else None
    )
    members: List[GroupMemberSchema] = []
    for m in group.members.select_related("orguser__user", "orguser__new_role", "invitation"):
        if m.orguser_id is not None and m.orguser is not None:
            members.append(
                GroupMemberSchema(
                    member_id=m.id,
                    orguser_id=m.orguser_id,
                    email=m.orguser.user.email,
                    role_name=m.orguser.new_role.name if m.orguser.new_role else None,
                    status="active",
                )
            )
        elif m.invitation is not None:
            members.append(
                GroupMemberSchema(
                    member_id=m.id,
                    orguser_id=None,
                    email=m.invitation.invited_email,
                    role_name=(
                        m.invitation.invited_new_role.name
                        if m.invitation.invited_new_role
                        else None
                    ),
                    status="pending",
                )
            )
    return GroupDetailSchema(
        id=group.id,
        name=group.name,
        created_by_email=created_by_email,
        created_at=group.created_at,
        members=members,
    )


def _resolve_pending_emails(
    org: Org,
    orguser: OrgUser,
    emails: List[str],
    invite_role_uuid: Optional[str],
) -> dict[str, int]:
    """For each email in `emails`, ensure an Invitation exists on the org.
    Returns {email → invitation_id}. Emails that already belong to an active
    orguser are ignored and will be resolved via `orguser_ids` later.
    """
    if not emails:
        return {}
    normalized = [e.strip().lower() for e in emails if e and e.strip()]
    if not normalized:
        return {}

    existing_active = set(
        OrgUser.objects.filter(org=org, user__email__in=normalized).values_list(
            "user__email", flat=True
        )
    )
    to_process = [e for e in normalized if e not in existing_active]
    if not to_process:
        return {}

    existing_invites = {
        inv.invited_email: inv.id
        for inv in Invitation.objects.filter(invited_by__org=org, invited_email__in=to_process)
    }
    to_invite = [e for e in to_process if e not in existing_invites]

    if to_invite:
        if not invite_role_uuid:
            raise HttpError(400, "invite_role_uuid is required to invite new emails to a group")
        if not Role.objects.filter(uuid=invite_role_uuid).exists():
            raise HttpError(400, "invalid invite_role_uuid")

        for email in to_invite:
            payload = NewInvitationSchema(
                invited_email=email,
                invited_role_uuid=invite_role_uuid,
            )
            _, error = orguserfunctions.invite_user_v1(orguser, payload)
            if error:
                raise HttpError(400, f"failed to invite {email}: {error}")
            invitation = Invitation.objects.filter(invited_by__org=org, invited_email=email).first()
            if invitation is not None:
                existing_invites[email] = invitation.id

    return existing_invites


def _add_members_to_group(
    group: OrgUserGroup,
    orguser_ids: List[int],
    email_to_invitation_id: dict[str, int],
) -> None:
    """Idempotent membership creation (skips existing rows)."""
    org = group.org

    if orguser_ids:
        valid_orguser_ids = set(
            OrgUser.objects.filter(org=org, id__in=orguser_ids).values_list("id", flat=True)
        )
        existing_orguser_ids = set(
            OrgUserGroupMember.objects.filter(
                group=group, orguser_id__in=valid_orguser_ids
            ).values_list("orguser_id", flat=True)
        )
        for orguser_id in valid_orguser_ids - existing_orguser_ids:
            OrgUserGroupMember.objects.create(group=group, orguser_id=orguser_id)

    if email_to_invitation_id:
        invitation_ids = set(email_to_invitation_id.values())
        existing_invitation_ids = set(
            OrgUserGroupMember.objects.filter(
                group=group, invitation_id__in=invitation_ids
            ).values_list("invitation_id", flat=True)
        )
        for invitation_id in invitation_ids - existing_invitation_ids:
            OrgUserGroupMember.objects.create(group=group, invitation_id=invitation_id)


@user_org_router.get(
    "/v1/organizations/user_groups",
    response=List[GroupListRow],
)
@has_permission(["can_view_user_groups"])
def list_user_groups(request):
    """list all user groups in the org (Groups tab)"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")
    groups = (
        OrgUserGroup.objects.filter(org=orguser.org)
        .select_related("created_by__user")
        .order_by("-created_at")
    )
    return [_serialize_group_row(g) for g in groups]


@user_org_router.post(
    "/v1/organizations/user_groups",
    response=GroupDetailSchema,
)
@has_permission(["can_create_user_group"])
@transaction.atomic
def create_user_group(request, payload: CreateGroupPayload):
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")

    name = payload.name.strip()
    if not name:
        raise HttpError(400, "group name is required")

    if OrgUserGroup.objects.filter(org=orguser.org, name__iexact=name).exists():
        raise HttpError(400, "a group with this name already exists")

    email_to_invitation_id = _resolve_pending_emails(
        orguser.org, orguser, payload.pending_emails, payload.invite_role_uuid
    )

    group = OrgUserGroup.objects.create(org=orguser.org, name=name, created_by=orguser)
    _add_members_to_group(group, payload.orguser_ids, email_to_invitation_id)

    return _serialize_group_detail(group)


@user_org_router.get(
    "/v1/organizations/user_groups/{group_id}",
    response=GroupDetailSchema,
)
@has_permission(["can_view_user_groups"])
def get_user_group(request, group_id: int):
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")
    group = OrgUserGroup.objects.filter(id=group_id, org=orguser.org).first()
    if group is None:
        raise HttpError(404, "group not found")
    return _serialize_group_detail(group)


@user_org_router.patch(
    "/v1/organizations/user_groups/{group_id}",
    response=GroupDetailSchema,
)
@has_permission(["can_edit_user_group"])
def rename_user_group(request, group_id: int, payload: UpdateGroupPayload):
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")
    group = OrgUserGroup.objects.filter(id=group_id, org=orguser.org).first()
    if group is None:
        raise HttpError(404, "group not found")

    name = payload.name.strip()
    if not name:
        raise HttpError(400, "group name is required")

    if (
        OrgUserGroup.objects.filter(org=orguser.org, name__iexact=name)
        .exclude(id=group.id)
        .exists()
    ):
        raise HttpError(400, "a group with this name already exists")

    group.name = name
    group.save(update_fields=["name", "updated_at"])
    return _serialize_group_detail(group)


@user_org_router.delete(
    "/v1/organizations/user_groups/{group_id}",
)
@has_permission(["can_delete_user_group"])
def delete_user_group(request, group_id: int):
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")
    group = OrgUserGroup.objects.filter(id=group_id, org=orguser.org).first()
    if group is None:
        raise HttpError(404, "group not found")

    if not can_delete_resource(orguser, group):
        raise HttpError(403, "only the group creator or an admin can delete this group")

    ResourceShare.objects.filter(
        org=group.org,
        principal_type=ResourceSharePrincipalType.GROUP,
        principal_id=group.id,
    ).delete()
    group.delete()
    return {"success": 1}


@user_org_router.post(
    "/v1/organizations/user_groups/{group_id}/members",
    response=GroupDetailSchema,
)
@has_permission(["can_edit_user_group"])
@transaction.atomic
def add_user_group_members(request, group_id: int, payload: AddMembersPayload):
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")
    group = OrgUserGroup.objects.filter(id=group_id, org=orguser.org).first()
    if group is None:
        raise HttpError(404, "group not found")

    email_to_invitation_id = _resolve_pending_emails(
        orguser.org, orguser, payload.pending_emails, payload.invite_role_uuid
    )
    _add_members_to_group(group, payload.orguser_ids, email_to_invitation_id)
    return _serialize_group_detail(group)


@user_org_router.delete(
    "/v1/organizations/user_groups/{group_id}/members/{member_id}",
)
@has_permission(["can_edit_user_group"])
def remove_user_group_member(request, group_id: int, member_id: int):
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")
    group = OrgUserGroup.objects.filter(id=group_id, org=orguser.org).first()
    if group is None:
        raise HttpError(404, "group not found")

    deleted, _ = OrgUserGroupMember.objects.filter(group=group, id=member_id).delete()
    if deleted == 0:
        raise HttpError(404, "member not found")
    return {"success": 1}


@user_org_router.post("/v1/organizations/users/delete")
@has_permission(["can_delete_orguser"])
def delete_organization_users_v1(request, payload: DeleteOrgUserPayload):
    """delete the orguser posted"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")

    # Capture the email before deletion for audit log
    deleted_email = payload.email

    _, error = orguserfunctions.delete_orguser_v1(orguser, payload)
    if error:
        raise HttpError(400, error)

    # Audit log: user removed from org
    _record_audit_log(
        orguser,
        AuditLogResourceType.ORG_USER,
        AuditLogAction.DELETE,
        resource_fields={"email": deleted_email},
    )

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

    # Audit log: user role changed
    _record_audit_log(
        orguser,
        AuditLogResourceType.ORG_USER,
        AuditLogAction.UPDATE,
        resource_id=str(orguser_to_be_assigned.id),
        resource_fields={
            "email": orguser_to_be_assigned.user.email,
            "role": role_to_be_assgined.slug,
        },
    )

    return {"success": 1}


@user_org_router.post("/organizations/warehouse/")
@has_permission(["can_create_warehouse"])
def post_organization_warehouse(request, payload: OrgWarehouseSchema):
    """registers a data warehouse for the org"""
    orguser: OrgUser = request.orguser
    try:
        orgwarehouse = airbytehelpers.create_warehouse(orguser.org, payload)
        create_audit_log(
            org=orguser.org,
            orguser=orguser,
            resource_type=AuditLogResourceType.WAREHOUSE,
            resource_id=orgwarehouse.airbyte_destination_id if orgwarehouse else "",
            action=AuditLogAction.CREATE,
            # Never log payload.airbyteConfig — warehouse connection credentials.
            resource_fields={"wtype": payload.wtype, "name": payload.name},
        )
    except ValueError as error:
        raise HttpError(400, str(error))
    except Exception as error:
        logger.exception(error)
        raise HttpError(500, "failed to create warehouse") from error

    return {"destinationId": orgwarehouse.airbyte_destination_id}


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
    "/users/forgot_password_v2/",
    auth=None,
)
def post_forgot_password_v2(
    request, payload: ForgotPasswordSchema
):  # pylint: disable=unused-argument
    """step 1 of the forgot-password flow"""
    _, error = orguserfunctions.request_reset_password(payload.email, True)
    if error:
        raise HttpError(400, error)

    # Audit log: password reset requested
    user = User.objects.filter(email__iexact=payload.email).first()
    if user:
        orguser = OrgUser.objects.filter(user=user).first()
        if orguser and orguser.org:
            create_audit_log(
                org=orguser.org,
                orguser=orguser,
                resource_type=AuditLogResourceType.AUTH,
                resource_id="",
                action=AuditLogAction.PASSWORD_RESET_REQUESTED,
            )

    return {"success": 1}


@user_org_router.post("/users/reset_password/", auth=None)
def post_reset_password(request, payload: ResetPasswordSchema):  # pylint: disable=unused-argument
    """step 2 of the forgot-password flow"""
    orguser, error = orguserfunctions.confirm_reset_password(payload)
    if error:
        raise HttpError(400, error)

    # Audit log: password reset completed
    if orguser and orguser.org:
        create_audit_log(
            org=orguser.org,
            orguser=orguser,
            resource_type=AuditLogResourceType.AUTH,
            resource_id="",
            action=AuditLogAction.PASSWORD_RESET_COMPLETED,
        )

    return {"success": 1}


@user_org_router.post("/users/change_password/")
def change_password(request, payload: ChangePasswordSchema):  # pylint: disable=unused-argument
    """change password from the user menu in the header"""
    orguser = request.orguser
    _, error = orguserfunctions.change_password(payload, orguser)
    if error:
        raise HttpError(400, error)

    # Audit log: password changed
    create_audit_log(
        org=orguser.org,
        orguser=orguser,
        resource_type=AuditLogResourceType.AUTH,
        resource_id="",
        action=AuditLogAction.PASSWORD_CHANGED,
    )

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
    orguser, error = orguserfunctions.verify_email(payload)
    if error:
        raise HttpError(400, error)

    # Audit log: email verified
    if orguser and orguser.org:
        create_audit_log(
            org=orguser.org,
            orguser=orguser,
            resource_type=AuditLogResourceType.AUTH,
            resource_id="",
            action=AuditLogAction.EMAIL_VERIFIED,
        )

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

    # Audit log: invitation sent
    # resource_id is empty because NewInvitationSchema has no id field;
    # the invited email in resource_fields is sufficient to identify who was invited
    create_audit_log(
        org=orguser.org,
        orguser=orguser,
        resource_type=AuditLogResourceType.INVITATION,
        resource_id="",
        action=AuditLogAction.CREATE,
        resource_fields={"email": payload.invited_email},
    )

    return retval


@user_org_router.post("/v1/organizations/users/invite/accept/", response=OrgUserResponse, auth=None)
def post_organization_user_accept_invite_v1(
    request, payload: AcceptInvitationSchema
):  # pylint: disable=unused-argument
    """User accepting the invite sent with a valid invite code"""
    # Get invitation details before it's deleted (for audit log)
    invitation = Invitation.objects.filter(invite_code=payload.invite_code).first()
    invited_email = invitation.invited_email if invitation else ""
    invited_by_org = invitation.invited_by.org if invitation and invitation.invited_by else None

    retval, error = orguserfunctions.accept_invitation_v1(payload)
    if error:
        raise HttpError(400, error)

    # Audit log: invitation accepted (user added to org)
    if invited_by_org:
        # Look up the orguser
        orguser = OrgUser.objects.filter(
            user__email__iexact=invited_email, org=invited_by_org
        ).first()
        if orguser:
            create_audit_log(
                org=invited_by_org,
                orguser=orguser,
                resource_type=AuditLogResourceType.INVITATION,
                resource_id="",
                action=AuditLogAction.UPDATE,
                resource_fields={"email": invited_email, "status": "accepted"},
            )
            create_audit_log(
                org=invited_by_org,
                orguser=orguser,
                resource_type=AuditLogResourceType.ORG_USER,
                resource_id=str(orguser.id),
                action=AuditLogAction.CREATE,
                resource_fields={"email": invited_email},
            )

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
    """Resend an invitation"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    # Get invitation details for audit log
    invitation = Invitation.objects.filter(id=invitation_id).first()

    _, error = orguserfunctions.resend_invitation(invitation_id)
    if error:
        raise HttpError(400, error)

    # Audit log: invitation resent
    create_audit_log(
        org=orguser.org,
        orguser=orguser,
        resource_type=AuditLogResourceType.INVITATION,
        resource_id=str(invitation_id),
        action=AuditLogAction.UPDATE,
        resource_fields={
            "email": invitation.invited_email if invitation else "",
            "action": "resent",
        },
    )

    return {"success": 1}


@user_org_router.delete("/users/invitations/delete/{invitation_id}")
@has_permission(["can_delete_invitation"])
def delete_invitation(request, invitation_id):
    """Delete an invitation"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    invitation = Invitation.objects.filter(id=invitation_id).first()

    # Capture email before deletion for audit log
    invited_email = invitation.invited_email if invitation else ""

    if invitation:
        invitation.delete()

        # Audit log: invitation deleted
        create_audit_log(
            org=orguser.org,
            orguser=orguser,
            resource_type=AuditLogResourceType.INVITATION,
            resource_id=str(invitation_id),
            action=AuditLogAction.DELETE,
            resource_fields={"email": invited_email},
        )

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

    # Audit log: organization created
    create_audit_log(
        org=org,
        orguser=orguser,
        resource_type=AuditLogResourceType.ORG,
        resource_id=str(org.id),
        action=AuditLogAction.CREATE,
        resource_fields={
            "base_plan": payload.base_plan,
            "subscription_duration": payload.subscription_duration,
            "superset_included": payload.superset_included,
        },
    )

    logger.info(f"{orguser.user.email} created new org {org.name}")
    return OrgSchema(name=org.name, airbyte_workspace_id=org.airbyte_workspace_id, slug=org.slug)


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
    warehouse_info = cleanup_src.delete_warehouse()
    cleanup_src.delete_transformation_layer()

    create_audit_log(
        org=org,
        orguser=orguser,
        resource_type=AuditLogResourceType.WAREHOUSE,
        resource_id=warehouse_info["airbyte_destination_id"] or str(org.id),
        action=AuditLogAction.DELETE,
        resource_fields={"name": warehouse_info["name"]},
    )

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

    # Audit log: user logged in
    user = User.objects.filter(email__iexact=payload.username).first()
    if user:
        orguser = OrgUser.objects.filter(user=user).first()
        if orguser and orguser.org:
            create_audit_log(
                org=orguser.org,
                orguser=orguser,
                resource_type=AuditLogResourceType.AUTH,
                resource_id="",
                action=AuditLogAction.LOGIN,
            )

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


@user_org_router.put("/org/logo/", response=ApiResponse[OrgLogoResponse])
@has_permission(["can_edit_org_notification_settings"])
def upload_logo_file(request, file: UploadedFile = File(...)):
    """Upload an image file as the org logo"""
    orguser: OrgUser = request.orguser

    # Check if logo already exists (update vs create)
    had_logo = bool(orguser.org.logo_url)

    try:
        org = orgfunctions.upload_logo_from_file(
            file_bytes=file.read(),
            content_type=file.content_type or "",
            filename=file.name or "",
            org=orguser.org,
        )

        # Audit log: logo uploaded/updated
        create_audit_log(
            org=orguser.org,
            orguser=orguser,
            resource_type=AuditLogResourceType.ORG,
            resource_id=str(orguser.org.id),
            action=AuditLogAction.UPDATE if had_logo else AuditLogAction.CREATE,
            resource_fields={"logo_url": org.logo_url},
        )

        return api_response(
            success=True,
            data=OrgLogoResponse.from_model(org),
            message="Logo uploaded successfully",
        )
    except OrgLogoValidationError as e:
        raise HttpError(400, str(e)) from e
    except OrgLogoS3Error as e:
        raise HttpError(502, str(e)) from e


@user_org_router.put("/org/logo/url/", response=ApiResponse[OrgLogoResponse])
@has_permission(["can_edit_org_notification_settings"])
def upload_logo_from_url(request, payload: OrgLogoUrlPayload):
    """Store an external image URL directly as the org logo — no S3 upload"""
    orguser: OrgUser = request.orguser

    # Check if logo already exists (update vs create)
    had_logo = bool(orguser.org.logo_url)

    try:
        org = orgfunctions.upload_logo_from_url(
            image_url=payload.image_url,
            org=orguser.org,
        )

        # Audit log: logo uploaded/updated
        create_audit_log(
            org=orguser.org,
            orguser=orguser,
            resource_type=AuditLogResourceType.ORG,
            resource_id=str(orguser.org.id),
            action=AuditLogAction.UPDATE if had_logo else AuditLogAction.CREATE,
            resource_fields={"logo_url": org.logo_url},
        )

        return api_response(
            success=True,
            data=OrgLogoResponse.from_model(org),
            message="Logo URL saved successfully",
        )
    except OrgLogoValidationError as e:
        raise HttpError(400, str(e)) from e
    except Exception as e:
        logger.error(f"Failed to save logo URL for {orguser.org.slug}: {e}")
        raise HttpError(500, "Failed to save logo URL") from e


@user_org_router.delete("/org/logo/", response=ApiResponse)
@has_permission(["can_edit_org_notification_settings"])
def delete_logo(request):
    """Delete the org logo from S3 and clear the fields on Org"""
    orguser: OrgUser = request.orguser

    try:
        orgfunctions.delete_logo(orguser.org)

        # Audit log: logo deleted
        create_audit_log(
            org=orguser.org,
            orguser=orguser,
            resource_type=AuditLogResourceType.ORG,
            resource_id=str(orguser.org.id),
            action=AuditLogAction.DELETE,
        )

        return api_response(success=True, message="Logo deleted successfully")
    except OrgLogoNotFoundError as e:
        raise HttpError(404, str(e)) from e
    except OrgLogoS3Error as e:
        raise HttpError(502, str(e)) from e


@user_org_router.get("/org/logo/")
@has_permission(["can_view_orgusers"])
def proxy_org_logo_image(request):
    """Return raw logo bytes so the frontend can use them in canvas without CORS issues."""
    orguser: OrgUser = request.orguser
    if not orguser.org.logo_url:
        raise HttpError(404, "No logo found")
    try:
        image_bytes, content_type = orgfunctions.get_logo_bytes(orguser.org.logo_url)
        return HttpResponse(image_bytes, content_type=content_type)
    except (OrgLogoS3Error, OrgLogoFetchError) as e:
        raise HttpError(502, str(e)) from e
