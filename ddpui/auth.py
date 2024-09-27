from functools import wraps
from ninja.security import HttpBearer
from ninja.errors import HttpError


from rest_framework.authentication import TokenAuthentication
from rest_framework.authtoken.models import Token

from ddpui.models.org_user import OrgUser
from ddpui.models.admin_user import AdminUser
from ddpui.models.role_based_access import RolePermission
from ddpui.utils import thread

from ddpui.models.org_user import OrgUserRole

UNAUTHORIZED = "unauthorized"

SUPER_ADMIN_ROLE = "super-admin"
ACCOUNT_MANAGER_ROLE = "account-manager"
PIPELINE_MANAGER_ROLE = "pipeline-manager"
ANALYST_ROLE = "analyst"
GUEST_ROLE = "guest"


class BearerAuthentication(TokenAuthentication):
    """
    This allows us to send the Authorization header "Bearer <token>"
    instead of "Token <token>"
    """

    keyword = "Bearer"


class PlatformAdmin(HttpBearer):
    """
    ninja middleware to look up a user and an admin-user
    (if it exists) from an auth token
    """

    def authenticate(self, request, token):
        tokenrecord = Token.objects.filter(key=token).first()
        if tokenrecord and tokenrecord.user:
            request.user = tokenrecord.user
            adminuser = AdminUser.objects.filter(user=request.user).first()
            if adminuser is not None:
                request.adminuser = adminuser
                return tokenrecord
        raise HttpError(400, UNAUTHORIZED)


def has_permission(permission_slugs: list):
    def decorator(api_endpoint):
        @wraps(api_endpoint)
        def wrapper(*args, **kwargs):
            # request will have set of permissions that are allowed
            # check if permission_slug lies in this set
            # throw error if nots
            request = args[0]
            try:
                if not request.permissions or len(request.permissions) == 0:
                    raise HttpError(403, "not allowed")

                if not set(request.permissions).issuperset(set(permission_slugs)):
                    raise HttpError(403, "not allowed")
            except:
                raise HttpError(404, UNAUTHORIZED)

            return api_endpoint(*args, **kwargs)

        return wrapper

    return decorator


def authenticate_org_user(request, token, allowed_roles, require_org):
    """docstring"""
    tokenrecord = Token.objects.filter(key=token).first()
    if tokenrecord and tokenrecord.user:
        request.user = tokenrecord.user
        q_orguser = OrgUser.objects.filter(user=request.user)
        if request.headers.get("x-dalgo-org"):
            orgslug = request.headers["x-dalgo-org"]
            q_orguser = q_orguser.filter(org__slug=orgslug)
        orguser = q_orguser.first()
        if orguser is not None:
            if require_org and orguser.org is None:
                raise HttpError(400, "register an organization first")
            if orguser.role in allowed_roles:
                request.orguser = orguser
                return request
    raise HttpError(400, UNAUTHORIZED)


class CustomAuthMiddleware(HttpBearer):
    """new middleware that works based on permissions from db"""

    def authenticate(self, request, token):
        tokenrecord = Token.objects.filter(key=token).first()
        if tokenrecord and tokenrecord.user:
            request.user = tokenrecord.user
            q_orguser = OrgUser.objects.filter(user=request.user)
            if request.headers.get("x-dalgo-org"):
                orgslug = request.headers["x-dalgo-org"]
                q_orguser = q_orguser.filter(org__slug=orgslug)
            orguser = q_orguser.select_related("org", "user").first()
            if orguser is not None:
                if orguser.org is None:
                    raise HttpError(400, "register an organization first")

                permission_slugs = RolePermission.objects.filter(role=orguser.new_role).values_list(
                    "permission__slug", flat=True
                )

                request.permissions = list(permission_slugs) or []
                request.orguser = orguser
                thread.set_current_request(request)
                return request

        raise HttpError(400, UNAUTHORIZED)


class AnyOrgUser(HttpBearer):
    """ninja middleware to allow any org user"""

    def authenticate(self, request, token):
        return authenticate_org_user(
            request,
            token,
            [
                OrgUserRole.REPORT_VIEWER,
                OrgUserRole.PIPELINE_MANAGER,
                OrgUserRole.ACCOUNT_MANAGER,
            ],
            False,
        )


class CanManagePipelines(HttpBearer):
    """ninja middleware to allow only account-owners or pipeline-managers"""

    def authenticate(self, request, token):
        return authenticate_org_user(
            request,
            token,
            [
                OrgUserRole.PIPELINE_MANAGER,
                OrgUserRole.ACCOUNT_MANAGER,
            ],
            True,
        )


class CanManageUsers(HttpBearer):
    """
    ninja middleware to look up a user and an org-user
    (if it exists and is an account-owner or a pipeline-manager) from an auth token
    """

    def authenticate(self, request, token):
        return authenticate_org_user(
            request,
            token,
            [
                OrgUserRole.PIPELINE_MANAGER,
                OrgUserRole.ACCOUNT_MANAGER,
            ],
            True,
        )


class FullAccess(HttpBearer):
    """
    ninja middleware to look up a user and an org-user
    (if it exists and is an account-owner) from an auth token
    """

    def authenticate(self, request, token):
        return authenticate_org_user(
            request,
            token,
            [
                OrgUserRole.ACCOUNT_MANAGER,
            ],
            False,
        )
