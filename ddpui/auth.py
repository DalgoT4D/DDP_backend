from functools import wraps
from ninja.security import HttpBearer
from ninja.errors import HttpError

from rest_framework.authtoken.models import Token
from django.contrib.auth.models import User
from ddpui.models.admin_user import AuthJWTToken
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import RolePermission
from ddpui.utils import thread
from rest_framework_simplejwt.tokens import AccessToken
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer

UNAUTHORIZED = "unauthorized"

SUPER_ADMIN_ROLE = "super-admin"
ACCOUNT_MANAGER_ROLE = "account-manager"
PIPELINE_MANAGER_ROLE = "pipeline-manager"
ANALYST_ROLE = "analyst"
GUEST_ROLE = "guest"


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


class CustomJwtAuthMiddleware(HttpBearer):
    def authenticate(self, request, token):
        # Validate and decode JWT using SimpleJWT's AccessToken
        token_payload = None
        try:
            access_token = AccessToken(token)
            token_payload = access_token.payload
        except Exception as err:
            raise HttpError(401, "Invalid or expired token")

        if token_payload and "user_id" in token_payload:
            user_id = token_payload["user_id"]
            request.user = User.objects.filter(id=user_id).first()
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

        raise HttpError(401, "Invalid or expired token")


class CustomTokenObtainSerializer(TokenObtainPairSerializer):
    @classmethod
    def get_token(cls, user):
        token = super().get_token(user)
        # Add more custom fields as needed in jwt payload
        # token["email"] = user.email

        # # fetch all permissions of each orguser
        # permissions = {}
        # for orguser in OrgUser.objects.filter(user=user).all():
        #     key = (orguser.id, orguser.org.slug)  # composite key
        #     role_permissions = RolePermission.objects.filter(role=orguser.new_role).values_list(
        #         "permission__slug", flat=True
        #     )
        #     permissions[key] = list(role_permissions)

        # token["permissions"] = permissions
        return token

    def validate(self, attrs):
        data = super().validate(attrs)
        access_token = data["access"]
        return {"access": access_token}
