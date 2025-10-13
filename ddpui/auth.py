import os
import uuid
import json
from functools import wraps
from ninja.security import HttpBearer
from ninja.errors import HttpError

from rest_framework.authtoken.models import Token
from rest_framework_simplejwt.tokens import AccessToken
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer, TokenRefreshSerializer
from django.contrib.auth.models import User

from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import RolePermission
from ddpui.utils import thread
from ddpui.utils.redis_client import RedisClient
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

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
                return request

        raise HttpError(400, UNAUTHORIZED)


def set_roles_and_permissions_in_redis(
    redis_client: RedisClient, role_permissions_key: str
) -> dict:
    """reads the RolesPermissions table and writes the mapping to redis"""
    role_permissions = {}
    for role_perm in RolePermission.objects.select_related("permission").all():
        role_permissions.setdefault(str(role_perm.role_id), [])
        role_permissions.get(str(role_perm.role_id)).append(role_perm.permission.slug)
    # set in redis
    redis_client.set(role_permissions_key, json.dumps(role_permissions))
    return role_permissions


class CustomJwtAuthMiddleware(HttpBearer):
    """the authenticate() function is called on every authenticated request via django middleware"""

    def __call__(self, request):
        # For /api/login_token/ endpoint, prioritize Authorization header over cookies
        # This is needed for iframe token validation
        if request.path == "/api/login_token/":
            logger.info(
                "CustomJwtAuthMiddleware: prioritizing Authorization header for /api/login_token/"
            )
            # First try Authorization header
            return super().__call__(request)

        # For all other endpoints, prioritize cookies first
        cookie_token = request.COOKIES.get("access_token")

        # If we have a cookie token, use it directly
        if cookie_token:
            return self.authenticate(request, cookie_token)

        # Otherwise, fall back to the default HttpBearer behavior (Authorization header)
        return super().__call__(request)

    def authenticate(self, request, token=None):
        if not token:
            raise HttpError(401, "No authentication token provided")

        # Validate and decode JWT using SimpleJWT's AccessToken
        token_payload = None
        try:
            access_token = AccessToken(token)
            token_payload = access_token.payload
        except Exception as err:
            logger.exception("Invalid or expired token: %s", err)
            raise HttpError(401, "Invalid or expired token") from err

        role_permissions_key = os.getenv("ROLE_PERMISSIONS_REDIS_KEY", "dalgo_permissions_key")

        user_id = token_payload.get("user_id")
        orguser_role_key = token_payload.get(
            "orguser_role_key"
        )  # this is currently f"orguser_role:{user.id}"

        if token_payload and user_id:
            request.user = User.objects.filter(id=user_id).first()
            q_orguser = OrgUser.objects.filter(user=request.user)
            if request.headers.get("x-dalgo-org"):
                orgslug = request.headers["x-dalgo-org"]
                q_orguser = q_orguser.filter(org__slug=orgslug)
            orguser = q_orguser.select_related("org").first()
            if orguser is not None:
                if orguser.org is None:
                    raise HttpError(400, "register an organization first")

                redis_client = RedisClient.get_instance()
                orguser_role_id = None
                permissions_json = None

                orguser_role_map_json = None
                if orguser_role_key:
                    orguser_role_map_str = redis_client.get(orguser_role_key)
                    if orguser_role_map_str:
                        try:
                            orguser_role_map_json = json.loads(orguser_role_map_str)
                        except ValueError:
                            orguser_role_map_json = None
                        if (
                            not isinstance(orguser_role_map_json, dict)
                            or len(orguser_role_map_json.keys()) == 0
                        ):
                            # cache corruption? build it below
                            orguser_role_map_json = None

                # its possible that new orguser is created for the user after the cache was last updated
                if not orguser_role_map_json:
                    orguser_role_map_json = {str(orguser.id): orguser.new_role.id}
                    redis_client.set(orguser_role_key, json.dumps(orguser_role_map_json))

                elif str(orguser.id) not in orguser_role_map_json:
                    orguser_role_map_json[str(orguser.id)] = orguser.new_role.id
                    redis_client.set(orguser_role_key, json.dumps(orguser_role_map_json))

                if role_permissions_key:
                    permissions_map = redis_client.get(role_permissions_key)
                    if permissions_map:
                        try:
                            permissions_json = json.loads(permissions_map)
                        except ValueError:
                            # this should never happen unless the cache has been tampered with
                            permissions_json = None

                if not permissions_json:
                    permissions_json = set_roles_and_permissions_in_redis(
                        redis_client, role_permissions_key
                    )

                orguser_role_id = orguser_role_map_json.get(str(orguser.id))
                request.permissions = permissions_json.get(str(orguser_role_id), [])
                request.orguser = orguser
                request.token = token
                return request

        raise HttpError(401, "Invalid or expired token")


class CustomTokenObtainSerializer(TokenObtainPairSerializer):
    """this is called via the login flow"""

    @classmethod
    def get_token(cls, user):
        token = super().get_token(user)  # This returns a RefreshToken and Not an AccessToken

        role_permissions_key = os.getenv("ROLE_PERMISSIONS_REDIS_KEY", "dalgo_permissions_key")

        redis_client = RedisClient.get_instance()
        role_permissions = redis_client.get(
            role_permissions_key
        )  # { role_id : list[permission_slugs] }

        # we clear the key during deployment
        if not role_permissions:
            set_roles_and_permissions_in_redis(redis_client, role_permissions_key)

        # always refresh this redis key when someone logs in
        # new orgusers might be created for the user
        orguser_role_key = f"orguser_role:{user.id}"

        orguser_role = {}  # { orguser_id : role_id }
        for orguser in OrgUser.objects.filter(user=user):
            orguser_role[orguser.id] = orguser.new_role.id

        redis_client.set(
            orguser_role_key,
            json.dumps(orguser_role),
        )

        # Add custom claims to refresh token (automatically propagates to access token)
        token["orguser_role_key"] = orguser_role_key
        return token

    def validate(self, attrs):
        data = super().validate(attrs)
        return {"access": data["access"], "refresh": data["refresh"]}


class CustomTokenRefreshSerializer(TokenRefreshSerializer):
    """client calls the refresh api to get a new access token"""

    def validate(self, attrs):
        data = super().validate(attrs)
        # Get the user from the refresh token
        refresh = self.token_class(attrs["refresh"])
        user_id = refresh.payload.get("user_id")
        user = User.objects.filter(id=user_id).first()
        if user:
            # Generate a new refresh token with custom claims (which will also add claims to access token)
            refresh_token = CustomTokenObtainSerializer.get_token(user)
            access_token = refresh_token.access_token
            data["access"] = str(access_token)
        return data
