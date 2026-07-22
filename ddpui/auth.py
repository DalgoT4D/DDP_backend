import os
import time
import uuid
import json
from functools import wraps
from ninja.security import HttpBearer
from ninja.errors import HttpError

from rest_framework_simplejwt.exceptions import ExpiredTokenError
from rest_framework_simplejwt.tokens import AccessToken, RefreshToken, TokenError
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
ADMIN_ROLE = "admin"
ANALYST_ROLE = "analyst"
MEMBER_ROLE = "member"

# Deprecated aliases. Access Control v2 (migration 0165) collapsed the four customer roles
# into three: account-manager was renamed to `admin`, guest to `member`, and
# pipeline-manager was MERGED into admin (the role no longer exists). These aliases keep
# existing imports resolving to the role each was renamed to. Do not reintroduce a
# PIPELINE_MANAGER_ROLE alias — pipeline-manager was a strictly lower role than admin, so
# aliasing it to "admin" would silently over-grant.
ACCOUNT_MANAGER_ROLE = ADMIN_ROLE
GUEST_ROLE = MEMBER_ROLE


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


# 403/404 noun per rtype for the resource gates below — same wording contract
# as gates.py (the webapp matches on these strings).
_NOUN_BY_RTYPE = {
    "chart": "chart",
    "dashboard": "dashboard",
    "report": "report",
    "alert": "alert",
    "metric": "metric",
    "kpi": "KPI",
}


def extract_resource(rtype: str, param: str = None):
    """② Fetch the org-scoped resource named by the route param (default
    ``{rtype}_id``) and attach it as ``request.resource``. Cross-org is
    indistinguishable from missing: 404. Stack order (top = runs first):
    ``@has_permission`` ①, this ②, ``@has_resource_permission`` ③.

    Sharing imports are deferred to decoration time — this module loads
    before the sharing package (which imports role constants from here)."""
    import inspect

    from ddpui.core.sharing.shareable_types import get_resource_type

    entry = get_resource_type(rtype)
    if entry is None:
        raise ValueError(f"extract_resource: unknown rtype '{rtype}'")
    param = param or f"{rtype}_id"
    noun = _NOUN_BY_RTYPE.get(rtype, rtype)
    not_found = f"{noun[0].upper()}{noun[1:]} not found"

    def decorator(api_endpoint):
        sig = inspect.signature(api_endpoint)

        @wraps(api_endpoint)
        def wrapper(*args, **kwargs):
            request = args[0]
            bound = sig.bind(*args, **kwargs)
            if param not in bound.arguments:
                raise HttpError(404, not_found)
            resource = entry.model.objects.filter(
                pk=bound.arguments[param], org=request.orguser.org
            ).first()
            if resource is None:
                raise HttpError(404, not_found)
            request.resource = resource
            request.resource_rtype = rtype
            return api_endpoint(*args, **kwargs)

        return wrapper

    return decorator


def has_resource_permission(slug: str):
    """③ Deny with 403 unless ``slug`` is in the viewer's resource
    permissions (``access_resolver.get_resource_permissions``) for
    ``request.resource``. Attaches the set as
    ``request.resource_permissions`` for body reads."""
    from ddpui.core.sharing.permission_map import RTYPE_LEVEL_SLUG, slug_for

    if slug not in set(RTYPE_LEVEL_SLUG.values()):
        # Fail at import time, not per request — typos never ship.
        raise ValueError(f"has_resource_permission: unknown resource slug '{slug}'")

    def decorator(api_endpoint):
        @wraps(api_endpoint)
        def wrapper(*args, **kwargs):
            from ddpui.core.sharing.access_resolver import get_resource_permissions

            request = args[0]
            resource = getattr(request, "resource", None)
            rtype = getattr(request, "resource_rtype", None)
            if resource is None or rtype is None:
                raise RuntimeError("has_resource_permission requires @extract_resource above it")
            permissions = get_resource_permissions(request.orguser, rtype, resource)
            if slug not in permissions:
                noun = _NOUN_BY_RTYPE.get(rtype, rtype)
                if slug == slug_for(rtype, "edit"):
                    raise HttpError(403, f"You do not have edit access to this {noun}")
                raise HttpError(403, f"You do not have access to this {noun}")
            request.resource_permissions = permissions
            return api_endpoint(*args, **kwargs)

        return wrapper

    return decorator


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


def blacklist_jti_in_redis(token_str, token_class):
    """Stores a token's JTI in Redis with TTL equal to its remaining lifetime."""
    try:
        token = token_class(token_str)
        jti = token.payload.get("jti")
        exp = token.payload.get("exp")
        if jti and exp:
            ttl = int(exp - time.time())
            if ttl > 0:
                redis_client = RedisClient.get_instance()
                redis_client.set(f"blacklisted_jti:{jti}", "1", ex=ttl)
    except (TokenError, Exception):
        pass


class CustomJwtAuthMiddleware(HttpBearer):
    """the authenticate() function is called on every authenticated request via django middleware"""

    def __call__(self, request):
        cookie_token = request.COOKIES.get("access_token")

        if not cookie_token:
            return super().__call__(request)

        # 498 tells the frontend to refresh; only signal that for genuine expiry.
        # Malformed / bad-signature / wrong-type tokens should force a re-login (401).
        try:
            AccessToken(cookie_token)
        except ExpiredTokenError as err:
            raise HttpError(498, "Token expired") from err
        except TokenError as err:
            raise HttpError(401, "Invalid token") from err

        return self.authenticate(request, cookie_token)

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

        # Check if this token's JTI has been blacklisted (e.g. user logged out)
        jti = token_payload.get("jti")
        if jti:
            redis_client = RedisClient.get_instance()
            if redis_client.get(f"blacklisted_jti:{jti}"):
                raise HttpError(401, "Token has been invalidated")

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

        # Reject if this refresh token was blacklisted in Redis (e.g. user already logged out)
        refresh_jti = refresh.payload.get("jti")
        if refresh_jti:
            redis_client = RedisClient.get_instance()
            if redis_client.get(f"blacklisted_jti:{refresh_jti}"):
                raise TokenError("Refresh token has been invalidated")

        user_id = refresh.payload.get("user_id")
        user = User.objects.filter(id=user_id).first()
        if user:
            # Generate a new refresh token with custom claims (which will also add claims to access token)
            refresh_token = CustomTokenObtainSerializer.get_token(user)
            access_token = refresh_token.access_token
            data["access"] = str(access_token)
        return data
