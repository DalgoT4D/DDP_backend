"""
Tests for AdminJwtAuthMiddleware (features/admin-portal/plan.md M1).

The admin API is guarded by a SEPARATE session: a distinct admin_access_token
cookie whose JWT carries session="admin". A normal login token (which lacks the
claim) must never authenticate an admin request.
"""

from unittest.mock import Mock, patch

import pytest
from django.contrib.auth.models import User
from django.core.management import call_command
from ninja.errors import HttpError
from rest_framework_simplejwt.tokens import AccessToken

from ninja.testing import TestClient

from ddpui.api.admin_api import (
    admin_router,
    post_admin_login,
    post_admin_logout,
    post_admin_token_refresh,
    get_admin_currentuser,
    AdminLoginSchema,
)
from ddpui.auth import AdminJwtAuthMiddleware, CustomJwtAuthMiddleware
from ddpui.core.admin import admin_service
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser, UserAttributes
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE

pytestmark = pytest.mark.django_db


@pytest.fixture(scope="session")
def seed_db(django_db_setup, django_db_blocker):
    """role/permission seed the middleware needs to resolve permissions"""
    with django_db_blocker.unblock():
        call_command("loaddata", "001_roles.json")
        call_command("loaddata", "002_permissions.json")
        call_command("loaddata", "003_role_permissions.json")


def _mock_auth_redis():
    """mock Redis for both the mint and the middleware, as the existing auth tests do"""
    return patch("ddpui.auth.RedisClient.get_instance"), patch(
        "ddpui.auth.set_roles_and_permissions_in_redis", return_value={}
    )


def test_admin_middleware_rejects_token_without_admin_claim():
    """A normal access token (no session='admin') is refused with 401."""
    user = User.objects.create(username="u@x.com", email="u@x.com", password="pw")
    token = str(AccessToken.for_user(user))  # a plain token, no session claim

    with pytest.raises(HttpError) as excinfo:
        AdminJwtAuthMiddleware().authenticate(Mock(), token)

    assert excinfo.value.status_code == 401


def test_admin_middleware_admits_admin_token_and_loads_orguser(seed_db):
    """An admin token (session='admin') authenticates and populates request.orguser."""
    user = User.objects.create_user(
        username="admin@dalgo.org", email="admin@dalgo.org", password="Secret@123"
    )
    org = Org.objects.create(name="ops-org", slug="ops-org")
    orguser = OrgUser.objects.create(
        user=user, org=org, new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first()
    )
    UserAttributes.objects.create(user=user, is_platform_admin=True)

    redis_patch, roles_patch = _mock_auth_redis()
    with redis_patch as mock_redis, roles_patch:
        mock_redis.return_value.get.return_value = None
        token_data, _ = admin_service.issue_admin_session("admin@dalgo.org", "Secret@123")

    request = Mock()
    request.headers = {"x-dalgo-org": org.slug}
    redis_patch2, roles_patch2 = _mock_auth_redis()
    with redis_patch2 as mock_redis2, roles_patch2:
        mock_redis2.return_value.get.return_value = None
        result = AdminJwtAuthMiddleware().authenticate(request, token_data["access"])

    assert result.orguser == orguser


# ---- POST /admin/login/ -------------------------------------------------------


def test_admin_login_refuses_non_platform_admin():
    """Correct password but not a platform admin -> 403, and no cookie is set."""
    User.objects.create_user(
        username="ops@dalgo.org", email="ops@dalgo.org", password="Secret@123"
    )
    with pytest.raises(HttpError) as excinfo:
        post_admin_login(Mock(), AdminLoginSchema(username="ops@dalgo.org", password="Secret@123"))
    assert excinfo.value.status_code == 403


def test_admin_login_wrong_password_is_401():
    """Wrong password -> 401."""
    user = User.objects.create_user(
        username="admin@dalgo.org", email="admin@dalgo.org", password="Secret@123"
    )
    UserAttributes.objects.create(user=user, is_platform_admin=True)
    with pytest.raises(HttpError) as excinfo:
        post_admin_login(Mock(), AdminLoginSchema(username="admin@dalgo.org", password="nope"))
    assert excinfo.value.status_code == 401


def test_admin_login_sets_admin_cookies_for_platform_admin():
    """A platform admin gets admin_access_token + admin_refresh_token cookies."""
    user = User.objects.create_user(
        username="admin@dalgo.org", email="admin@dalgo.org", password="Secret@123"
    )
    UserAttributes.objects.create(user=user, is_platform_admin=True)

    redis_patch, roles_patch = _mock_auth_redis()
    with redis_patch as mock_redis, roles_patch:
        mock_redis.return_value.get.return_value = None
        response = post_admin_login(
            Mock(), AdminLoginSchema(username="admin@dalgo.org", password="Secret@123")
        )

    assert response.status_code == 200
    assert "admin_access_token" in response.cookies
    assert "admin_refresh_token" in response.cookies


# ---- logout / currentuser / refresh -------------------------------------------


def test_admin_logout_clears_admin_cookies():
    """Admin logout deletes only the admin_* cookies (independent of the normal session)."""
    request = Mock()
    request.COOKIES = {}
    response = post_admin_logout(request)
    assert response.status_code == 200
    assert response.cookies["admin_access_token"].value == ""
    assert response.cookies["admin_refresh_token"].value == ""


def test_admin_currentuser_reports_platform_admin(seed_db):
    """currentuser returns the admin's email + is_platform_admin, via the admin session."""
    user = User.objects.create_user(
        username="admin@dalgo.org", email="admin@dalgo.org", password="Secret@123"
    )
    org = Org.objects.create(name="ops-org", slug="ops-org")
    orguser = OrgUser.objects.create(
        user=user, org=org, new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first()
    )
    UserAttributes.objects.create(user=user, is_platform_admin=True)

    request = Mock()
    request.orguser = orguser
    result = get_admin_currentuser(request)
    assert result["is_platform_admin"] is True
    assert result["email"] == "admin@dalgo.org"


def test_admin_token_refresh_without_cookie_is_401():
    request = Mock()
    request.COOKIES = {}
    with pytest.raises(HttpError) as excinfo:
        post_admin_token_refresh(request)
    assert excinfo.value.status_code == 401


def test_admin_token_refresh_issues_new_admin_access():
    """A valid admin refresh token yields a new admin_access_token carrying session='admin'."""
    user = User.objects.create_user(
        username="admin@dalgo.org", email="admin@dalgo.org", password="Secret@123"
    )
    UserAttributes.objects.create(user=user, is_platform_admin=True)

    redis_patch, roles_patch = _mock_auth_redis()
    with redis_patch as mock_redis, roles_patch:
        mock_redis.return_value.get.return_value = None
        token_data, _ = admin_service.issue_admin_session("admin@dalgo.org", "Secret@123")

    request = Mock()
    request.COOKIES = {"admin_refresh_token": token_data["refresh"]}
    # the refresh endpoint reads the blacklist via admin_api's RedisClient
    with patch("ddpui.api.admin_api.RedisClient.get_instance") as mock_redis2:
        mock_redis2.return_value.get.return_value = None
        response = post_admin_token_refresh(request)

    assert "admin_access_token" in response.cookies
    access = AccessToken(response.cookies["admin_access_token"].value)
    assert access["session"] == "admin"


def test_admin_routes_require_admin_session():
    """Router-level auth: an admin route rejects a request carrying no admin session (401).
    A bare normal access_token would land here too — it isn't the admin cookie."""
    client = TestClient(admin_router)
    response = client.get("/ping")
    assert response.status_code == 401


def test_cookie_name_extraction_is_non_behavioral():
    """Regression: the normal middleware still reads the 'access_token' cookie; the admin
    subclass reads a distinct one. Guards the extraction that let the subclass exist."""
    assert CustomJwtAuthMiddleware.cookie_name == "access_token"
    assert AdminJwtAuthMiddleware.cookie_name == "admin_access_token"
