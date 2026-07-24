"""
Tests for AdminJwtAuthMiddleware (features/admin-portal/plan.md M1).

The admin API is guarded by a SEPARATE session: a distinct admin_access_token
cookie whose JWT carries session="admin". A normal login token (which lacks the
claim) must never authenticate an admin request.

These are middleware-level unit tests. The tests that exercise the admin_api view
functions (login / logout / refresh / currentuser) live with the other API tests,
in ddpui/tests/api_tests/test_admin_api.py.
"""

import os
from datetime import timedelta
from unittest.mock import Mock, patch

import django
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from rest_framework_simplejwt.tokens import AccessToken

from ddpui.auth import AdminJwtAuthMiddleware, CustomJwtAuthMiddleware, ACCOUNT_MANAGER_ROLE
from ddpui.core.admin import admin_service
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser, UserAttributes
from ddpui.models.role_based_access import Role
from ddpui.schemas.admin_schema import AdminLoginSchema
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


def _mock_auth_redis():
    """mock Redis for both the mint and the middleware — matches the inline patch pattern
    test_auth.py uses at each call site — no shared helper exists for this pair"""
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
        token_data = admin_service.issue_admin_session(
            AdminLoginSchema(username="admin@dalgo.org", password="Secret@123")
        )

    request = Mock()
    request.headers = {"x-dalgo-org": org.slug}
    redis_patch2, roles_patch2 = _mock_auth_redis()
    with redis_patch2 as mock_redis2, roles_patch2:
        mock_redis2.return_value.get.return_value = None
        result = AdminJwtAuthMiddleware().authenticate(request, token_data["access"])

    assert result.orguser == orguser


def test_cookie_name_extraction_is_non_behavioral():
    """Regression: the normal middleware still reads the 'access_token' cookie; the admin
    subclass reads a distinct one. Guards the extraction that let the subclass exist."""
    assert CustomJwtAuthMiddleware.cookie_name == "access_token"
    assert AdminJwtAuthMiddleware.cookie_name == "admin_access_token"


def test_admin_middleware_ignores_the_normal_session_cookie():
    """
    SESSION INDEPENDENCE (behavioral): holding a perfectly valid NORMAL session is not
    enough to reach the admin API. The admin middleware reads only admin_access_token,
    so an `access_token` cookie is never even looked at — the request goes unauthenticated
    and ninja answers 401. The sibling assertion above only checks the attribute; this
    checks the behaviour it is supposed to produce.
    """
    user = User.objects.create(username="normal@x.com", email="normal@x.com", password="pw")
    request = Mock()
    request.COOKIES = {"access_token": str(AccessToken.for_user(user))}
    request.headers = {}

    assert AdminJwtAuthMiddleware()(request) is None


def test_admin_middleware_maps_malformed_admin_cookie_to_401():
    """A garbage admin_access_token forces a re-login (401), not a refresh (498)."""
    request = Mock()
    request.COOKIES = {"admin_access_token": "not-a-jwt"}
    request.headers = {}

    with pytest.raises(HttpError) as excinfo:
        AdminJwtAuthMiddleware()(request)

    assert excinfo.value.status_code == 401


def test_admin_middleware_maps_expired_admin_cookie_to_498():
    """
    An EXPIRED admin token gets 498, which is the frontend's signal to call
    /token/refresh — distinct from the 401 a malformed token gets. Without this the
    admin portal would bounce the user to login every 15 minutes instead of refreshing.
    """
    user = User.objects.create(username="expired@x.com", email="expired@x.com", password="pw")
    token = AccessToken.for_user(user)
    token.set_exp(lifetime=timedelta(seconds=-10))  # already expired

    request = Mock()
    request.COOKIES = {"admin_access_token": str(token)}
    request.headers = {}

    with pytest.raises(HttpError) as excinfo:
        AdminJwtAuthMiddleware()(request)

    assert excinfo.value.status_code == 498
