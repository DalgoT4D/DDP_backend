"""
Tests for AdminJwtAuthMiddleware (features/admin-portal/plan.md M1).

The admin API is guarded by a SEPARATE session: a distinct admin_access_token
cookie whose JWT carries session="admin". A normal login token (which lacks the
claim) must never authenticate an admin request.

These are middleware-level unit tests. The tests that exercise the admin_api view
functions (login / logout / refresh / currentuser) live with the other API tests,
in ddpui/tests/api_tests/test_admin_api.py.
"""

from unittest.mock import Mock, patch

import pytest
from django.contrib.auth.models import User
from django.core.management import call_command
from ninja.errors import HttpError
from rest_framework_simplejwt.tokens import AccessToken

from ddpui.auth import AdminJwtAuthMiddleware, CustomJwtAuthMiddleware, ACCOUNT_MANAGER_ROLE
from ddpui.core.admin import admin_service
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser, UserAttributes
from ddpui.models.role_based_access import Role

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


def test_cookie_name_extraction_is_non_behavioral():
    """Regression: the normal middleware still reads the 'access_token' cookie; the admin
    subclass reads a distinct one. Guards the extraction that let the subclass exist."""
    assert CustomJwtAuthMiddleware.cookie_name == "access_token"
    assert AdminJwtAuthMiddleware.cookie_name == "admin_access_token"
