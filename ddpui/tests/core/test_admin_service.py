"""
Tests for the admin-session service (features/admin-portal/plan.md M1).

issue_admin_session verifies BOTH valid credentials AND is_platform_admin=True
before minting a distinct admin token carrying a session="admin" claim.
"""

from unittest.mock import patch

import pytest
from django.contrib.auth.models import User
from rest_framework_simplejwt.tokens import AccessToken, RefreshToken

from ddpui.core.admin import admin_service
from ddpui.models.org_user import UserAttributes

pytestmark = pytest.mark.django_db


def _mock_redis():
    """get_token touches Redis; mock it the way the existing auth tests do."""
    return patch("ddpui.auth.RedisClient.get_instance"), patch(
        "ddpui.auth.set_roles_and_permissions_in_redis", return_value={}
    )


def test_issue_admin_session_refuses_non_platform_admin():
    """Correct credentials but not a platform admin -> no session is issued."""
    User.objects.create_user(username="ops@dalgo.org", email="ops@dalgo.org", password="Secret@123")
    # no UserAttributes row -> is_platform_admin is effectively False

    token_data, error = admin_service.issue_admin_session("ops@dalgo.org", "Secret@123")

    assert token_data is None
    assert error is not None


def test_issue_admin_session_refuses_wrong_password():
    """Wrong password -> no session, regardless of platform-admin status."""
    user = User.objects.create_user(
        username="admin@dalgo.org", email="admin@dalgo.org", password="Secret@123"
    )
    UserAttributes.objects.create(user=user, is_platform_admin=True)

    token_data, error = admin_service.issue_admin_session("admin@dalgo.org", "wrong-password")

    assert token_data is None
    assert error is not None


def test_issue_admin_session_mints_admin_token_for_platform_admin():
    """A platform admin gets access+refresh tokens carrying a session='admin' claim."""
    user = User.objects.create_user(
        username="admin@dalgo.org", email="admin@dalgo.org", password="Secret@123"
    )
    UserAttributes.objects.create(user=user, is_platform_admin=True)

    redis_patch, roles_patch = _mock_redis()
    with redis_patch as mock_redis, roles_patch:
        mock_redis.return_value.get.return_value = None
        token_data, error = admin_service.issue_admin_session("admin@dalgo.org", "Secret@123")

    assert error is None
    assert "access" in token_data and "refresh" in token_data
    access = AccessToken(token_data["access"])
    assert access["session"] == "admin"


def test_issue_admin_session_uses_short_admin_lifetimes():
    """Admin tokens are shorter-lived than the normal app: 15 min access, 8 h refresh."""
    user = User.objects.create_user(
        username="admin@dalgo.org", email="admin@dalgo.org", password="Secret@123"
    )
    UserAttributes.objects.create(user=user, is_platform_admin=True)

    redis_patch, roles_patch = _mock_redis()
    with redis_patch as mock_redis, roles_patch:
        mock_redis.return_value.get.return_value = None
        token_data, _ = admin_service.issue_admin_session("admin@dalgo.org", "Secret@123")

    access = AccessToken(token_data["access"])
    refresh = RefreshToken(token_data["refresh"])
    assert access.payload["exp"] - access.payload["iat"] == 15 * 60
    assert refresh.payload["exp"] - refresh.payload["iat"] == 8 * 60 * 60
