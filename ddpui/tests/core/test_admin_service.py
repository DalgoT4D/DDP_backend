"""
Tests for the admin-session service (features/admin-portal/plan.md M1).

issue_admin_session verifies BOTH valid credentials AND is_platform_admin=True
before minting a distinct admin token carrying a session="admin" claim.
"""

import os
from unittest.mock import patch
from uuid import uuid4

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from django.utils import timezone
from rest_framework_simplejwt.tokens import AccessToken, RefreshToken

from ddpui.core.admin import admin_service
from ddpui.core.admin.exceptions import (
    AdminInvalidCredentialsError,
    AdminNotPlatformAdminError,
    AdminSessionError,
)
from ddpui.models.org import Org
from ddpui.models.org_plans import OrgPlans
from ddpui.models.org_user import Invitation, OrgUser, UserAttributes, LoginPayload
from ddpui.schemas.admin_schema import AdminUpdateOrgSchema

pytestmark = pytest.mark.django_db


def _mock_redis():
    """get_token touches Redis; mock it — matches the inline patch pattern test_auth.py
    uses at each call site — no shared helper exists for this pair."""
    return patch("ddpui.auth.RedisClient.get_instance"), patch(
        "ddpui.auth.set_roles_and_permissions_in_redis", return_value={}
    )


def test_issue_admin_session_refuses_non_platform_admin():
    """Correct credentials but not a platform admin -> AdminNotPlatformAdminError (API 403)."""
    User.objects.create_user(username="ops@dalgo.org", email="ops@dalgo.org", password="Secret@123")
    # no UserAttributes row -> is_platform_admin is effectively False

    with pytest.raises(AdminNotPlatformAdminError):
        admin_service.issue_admin_session(
            LoginPayload(username="ops@dalgo.org", password="Secret@123")
        )


def test_issue_admin_session_refuses_wrong_password():
    """Wrong password -> AdminInvalidCredentialsError (API 401), regardless of admin status."""
    user = User.objects.create_user(
        username="admin@dalgo.org", email="admin@dalgo.org", password="Secret@123"
    )
    UserAttributes.objects.create(user=user, is_platform_admin=True)

    with pytest.raises(AdminInvalidCredentialsError):
        admin_service.issue_admin_session(
            LoginPayload(username="admin@dalgo.org", password="wrong-password")
        )


def test_issue_admin_session_mints_admin_token_for_platform_admin():
    """A platform admin gets access+refresh tokens carrying a session='admin' claim."""
    user = User.objects.create_user(
        username="admin@dalgo.org", email="admin@dalgo.org", password="Secret@123"
    )
    UserAttributes.objects.create(user=user, is_platform_admin=True)

    redis_patch, roles_patch = _mock_redis()
    with redis_patch as mock_redis, roles_patch:
        mock_redis.return_value.get.return_value = None
        token_data = admin_service.issue_admin_session(
            LoginPayload(username="admin@dalgo.org", password="Secret@123")
        )

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
        token_data = admin_service.issue_admin_session(
            LoginPayload(username="admin@dalgo.org", password="Secret@123")
        )

    access = AccessToken(token_data["access"])
    refresh = RefreshToken(token_data["refresh"])
    assert access.payload["exp"] - access.payload["iat"] == 15 * 60
    assert refresh.payload["exp"] - refresh.payload["iat"] == 8 * 60 * 60


# --------------------------------------------------------------------------- #
# refresh_admin_session
# --------------------------------------------------------------------------- #
# The API-level test only covers the happy path with Redis mocked clean. These
# cover the three refusal paths, which are what stop a normal session being
# escalated into an admin one.


def test_refresh_admin_session_rejects_normal_refresh_token():
    """
    A normal login refresh token lacks session="admin", so it can never be upgraded
    into an admin session. Refused before Redis is even consulted.
    """
    user = User.objects.create_user(
        username="user@dalgo.org", email="user@dalgo.org", password="Secret@123"
    )
    normal_refresh = str(RefreshToken.for_user(user))

    with pytest.raises(AdminSessionError) as excinfo:
        admin_service.refresh_admin_session(normal_refresh)
    assert str(excinfo.value) == "not an admin session"


def test_refresh_admin_session_rejects_blacklisted_token():
    """
    Logout blacklists the refresh token's JTI in Redis. A refresh presented after
    logout is refused, so signing out really ends the session.
    """
    user = User.objects.create_user(
        username="admin@dalgo.org", email="admin@dalgo.org", password="Secret@123"
    )
    UserAttributes.objects.create(user=user, is_platform_admin=True)

    redis_patch, roles_patch = _mock_redis()
    with redis_patch as mock_redis, roles_patch:
        mock_redis.return_value.get.return_value = None
        token_data = admin_service.issue_admin_session(
            LoginPayload(username="admin@dalgo.org", password="Secret@123")
        )

    # the blacklist lookup lives in the service, so patch it at its import site
    with patch("ddpui.core.admin.admin_service.RedisClient.get_instance") as mock_redis2:
        mock_redis2.return_value.get.return_value = b"1"  # this JTI is blacklisted
        with pytest.raises(AdminSessionError) as excinfo:
            admin_service.refresh_admin_session(token_data["refresh"])

    assert str(excinfo.value) == "Refresh token has been invalidated"
    jti = RefreshToken(token_data["refresh"]).payload["jti"]
    mock_redis2.return_value.get.assert_called_once_with(f"blacklisted_jti:{jti}")


def test_refresh_admin_session_rejects_unreadable_token():
    """Garbage in the cookie -> AdminSessionError, which the caller maps to a 401."""
    with pytest.raises(AdminSessionError) as excinfo:
        admin_service.refresh_admin_session("not-a-jwt")
    assert str(excinfo.value) == "Invalid token"


def test_refresh_admin_session_keeps_admin_claim_and_short_lifetime():
    """
    The refreshed access token is still an admin token (session="admin") and still
    gets the SHORT 15-minute admin lifetime — refreshing must not quietly widen the
    session to the normal app's 30 minutes.
    """
    user = User.objects.create_user(
        username="admin@dalgo.org", email="admin@dalgo.org", password="Secret@123"
    )
    UserAttributes.objects.create(user=user, is_platform_admin=True)

    redis_patch, roles_patch = _mock_redis()
    with redis_patch as mock_redis, roles_patch:
        mock_redis.return_value.get.return_value = None
        token_data = admin_service.issue_admin_session(
            LoginPayload(username="admin@dalgo.org", password="Secret@123")
        )

    with patch("ddpui.core.admin.admin_service.RedisClient.get_instance") as mock_redis2:
        mock_redis2.return_value.get.return_value = None  # not blacklisted
        refreshed = admin_service.refresh_admin_session(token_data["refresh"])

    assert set(refreshed.keys()) == {"access"}  # a refresh does NOT re-issue a refresh token
    access = AccessToken(refreshed["access"])
    assert access["session"] == "admin"
    assert access.payload["exp"] - access.payload["iat"] == 15 * 60


# --------------------------------------------------------------------------- #
# update_org
# --------------------------------------------------------------------------- #


def test_update_org_leaves_omitted_fields_untouched():
    """
    Partial update: a field passed as None is NOT cleared, it is left alone. The API
    sends None for every field the admin did not edit, so treating None as "set to null"
    would wipe viz_url every time someone renamed an org.
    """
    org = Org.objects.create(name="Before", slug="before-slug", viz_url="https://viz.example.com/")

    admin_service.update_org(org, AdminUpdateOrgSchema(name="After"))

    org.refresh_from_db()
    assert org.name == "After"
    assert org.viz_url == "https://viz.example.com/"  # NOT cleared
    assert org.slug == "before-slug"  # slug is never touched


def test_update_org_without_org_plans_row_does_not_raise():
    """
    base_plan lives on OrgPlans, and an org can exist without one (nothing guarantees the
    row). Setting a plan on such an org is a no-op rather than a crash.
    """
    org = Org.objects.create(name="Planless", slug="planless")
    assert not OrgPlans.objects.filter(org=org).exists()

    result = admin_service.update_org(org, AdminUpdateOrgSchema(base_plan="Dalgo"))

    assert result.id == org.id
    assert not OrgPlans.objects.filter(org=org).exists()  # still none — silently skipped


# --------------------------------------------------------------------------- #
# invitation lookup + scoping
# --------------------------------------------------------------------------- #


def _make_invitation(org, email, inviter_org=None):
    """an Invitation into `org`, sent by someone who may belong to a different org"""
    inviter_user = User.objects.create(username=f"inviter-{email}", email=f"inviter-{email}")
    inviter = OrgUser.objects.create(user=inviter_user, org=inviter_org or org)
    return Invitation.objects.create(
        invited_email=email,
        invited_by=inviter,
        invited_in_org=org,
        invited_on=timezone.now(),
        invite_code=str(uuid4()),
    )


def test_get_pending_invitation_normalizes_the_email():
    """
    Invitation emails are matched case-insensitively and whitespace-trimmed, so the
    lookup after an invite finds the row the invite just wrote regardless of how the
    admin typed the address.
    """
    org = Org.objects.create(name="Akshara", slug="akshara-norm")
    invitation = _make_invitation(org, "Priya@Akshara.ORG")

    found = admin_service.get_pending_invitation(org, "  priya@akshara.org  ")

    assert found is not None
    assert found.id == invitation.id


def test_list_org_invitations_is_scoped_by_target_org():
    """
    The Users tab lists invitations by TARGET org (invited_in_org), not by the inviter's
    org. So a cross-org invite a platform admin sent into Akshara shows on Akshara's tab,
    and Bhumi's invites never leak into it.
    """
    akshara = Org.objects.create(name="Akshara", slug="akshara-scope")
    bhumi = Org.objects.create(name="Bhumi", slug="bhumi-scope")
    # the inviter belongs to Bhumi but the invite targets Akshara — the cross-org case
    _make_invitation(akshara, "into-akshara@x.org", inviter_org=bhumi)
    _make_invitation(bhumi, "into-bhumi@x.org", inviter_org=bhumi)

    emails = {inv.invited_email for inv in admin_service.list_org_invitations(akshara)}

    assert emails == {"into-akshara@x.org"}  # Bhumi's invite is not listed here


def test_removal_impact_is_zero_for_a_user_with_no_content():
    """
    A user who created nothing orphans nothing. The confirm dialog reads these counts,
    so zeros are what let it skip the warning instead of showing an empty one.
    """
    org = Org.objects.create(name="Empty", slug="empty-org")
    user = User.objects.create(username="nobody@x.org", email="nobody@x.org")
    orguser = OrgUser.objects.create(user=user, org=org)

    assert admin_service.removal_impact(orguser) == (0, 0, 0)
