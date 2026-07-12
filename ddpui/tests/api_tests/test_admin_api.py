"""
Tests for the Admin Portal API and its platform-admin gate.

Milestone 1 acceptance (features/admin-portal/v1/plan.md §6, §7):
  - non-platform-admin -> 403 on the guarded /admin/ping route
  - platform admin      -> 200 on the same route
  - /currentuserv2 surfaces is_platform_admin
"""

import pytest
from unittest.mock import Mock
from django.core.management import call_command
from django.contrib.auth.models import User
from ninja.errors import HttpError

from ddpui.api.admin_api import get_admin_ping
from ddpui.api.user_org_api import get_current_user_v2
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser, UserAttributes
from ddpui.models.role_based_access import Role, RolePermission
from ddpui.auth import ACCOUNT_MANAGER_ROLE

pytestmark = pytest.mark.django_db


@pytest.fixture(scope="session")
def seed_db(django_db_setup, django_db_blocker):
    """load the role/permission seed data the guard and currentuserv2 need"""
    with django_db_blocker.unblock():
        call_command("loaddata", "001_roles.json")
        call_command("loaddata", "002_permissions.json")
        call_command("loaddata", "003_role_permissions.json")


@pytest.fixture
def org():
    """an Org to hang OrgUsers off of"""
    org = Org.objects.create(name="admin-test-org", slug="admin-test-org")
    yield org
    org.delete()


@pytest.fixture
def authuser():
    """a django User"""
    user = User.objects.create(
        username="admin-test-user", email="admin-test-user@example.com", password="pw"
    )
    yield user
    user.delete()


@pytest.fixture
def orguser(authuser, org, seed_db):
    """an OrgUser with the account-manager role (which has can_view_orgusers)"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


def mock_request(orguser: OrgUser = None):
    """mirror the mock_request helper in test_user_org_api.py"""
    request = Mock()
    request.orguser = orguser
    request.permissions = []
    if orguser and orguser.new_role:
        permission_slugs = RolePermission.objects.filter(role=orguser.new_role).values_list(
            "permission__slug", flat=True
        )
        request.permissions = list(permission_slugs)
    return request


# ---- the guard: /admin/ping 403 vs 200 ----------------------------------------


def test_admin_ping_forbidden_for_non_platform_admin(orguser):
    """a user without is_platform_admin is refused with 403"""
    request = mock_request(orguser)
    # no UserAttributes row at all -> not a platform admin
    with pytest.raises(HttpError) as excinfo:
        get_admin_ping(request)
    assert excinfo.value.status_code == 403
    assert str(excinfo.value) == "platform admin access required"


def test_admin_ping_forbidden_when_flag_false(orguser):
    """a user whose is_platform_admin is explicitly False is refused with 403"""
    UserAttributes.objects.create(user=orguser.user, is_platform_admin=False)
    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        get_admin_ping(request)
    assert excinfo.value.status_code == 403


def test_admin_ping_ok_for_platform_admin(orguser):
    """a platform admin gets 200 (the stub payload)"""
    UserAttributes.objects.create(user=orguser.user, is_platform_admin=True)
    request = mock_request(orguser)
    response = get_admin_ping(request)
    assert response == {"detail": "pong"}


# ---- /currentuserv2 surfaces is_platform_admin --------------------------------


def test_currentuserv2_reports_platform_admin_true(orguser):
    """currentuserv2 returns is_platform_admin: true for a platform admin"""
    UserAttributes.objects.create(user=orguser.user, is_platform_admin=True)
    request = mock_request(orguser)
    response = get_current_user_v2(request)
    assert len(response) == 1
    assert response[0].is_platform_admin is True


def test_currentuserv2_reports_platform_admin_false(orguser):
    """currentuserv2 defaults is_platform_admin to false for a normal user"""
    request = mock_request(orguser)
    response = get_current_user_v2(request)
    assert len(response) == 1
    assert response[0].is_platform_admin is False
