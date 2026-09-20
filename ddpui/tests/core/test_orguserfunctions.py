"""
Tests for ddpui.core.orguserfunctions

Focuses on the username/email sync behaviour: users are created with
username == email, so any email update must also update username to keep
email-based authentication working.
"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from django.contrib.auth.models import User
from django.core.management import call_command

from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core import orguserfunctions
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser, OrgUserUpdate, OrgUserUpdatev1
from ddpui.models.role_based_access import Role

pytestmark = pytest.mark.django_db


# ---------------------------------------------------------------------------
# Session-scoped seed fixture (roles / permissions)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def seed_db(django_db_setup, django_db_blocker):
    with django_db_blocker.unblock():
        call_command("loaddata", "001_roles.json")
        call_command("loaddata", "002_permissions.json")
        call_command("loaddata", "003_role_permissions.json")


# ---------------------------------------------------------------------------
# Per-test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def test_org(seed_db):
    org = Org.objects.create(name="test-ouf-org", slug="test-ouf-org")
    yield org
    org.delete()


@pytest.fixture
def test_user():
    """Django User created the same way the signup flow does it: username == email."""
    email = "original@example.com"
    user = User.objects.create_user(username=email, email=email, password="secret")
    yield user
    user.delete()


@pytest.fixture
def test_orguser(test_user, test_org, seed_db):
    role = Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first()
    orguser = OrgUser.objects.create(user=test_user, org=test_org, new_role=role)
    yield orguser
    orguser.delete()


# ---------------------------------------------------------------------------
# update_orguser (legacy v0) — username/email sync
# ---------------------------------------------------------------------------


class TestUpdateOrguser:
    def test_email_update_also_updates_username(self, test_orguser):
        """Changing the email via update_orguser must keep username in sync."""
        new_email = "updated@example.com"
        payload = OrgUserUpdate(email=new_email)

        orguserfunctions.update_orguser(test_orguser, payload)

        # Refresh from DB to verify persistence
        test_orguser.user.refresh_from_db()
        assert test_orguser.user.email == new_email
        assert test_orguser.user.username == new_email, (
            "User.username must be updated together with User.email so that "
            "email-based login continues to work."
        )

    def test_email_update_lowercases_and_strips(self, test_orguser):
        """Email normalisation (lowercase + strip) is applied to username too."""
        payload = OrgUserUpdate(email="  UPPER@Example.COM  ")

        orguserfunctions.update_orguser(test_orguser, payload)

        test_orguser.user.refresh_from_db()
        assert test_orguser.user.email == "upper@example.com"
        assert test_orguser.user.username == "upper@example.com"

    def test_no_email_in_payload_leaves_username_unchanged(self, test_orguser):
        """When payload.email is None/empty, username must not be touched."""
        original_username = test_orguser.user.username
        payload = OrgUserUpdate(email=None)

        orguserfunctions.update_orguser(test_orguser, payload)

        test_orguser.user.refresh_from_db()
        assert test_orguser.user.username == original_username


# ---------------------------------------------------------------------------
# update_orguser_v1 — username/email sync
# ---------------------------------------------------------------------------


class TestUpdateOrgUserV1:
    def test_email_update_also_updates_username(self, test_orguser):
        """Changing the email via update_orguser_v1 must keep username in sync."""
        new_email = "v1updated@example.com"
        payload = OrgUserUpdatev1(email=new_email)

        orguserfunctions.update_orguser_v1(test_orguser, payload)

        test_orguser.user.refresh_from_db()
        assert test_orguser.user.email == new_email
        assert test_orguser.user.username == new_email, (
            "User.username must be updated together with User.email so that "
            "email-based login continues to work."
        )

    def test_email_update_lowercases_and_strips(self, test_orguser):
        """Email normalisation (lowercase + strip) is applied to username too."""
        payload = OrgUserUpdatev1(email="  V1UPPER@Example.COM  ")

        orguserfunctions.update_orguser_v1(test_orguser, payload)

        test_orguser.user.refresh_from_db()
        assert test_orguser.user.email == "v1upper@example.com"
        assert test_orguser.user.username == "v1upper@example.com"

    def test_no_email_in_payload_leaves_username_unchanged(self, test_orguser):
        """When payload.email is None/empty, username must not be touched."""
        original_username = test_orguser.user.username
        payload = OrgUserUpdatev1(email=None)

        orguserfunctions.update_orguser_v1(test_orguser, payload)

        test_orguser.user.refresh_from_db()
        assert test_orguser.user.username == original_username

    def test_username_and_email_remain_consistent_after_multiple_updates(self, test_orguser):
        """Repeated email changes must keep username == email at every step."""
        for new_email in ["first@example.com", "second@example.com", "third@example.com"]:
            payload = OrgUserUpdatev1(email=new_email)
            orguserfunctions.update_orguser_v1(test_orguser, payload)
            test_orguser.user.refresh_from_db()
            assert test_orguser.user.username == test_orguser.user.email == new_email
