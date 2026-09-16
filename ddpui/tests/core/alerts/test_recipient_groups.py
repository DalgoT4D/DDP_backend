"""Tests for user_group recipient type — validation in AlertService."""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from django.contrib.auth.models import User

from ddpui.core.alerts.alert_service import _validate_recipients
from ddpui.core.alerts.exceptions import AlertValidationError
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser, OrgUserGroup
from ddpui.models.role_based_access import Role
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


@pytest.fixture
def org():
    o = Org.objects.create(name="RecipTest Org", slug="recip-test", airbyte_workspace_id="ws-r")
    yield o
    o.delete()


@pytest.fixture
def other_org():
    o = Org.objects.create(name="Other Org", slug="other-org", airbyte_workspace_id="ws-o")
    yield o
    o.delete()


@pytest.fixture
def orguser(org):
    user = User.objects.create(username="recipuser", email="recip@test.com")
    role = Role.objects.filter(slug="analyst").first()
    ou = OrgUser.objects.create(user=user, org=org, new_role=role)
    yield ou
    ou.delete()
    user.delete()


@pytest.fixture
def group(org, orguser):
    g = OrgUserGroup.objects.create(name="Test Group", org=org, created_by=orguser)
    yield g
    g.delete()


@pytest.fixture
def other_group(other_org, orguser):
    g = OrgUserGroup.objects.create(name="Other Group", org=other_org, created_by=orguser)
    yield g
    g.delete()


# ── user_group type ────────────────────────────────────────────────────────


def test_validate_user_group_recipient_valid(seed_db, org, group):
    """A user_group entry with a valid group ID in the same org passes."""
    _validate_recipients([{"type": "user_group", "user_group_id": group.id}], org)


def test_validate_user_group_recipient_wrong_org(seed_db, org, other_group):
    """A group from another org raises AlertValidationError."""
    with pytest.raises(AlertValidationError, match="not in this org"):
        _validate_recipients([{"type": "user_group", "user_group_id": other_group.id}], org)


def test_validate_user_group_recipient_missing_id(seed_db, org):
    """A user_group entry with no user_group_id raises AlertValidationError."""
    with pytest.raises(AlertValidationError, match="user_group_id is required"):
        _validate_recipients([{"type": "user_group", "user_group_id": None}], org)


def test_validate_unknown_type_rejected(seed_db, org):
    """An unknown type raises AlertValidationError."""
    with pytest.raises(AlertValidationError, match="type must be"):
        _validate_recipients([{"type": "unknown_type"}], org)


# ── backward compat ────────────────────────────────────────────────────────


def test_existing_orguser_still_valid(seed_db, org, orguser):
    """orguser type still passes — backward compatibility."""
    _validate_recipients([{"type": "orguser", "orguser_id": orguser.id}], org)


def test_existing_external_still_valid(seed_db, org):
    """external type still passes — backward compatibility."""
    _validate_recipients([{"type": "external", "email": "ext@example.com"}], org)


def test_mixed_recipient_types_valid(seed_db, org, orguser, group):
    """All three types together in one list pass validation."""
    _validate_recipients(
        [
            {"type": "orguser", "orguser_id": orguser.id},
            {"type": "user_group", "user_group_id": group.id},
            {"type": "external", "email": "ext@example.com"},
        ],
        org,
    )
