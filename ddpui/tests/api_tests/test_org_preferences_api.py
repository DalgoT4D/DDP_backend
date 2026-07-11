"""Task 11 Part B: `/api/orgpreferences` exposure of the three Resource
Sharing fields (allow_public_sharing/default_general_audience/
default_general_level).

Tests:
1. GET returns the 3 fields (to_json())
2. PUT (sharing settings) by a non-admin is denied
3. PUT by an admin applies the change
4. PUT with an invalid audience/level value -> 400
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from ninja.errors import HttpError

from django.contrib.auth.models import User
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.general_access import GeneralAudience, GeneralLevel
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE, ANALYST_ROLE
from ddpui.api.org_preferences_api import (
    get_org_preferences,
    update_sharing_preferences,
)
from ddpui.schemas.org_preferences_schema import UpdateSharingPreferencesSchema
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="orgprefapiuser", email="orgprefapiuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Org Preferences API Test Org",
        slug="org-pref-api-org",
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


@pytest.fixture
def orguser(authuser, org):
    """An admin-role OrgUser (ACCOUNT_MANAGER_ROLE == admin)."""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def analyst_authuser():
    user = User.objects.create(
        username="orgprefanalyst", email="orgprefanalyst@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def analyst_orguser(analyst_authuser, org):
    """A non-admin (analyst) OrgUser in the same org."""
    orguser = OrgUser.objects.create(
        user=analyst_authuser,
        org=org,
        new_role=Role.objects.filter(slug=ANALYST_ROLE).first(),
    )
    yield orguser
    orguser.delete()


class TestGetOrgPreferencesSharingFields:
    def test_get_returns_the_three_fields_with_no_row(self, orguser, seed_db):
        assert not OrgPreferences.objects.filter(org=orguser.org).exists()
        request = mock_request(orguser)
        response = get_org_preferences(request)

        res = response["res"]
        assert res["allow_public_sharing"] is True
        assert res["default_general_audience"] == GeneralAudience.ALL_USERS
        assert res["default_general_level"] == GeneralLevel.VIEW

    def test_get_returns_the_three_fields_with_row(self, orguser, seed_db):
        OrgPreferences.objects.create(
            org=orguser.org,
            allow_public_sharing=False,
            default_general_audience=GeneralAudience.ADMINS,
            default_general_level=GeneralLevel.EDIT,
        )
        request = mock_request(orguser)
        response = get_org_preferences(request)

        res = response["res"]
        assert res["allow_public_sharing"] is False
        assert res["default_general_audience"] == GeneralAudience.ADMINS
        assert res["default_general_level"] == GeneralLevel.EDIT


class TestUpdateSharingPreferences:
    def test_non_admin_denied(self, analyst_orguser, seed_db):
        request = mock_request(analyst_orguser)
        payload = UpdateSharingPreferencesSchema(allow_public_sharing=False)
        with pytest.raises(HttpError) as exc_info:
            update_sharing_preferences(request, payload)
        assert exc_info.value.status_code == 403

    def test_admin_applies_change(self, orguser, seed_db):
        request = mock_request(orguser)
        payload = UpdateSharingPreferencesSchema(
            allow_public_sharing=False,
            default_general_audience=GeneralAudience.ADMINS,
            default_general_level=GeneralLevel.EDIT,
        )
        response = update_sharing_preferences(request, payload)

        res = response["res"]
        assert res["allow_public_sharing"] is False
        assert res["default_general_audience"] == GeneralAudience.ADMINS
        assert res["default_general_level"] == GeneralLevel.EDIT

        prefs = OrgPreferences.objects.get(org=orguser.org)
        assert prefs.allow_public_sharing is False
        assert prefs.default_general_audience == GeneralAudience.ADMINS
        assert prefs.default_general_level == GeneralLevel.EDIT

    def test_invalid_audience_returns_400(self, orguser, seed_db):
        request = mock_request(orguser)
        payload = UpdateSharingPreferencesSchema(default_general_audience="not-a-real-audience")
        with pytest.raises(HttpError) as exc_info:
            update_sharing_preferences(request, payload)
        assert exc_info.value.status_code == 400

    def test_invalid_level_returns_400(self, orguser, seed_db):
        request = mock_request(orguser)
        payload = UpdateSharingPreferencesSchema(default_general_level="not-a-real-level")
        with pytest.raises(HttpError) as exc_info:
            update_sharing_preferences(request, payload)
        assert exc_info.value.status_code == 400

    def test_creates_preferences_row_when_missing(self, orguser, seed_db):
        assert not OrgPreferences.objects.filter(org=orguser.org).exists()
        request = mock_request(orguser)
        payload = UpdateSharingPreferencesSchema(allow_public_sharing=False)
        update_sharing_preferences(request, payload)

        assert OrgPreferences.objects.filter(org=orguser.org).exists()
