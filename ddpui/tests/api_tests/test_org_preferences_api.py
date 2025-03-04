from unittest.mock import Mock, patch

import pytest
from ninja.errors import HttpError

from django.contrib.auth.models import User
from ddpui.api.org_preferences_api import create_org_preferences, update_org_preferences
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_user import OrgUser
from ddpui.models.userpreferences import UserPreferences
from ddpui.models.org_user import OrgUserRole
from ddpui.models.role_based_access import Role
from ddpui.models.org import Org
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.schemas.org_preferences_schema import (
    CreateOrgPreferencesSchema,
    UpdateLLMOptinSchema,
    UpdateDiscordNotificationsSchema,
)
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


@pytest.fixture()
def org_fixture():
    """org without dbt workspace"""
    org_slug = "test-org-slug"

    org = Org.objects.create(
        airbyte_workspace_id="FAKE-WORKSPACE-ID-1",
        slug=org_slug,
        name=org_slug,
    )
    yield org
    org.delete()


@pytest.fixture
def orguser_fixture(org_fixture):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser = OrgUser.objects.create(
        user=User.objects.create(
            username="tempusername", email="tempuseremail", password="tempuserpassword"
        ),
        org=org_fixture,
        role=OrgUserRole.ACCOUNT_MANAGER,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()
    orguser.user.delete()


def test_create_org_preferences_exists(orguser_fixture):
    """tests create_org_preferences"""
    request = Mock()
    request.orguser = orguser_fixture
    OrgPreferences.objects.create(org=orguser_fixture.org)
    with pytest.raises(HttpError) as excinfo:
        create_org_preferences(
            request,
            CreateOrgPreferencesSchema(org=orguser_fixture.org.id),
        )
    assert str(excinfo.value) == "Organization preferences already exist"


def test_create_org_preferences_create(orguser_fixture):
    """tests create_org_preferences"""
    request = Mock()
    request.orguser = orguser_fixture
    retval = create_org_preferences(
        request,
        CreateOrgPreferencesSchema(
            org=orguser_fixture.org.id,
            llm_optin=True,
            llm_optin_date="2021-01-01T00:00:00+00:00",
            enable_discord_notifications=True,
            discord_webhook="http://example.com",
        ),
    )
    assert OrgPreferences.objects.filter(org=orguser_fixture.org).exists()
    assert retval["success"] is True
    assert retval["res"]["llm_optin"] is True
    assert retval["res"]["llm_optin_date"] == "2021-01-01T00:00:00+00:00"
    assert retval["res"]["enable_discord_notifications"] is True
    assert retval["res"]["discord_webhook"] == "http://example.com"


@patch("ddpui.api.org_preferences_api.create_notification")
def test_update_org_preferences_llm_optin(mock_create_notification: Mock, orguser_fixture):
    """tests update_org_preferences"""
    request = mock_request(orguser_fixture)

    mock_create_notification.return_value = (None, None)

    retval = update_org_preferences(request, UpdateLLMOptinSchema(llm_optin=True))
    assert retval["success"] is True
    assert retval["res"]["llm_optin"] is True
    org_preferences = OrgPreferences.objects.filter(org=orguser_fixture.org).first()
    assert org_preferences.llm_optin is True
    assert org_preferences.llm_optin_approved_by == orguser_fixture
    assert org_preferences.enable_llm_request is False
    assert org_preferences.enable_llm_requested_by is None

    user_preferences = UserPreferences.objects.filter(orguser=orguser_fixture).first()
    assert user_preferences.disclaimer_shown is True


@patch("ddpui.api.org_preferences_api.create_notification")
def test_update_org_preferences_llm_optout(mock_create_notification: Mock, orguser_fixture):
    """tests update_org_preferences"""
    request = mock_request(orguser_fixture)

    mock_create_notification.return_value = (None, None)

    retval = update_org_preferences(request, UpdateLLMOptinSchema(llm_optin=False))
    assert retval["success"] is True
    assert retval["res"]["llm_optin"] is False
    org_preferences = OrgPreferences.objects.filter(org=orguser_fixture.org).first()
    assert org_preferences.llm_optin is False
    assert org_preferences.llm_optin_approved_by is None
    assert org_preferences.llm_optin_date is None
