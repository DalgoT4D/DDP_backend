import os
import django
import pytest
from ninja.errors import HttpError
from ddpui.models.org import Org
from ddpui.models.role_based_access import Permission, Role, RolePermission
from ddpui.models.userpreferences import UserPreferences
from ddpui.models.org_user import OrgUser, OrgUserRole
from ddpui import auth
from django.contrib.auth.models import User
from ddpui.tests.api_tests.test_user_org_api import mock_request, seed_db
from ddpui.api.user_preferences_api import (
    create_user_preferences,
    get_user_preferences,
    update_user_preferences,
)
from ddpui.schemas.userpreferences_schema import (
    CreateUserPreferencesSchema,
    UpdateUserPreferencesSchema,
)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

pytestmark = pytest.mark.django_db


@pytest.fixture
def authuser():
    """a django User object"""
    user = User.objects.create(
        username="tempusername", email="tempuseremail", password="tempuserpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org_without_workspace():
    """a pytest fixture which creates an Org without an airbyte workspace"""
    org = Org.objects.create(airbyte_workspace_id=None, slug="test-org-slug")
    yield org
    org.delete()


@pytest.fixture
def orguser(authuser, org_without_workspace):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org_without_workspace,
        role=OrgUserRole.ACCOUNT_MANAGER,
        new_role=Role.objects.filter(slug=auth.ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def user_preferences(orguser):
    """a pytest fixture which creates the user preferences for the OrgUser"""
    return UserPreferences.objects.create(
        orguser=orguser,
        enable_email_notifications=True,
    )


def test_seed_data(seed_db):
    """a test to seed the database"""
    assert Role.objects.count() == 5
    assert RolePermission.objects.count() > 5
    assert Permission.objects.count() > 5


def test_create_user_preferences_success(orguser):
    """tests the success of creating user preferences for the OrgUser"""
    request = mock_request(orguser)
    payload = CreateUserPreferencesSchema(enable_email_notifications=True, disclaimer_shown=True)

    response = create_user_preferences(request, payload)

    # Assertions
    assert response["success"] is True
    preferences = response["res"]
    assert preferences["enable_email_notifications"] is True


def test_create_user_preferences_already_exists(user_preferences):
    """
    tests failure in case the user preferences
    already exists for an OrgUser
    """
    request = mock_request(orguser=user_preferences.orguser)
    payload = CreateUserPreferencesSchema(
        enable_email_notifications=True,
    )

    with pytest.raises(HttpError) as excinfo:
        create_user_preferences(request, payload)

    assert str(excinfo.value) == "Preferences already exist"


def test_update_user_preferences_success(orguser, user_preferences):
    """tests the success of updating user preferences for the OrgUser"""
    request = mock_request(orguser)
    payload = UpdateUserPreferencesSchema(
        enable_email_notifications=False,
    )

    response = update_user_preferences(request, payload)
    assert response["success"] is True
    updated_preferences = UserPreferences.objects.get(orguser=user_preferences.orguser)
    assert updated_preferences.enable_email_notifications is False


def test_update_user_preferences_create_success_if_not_exist(orguser):
    """
    tests the success of updating user preferences
    for the OrgUser with no initial user preferences
    """
    request = mock_request(orguser)
    payload = UpdateUserPreferencesSchema(
        enable_email_notifications=True,
    )

    response = update_user_preferences(request, payload)
    assert response["success"] is True
    user_preferences = UserPreferences.objects.get(orguser=orguser)
    assert user_preferences.enable_email_notifications is True


def test_get_user_preferences_success(orguser, user_preferences):
    """tests the success of fetching user preferences for the OrgUser"""
    request = mock_request(orguser)
    response = get_user_preferences(request)
    assert response["success"] is True
    assert response["res"] == {
        "enable_email_notifications": user_preferences.enable_email_notifications,
        "disclaimer_shown": user_preferences.disclaimer_shown,
        "is_llm_active": False,
    }


def test_get_user_preferences_success_if_not_exist(orguser):
    """
    tests the success of fetching user preferences
    for the OrgUser with no initial user preferences
    """
    request = mock_request(orguser)
    response = get_user_preferences(request)
    assert response["success"] is True
    assert response["res"] == {
        "enable_email_notifications": False,
        "disclaimer_shown": False,
        "is_llm_active": False,
    }
    assert UserPreferences.objects.filter(orguser=orguser).exists()
