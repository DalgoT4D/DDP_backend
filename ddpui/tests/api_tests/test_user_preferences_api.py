import os
import django
import pytest
from ninja.errors import HttpError
from ddpui.models.org import Org
from ddpui.models.role_based_access import Permission, Role, RolePermission
from ddpui.models.userpreferences import UserPreferences
from ddpui.models.org_user import OrgUser
from ddpui import auth
from django.contrib.auth.models import User
from ddpui.tests.api_tests.test_user_org_api import mock_request, seed_db
from ddpui.api.user_preferences_api import (
    create_user_preferences,
    get_user_preferences,
    update_user_preferences,
    update_trial_walkthrough,
)
from ddpui.schemas.userpreferences_schema import (
    CreateUserPreferencesSchema,
    UpdateUserPreferencesSchema,
    UpdateTrialWalkthroughSchema,
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
    assert Role.objects.count() == 4
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
        "last_visited_transform_tab": user_preferences.last_visited_transform_tab,
        "is_llm_active": False,
        "enable_llm_requested": False,
        "trial_walkthrough": {},
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
        "last_visited_transform_tab": None,
        "is_llm_active": False,
        "enable_llm_requested": False,
        "trial_walkthrough": {},
    }
    assert UserPreferences.objects.filter(orguser=orguser).exists()


def test_create_user_preferences_with_transform_tab(orguser):
    """tests creating user preferences with last_visited_transform_tab"""
    request = mock_request(orguser)
    payload = CreateUserPreferencesSchema(
        enable_email_notifications=True, disclaimer_shown=True, last_visited_transform_tab="ui"
    )

    response = create_user_preferences(request, payload)

    # Assertions
    assert response["success"] is True
    preferences = response["res"]
    assert preferences["last_visited_transform_tab"] == "ui"


def test_update_transform_tab_preference(orguser, user_preferences):
    """tests updating last_visited_transform_tab preference"""
    request = mock_request(orguser)

    # Update to 'github'
    payload = UpdateUserPreferencesSchema(last_visited_transform_tab="github")
    response = update_user_preferences(request, payload)
    assert response["success"] is True
    updated_preferences = UserPreferences.objects.get(orguser=user_preferences.orguser)
    assert updated_preferences.last_visited_transform_tab == "github"

    # Update to 'ui'
    payload = UpdateUserPreferencesSchema(last_visited_transform_tab="ui")
    response = update_user_preferences(request, payload)
    assert response["success"] is True
    updated_preferences = UserPreferences.objects.get(orguser=user_preferences.orguser)
    assert updated_preferences.last_visited_transform_tab == "ui"


def test_update_trial_walkthrough_skipped(orguser):
    """skipping a flow with no prior state records it as skipped, not completed"""
    request = mock_request(orguser)
    payload = UpdateTrialWalkthroughSchema(flow="product_tour", skipped=True)

    response = update_trial_walkthrough(request, payload)

    assert response["success"] is True
    assert response["res"] == {"product_tour": {"skipped": True, "completed": False}}
    prefs = UserPreferences.objects.get(orguser=orguser)
    assert prefs.trial_walkthrough == {"product_tour": {"skipped": True, "completed": False}}


def test_update_trial_walkthrough_completing_after_skip_clears_skipped(orguser):
    """finishing a flow that was previously skipped clears the skipped flag"""
    request = mock_request(orguser)
    update_trial_walkthrough(request, UpdateTrialWalkthroughSchema(flow="insights", skipped=True))

    response = update_trial_walkthrough(
        request, UpdateTrialWalkthroughSchema(flow="insights", completed=True)
    )

    assert response["res"]["insights"] == {"skipped": False, "completed": True}


def test_update_trial_walkthrough_merges_without_clobbering_other_flows(orguser):
    """updating one flow must not wipe out the other two already-recorded flows"""
    request = mock_request(orguser)
    update_trial_walkthrough(
        request, UpdateTrialWalkthroughSchema(flow="product_tour", completed=True)
    )
    update_trial_walkthrough(
        request, UpdateTrialWalkthroughSchema(flow="automate_pipeline", skipped=True)
    )

    response = update_trial_walkthrough(
        request, UpdateTrialWalkthroughSchema(flow="insights", completed=True)
    )

    assert response["res"] == {
        "product_tour": {"skipped": False, "completed": True},
        "automate_pipeline": {"skipped": True, "completed": False},
        "insights": {"skipped": False, "completed": True},
    }


def test_update_trial_walkthrough_feature_nudge_merges_alongside_flows(orguser):
    """dismissing a feature nudge records it under its own key and leaves the flows alone"""
    request = mock_request(orguser)
    update_trial_walkthrough(request, UpdateTrialWalkthroughSchema(flow="insights", completed=True))

    response = update_trial_walkthrough(
        request, UpdateTrialWalkthroughSchema(flow="reports_nudge", completed=True)
    )

    assert response["res"] == {
        "insights": {"skipped": False, "completed": True},
        "reports_nudge": {"skipped": False, "completed": True},
    }
    prefs = UserPreferences.objects.get(orguser=orguser)
    assert prefs.trial_walkthrough["reports_nudge"] == {"skipped": False, "completed": True}


@pytest.mark.parametrize("nudge", ["reports_nudge", "alerts_nudge", "metrics_nudge"])
def test_update_trial_walkthrough_accepts_every_feature_nudge(orguser, nudge):
    """all three nudge keys are in the Literal — a missing one silently 422s the coachmark, so
    the dismissal never sticks and it reappears on every visit"""
    response = update_trial_walkthrough(
        mock_request(orguser), UpdateTrialWalkthroughSchema(flow=nudge, completed=True)
    )

    assert response["res"][nudge] == {"skipped": False, "completed": True}


def test_update_trial_walkthrough_requires_skipped_or_completed(orguser):
    """neither flag set is a client error, not a silent no-op"""
    request = mock_request(orguser)
    payload = UpdateTrialWalkthroughSchema(flow="insights")

    with pytest.raises(HttpError) as excinfo:
        update_trial_walkthrough(request, payload)

    assert str(excinfo.value) == "Set either skipped or completed to true"


@pytest.mark.parametrize(
    "payload",
    [
        UpdateTrialWalkthroughSchema(flow="insights", completed=False),
        UpdateTrialWalkthroughSchema(flow="insights", skipped=False),
        UpdateTrialWalkthroughSchema(flow="insights", skipped=False, completed=False),
    ],
)
def test_update_trial_walkthrough_rejects_explicit_false(orguser, payload):
    """an explicit false is a client error too — it used to 200 having written nothing.

    There is no un-complete/un-skip operation, so a falsy flag can only be a caller mistake,
    and answering 200 to it makes that mistake invisible.
    """
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        update_trial_walkthrough(request, payload)

    assert str(excinfo.value) == "Set either skipped or completed to true"
    assert not UserPreferences.objects.filter(orguser=orguser).exists()


def test_update_trial_walkthrough_completed_wins_over_skipped(orguser):
    """a caller sending both must not store a contradiction"""
    request = mock_request(orguser)
    payload = UpdateTrialWalkthroughSchema(flow="insights", skipped=True, completed=True)

    response = update_trial_walkthrough(request, payload)

    assert response["res"]["insights"] == {"skipped": False, "completed": True}


def test_get_user_preferences_with_transform_tab(orguser):
    """tests fetching user preferences with transform tab preference set"""
    # Create preferences with transform tab
    UserPreferences.objects.create(
        orguser=orguser, enable_email_notifications=True, last_visited_transform_tab="github"
    )

    request = mock_request(orguser)
    response = get_user_preferences(request)
    assert response["success"] is True
    assert response["res"]["last_visited_transform_tab"] == "github"


def test_trial_emails_sent_defaults_to_empty_dict(orguser):
    """a fresh UserPreferences row has no emails recorded as sent"""
    prefs = UserPreferences.objects.create(orguser=orguser)
    assert prefs.trial_emails_sent == {}


def test_trial_emails_sent_round_trips_and_appears_in_to_json(orguser):
    """flags written to the field survive a reload and are exposed via to_json"""
    prefs = UserPreferences.objects.create(orguser=orguser)
    prefs.trial_emails_sent = {"day3": "2026-08-09T10:00:00+00:00"}
    prefs.save()

    prefs.refresh_from_db()
    assert prefs.trial_emails_sent == {"day3": "2026-08-09T10:00:00+00:00"}
    assert prefs.to_json()["trial_emails_sent"] == {"day3": "2026-08-09T10:00:00+00:00"}
