"""Tests for the subscription/upgrade request endpoint in ddpui.api.org_preferences_api.

The endpoint is once-per-org and its only side effect visible to the team is the email, so
these tests pin three things: the flag transition, exactly who gets emailed, and — most
importantly — that the flag stays False whenever nobody was actually told.
"""

import os
from unittest.mock import Mock, patch

import django
import pytest
from ninja.errors import HttpError

from django.contrib.auth.models import User

from ddpui import auth
from ddpui.models.org import Org
from ddpui.models.org_plans import OrgPlans
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.api.org_preferences_api import initiate_upgrade_dalgo_plan
from ddpui.tests.api_tests.test_user_org_api import mock_request, seed_db

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

pytestmark = pytest.mark.django_db

BIZ_DEV = "priyesh@projecttech4dev.org,partnerships@dalgo.org"


@pytest.fixture
def org():
    org = Org.objects.create(
        airbyte_workspace_id=None,
        slug="test-sub-org-slug",
        name="Test Sub Org",
    )
    yield org
    org.delete()


@pytest.fixture
def orguser(org, seed_db):  # pylint: disable=redefined-outer-name, unused-argument
    """an admin org-user — the role that holds can_initiate_org_plan_upgrade"""
    user = User.objects.create(
        username="subuser", email="subuser@example.com", password="pwd", first_name="Sub"
    )
    orguser = OrgUser.objects.create(
        user=user,
        org=org,
        new_role=Role.objects.filter(slug=auth.ACCOUNT_MANAGER_ROLE).first(),
        work_domain="monitoring_evaluation",
    )
    yield orguser
    orguser.delete()
    user.delete()


@pytest.fixture
def org_plan(org):  # pylint: disable=redefined-outer-name
    plan = OrgPlans.objects.create(org=org, base_plan="Free Trial")
    yield plan
    plan.delete()


def test_upgrade_raises_when_org_has_no_plan(orguser):  # pylint: disable=redefined-outer-name
    with pytest.raises(HttpError) as excinfo:
        initiate_upgrade_dalgo_plan(mock_request(orguser))
    assert str(excinfo.value) == "Org's Plan not found"


@patch.dict(os.environ, {"BIZ_DEV_EMAILS": BIZ_DEV})
@patch("ddpui.api.org_preferences_api.send_text_message")
def test_upgrade_emails_every_recipient_and_sets_flag(
    send_text_message, orguser, org_plan
):  # pylint: disable=redefined-outer-name
    response = initiate_upgrade_dalgo_plan(mock_request(orguser))

    assert response == {"success": True, "already_requested": False}
    org_plan.refresh_from_db()
    assert org_plan.upgrade_requested is True

    assert send_text_message.call_count == 2
    recipients = [call.args[0] for call in send_text_message.call_args_list]
    assert recipients == ["priyesh@projecttech4dev.org", "partnerships@dalgo.org"]

    subject, message = send_text_message.call_args_list[0].args[1:3]
    assert subject == "Subscription request: Test Sub Org"
    assert "test-sub-org-slug" in message
    assert "subuser@example.com" in message
    # both roles are present and distinct: job title vs RBAC role
    assert "Monitoring and Evaluation" in message
    assert "Dalgo role:   Admin" in message


@patch.dict(os.environ, {"BIZ_DEV_EMAILS": " priyesh@projecttech4dev.org , ,"})
@patch("ddpui.api.org_preferences_api.send_text_message")
def test_upgrade_trims_whitespace_and_drops_blank_recipients(
    send_text_message, orguser, org_plan
):  # pylint: disable=redefined-outer-name, unused-argument
    initiate_upgrade_dalgo_plan(mock_request(orguser))

    assert send_text_message.call_count == 1
    assert send_text_message.call_args.args[0] == "priyesh@projecttech4dev.org"


@patch.dict(os.environ, {"BIZ_DEV_EMAILS": BIZ_DEV})
@patch("ddpui.api.org_preferences_api.send_text_message")
def test_upgrade_is_a_noop_when_already_requested(
    send_text_message, orguser, org_plan
):  # pylint: disable=redefined-outer-name
    org_plan.upgrade_requested = True
    org_plan.save()

    response = initiate_upgrade_dalgo_plan(mock_request(orguser))

    assert response["success"] is True
    assert response["already_requested"] is True
    send_text_message.assert_not_called()


@patch.dict(os.environ, {"BIZ_DEV_EMAILS": ""})
@patch("ddpui.api.org_preferences_api.send_text_message")
def test_upgrade_leaves_flag_false_when_no_recipients_configured(
    send_text_message, orguser, org_plan
):  # pylint: disable=redefined-outer-name
    with pytest.raises(HttpError):
        initiate_upgrade_dalgo_plan(mock_request(orguser))

    send_text_message.assert_not_called()
    org_plan.refresh_from_db()
    # nobody was told, so a later correctly-configured retry must still be able to send
    assert org_plan.upgrade_requested is False


@patch.dict(os.environ, {"BIZ_DEV_EMAILS": BIZ_DEV})
@patch("ddpui.api.org_preferences_api.send_text_message")
def test_upgrade_survives_one_failing_recipient(
    send_text_message, orguser, org_plan
):  # pylint: disable=redefined-outer-name
    send_text_message.side_effect = [Exception("bounced"), Mock()]

    response = initiate_upgrade_dalgo_plan(mock_request(orguser))

    assert response["success"] is True
    assert send_text_message.call_count == 2
    org_plan.refresh_from_db()
    assert org_plan.upgrade_requested is True


@patch.dict(os.environ, {"BIZ_DEV_EMAILS": BIZ_DEV})
@patch("ddpui.api.org_preferences_api.send_text_message")
def test_upgrade_leaves_flag_false_when_every_recipient_fails(
    send_text_message, orguser, org_plan
):  # pylint: disable=redefined-outer-name
    send_text_message.side_effect = Exception("ses down")

    with pytest.raises(HttpError):
        initiate_upgrade_dalgo_plan(mock_request(orguser))

    org_plan.refresh_from_db()
    assert org_plan.upgrade_requested is False
