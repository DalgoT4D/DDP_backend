"""Tests for the schema-change notification trigger."""

import os
from unittest.mock import patch

import django
import pytest
from django.contrib.auth.models import User

from ddpui import auth
from ddpui.core.notifications.triggers.schema_change import notify_schema_change
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.userpreferences import UserPreferences
from ddpui.tests.api_tests.test_user_org_api import seed_db  # noqa: F401 — pytest fixture

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

pytestmark = pytest.mark.django_db


def _make_orguser(email: str, org: Org, *, enable_schema_change: bool) -> OrgUser:
    user = User.objects.create(username=email, email=email, password="pw")
    orguser = OrgUser.objects.create(
        user=user,
        org=org,
        new_role=Role.objects.filter(slug=auth.ACCOUNT_MANAGER_ROLE).first(),
    )
    UserPreferences.objects.create(
        orguser=orguser, enable_schema_change_notifications=enable_schema_change
    )
    return orguser


def test_notify_schema_change_only_fans_out_to_opted_in_users(seed_db):
    """Opted-in users get the notification; everyone else is silent."""
    org = Org.objects.create(slug="tsc-org-a")
    opted_in = _make_orguser("in@x.org", org, enable_schema_change=True)
    _make_orguser("out@x.org", org, enable_schema_change=False)

    with patch(
        "ddpui.core.notifications.triggers.schema_change.create_notification"
    ) as mock_create:
        mock_create.return_value = (None, {"res": {}, "errors": []})
        notify_schema_change(org, "msg", "subj")

    mock_create.assert_called_once()
    payload = mock_create.call_args[0][0]
    assert payload.recipients == [opted_in.id]
    assert payload.email_subject == "subj"
    assert payload.message == "msg"


def test_notify_schema_change_scopes_to_org(seed_db):
    """A user opted-in in a DIFFERENT org must not receive."""
    org = Org.objects.create(slug="tsc-org-b")
    other_org = Org.objects.create(slug="tsc-org-b-other")
    ours = _make_orguser("mine@x.org", org, enable_schema_change=True)
    _make_orguser("theirs@x.org", other_org, enable_schema_change=True)

    with patch(
        "ddpui.core.notifications.triggers.schema_change.create_notification"
    ) as mock_create:
        mock_create.return_value = (None, {"res": {}, "errors": []})
        notify_schema_change(org, "msg", "subj")

    payload = mock_create.call_args[0][0]
    assert payload.recipients == [ours.id]


def test_notify_schema_change_noop_when_no_subscribers(seed_db):
    """If no user in the org has opted in, create_notification is never called."""
    org = Org.objects.create(slug="tsc-org-c")
    _make_orguser("none@x.org", org, enable_schema_change=False)

    with patch(
        "ddpui.core.notifications.triggers.schema_change.create_notification"
    ) as mock_create:
        notify_schema_change(org, "msg", "subj")

    mock_create.assert_not_called()
