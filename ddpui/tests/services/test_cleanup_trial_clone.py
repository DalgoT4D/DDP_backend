"""tests for the cleanup_trial_clone management command"""

from unittest.mock import patch, Mock

import pytest
from django.core.management import call_command
from django.contrib.auth.models import User

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE

pytestmark = pytest.mark.django_db


@patch("ddpui.management.commands.cleanup_trial_clone.drop_trial_database")
@patch("ddpui.management.commands.cleanup_trial_clone.OrgCleanupService")
def test_cleanup_deletes_org_db_and_user(mock_cleanup_cls, mock_drop):
    role, _ = Role.objects.get_or_create(
        slug=ACCOUNT_MANAGER_ROLE, defaults={"name": "admin", "level": 1}
    )
    org = Org.objects.create(name="Trial x", slug="trial-x")
    user = User.objects.create_user(username="t@x.org", email="t@x.org")
    OrgUser.objects.create(user=user, org=org, new_role=role)

    call_command("cleanup_trial_clone", "--email", "t@x.org")

    mock_cleanup_cls.assert_called_once_with(org, dry_run=False)
    mock_cleanup_cls.return_value.delete_org.assert_called_once()
    mock_drop.assert_called_once_with("t@x.org")
    # the leftover Django user is removed too
    assert not User.objects.filter(username="t@x.org").exists()


@patch("ddpui.management.commands.cleanup_trial_clone.drop_trial_database")
@patch("ddpui.management.commands.cleanup_trial_clone.OrgCleanupService")
def test_cleanup_still_drops_db_when_no_org(mock_cleanup_cls, mock_drop):
    """No trial org/user (already gone) → still attempt the deterministic RDS drop, don't error."""
    call_command("cleanup_trial_clone", "--email", "gone@x.org")

    mock_cleanup_cls.assert_not_called()
    mock_drop.assert_called_once_with("gone@x.org")
