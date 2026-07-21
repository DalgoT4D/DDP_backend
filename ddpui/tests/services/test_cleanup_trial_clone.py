"""tests for the cleanup_trial_clone management command"""

from unittest.mock import patch, Mock

import pytest
from django.core.management import call_command
from django.contrib.auth.models import User

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.trial.warehouse_provision import email_hash8

pytestmark = pytest.mark.django_db


@patch("ddpui.management.commands.cleanup_trial_clone.drop_trial_database")
@patch("ddpui.management.commands.cleanup_trial_clone.delete_trial_org")
def test_cleanup_deletes_org_db_and_user(mock_delete_org, mock_drop):
    role, _ = Role.objects.get_or_create(
        slug=ACCOUNT_MANAGER_ROLE, defaults={"name": "admin", "level": 1}
    )
    org = Org.objects.create(name="Trial x", slug="trial-x")
    user = User.objects.create_user(username="t@x.org", email="t@x.org")
    OrgUser.objects.create(user=user, org=org, new_role=role)

    call_command("cleanup_trial_clone", "--email", "t@x.org")

    mock_delete_org.assert_called_once_with(org)
    mock_drop.assert_called_once_with("t@x.org")
    # the leftover Django user is removed too
    assert not User.objects.filter(username="t@x.org").exists()


@patch("ddpui.management.commands.cleanup_trial_clone.drop_trial_database")
@patch("ddpui.management.commands.cleanup_trial_clone.delete_trial_org")
def test_cleanup_still_drops_db_when_no_org(mock_delete_org, mock_drop):
    """No trial org/user (already gone) → still attempt the deterministic RDS drop, don't error."""
    call_command("cleanup_trial_clone", "--email", "gone@x.org")

    mock_delete_org.assert_not_called()
    mock_drop.assert_called_once_with("gone@x.org")


@patch("ddpui.management.commands.cleanup_trial_clone.drop_trial_database")
@patch("ddpui.management.commands.cleanup_trial_clone.delete_trial_org")
def test_cleanup_reaps_orphaned_org_by_email_slug(mock_delete_org, mock_drop):
    """An org with 0 OrgUsers (orphan) is still found via the deterministic `trial-<hash8>` slug
    and reaped — this is the case that previously blocked the next clone by leaving the name taken.
    """
    email = "orphan@x.org"
    orphan = Org.objects.create(
        name=f"Trial {email_hash8(email)} orphan health_org",
        slug=f"trial-{email_hash8(email)}-orph",
    )

    call_command("cleanup_trial_clone", "--email", email)

    mock_delete_org.assert_called_once_with(orphan)
    mock_drop.assert_called_once_with(email)


@patch("ddpui.management.commands.cleanup_trial_clone.RedisClient")
@patch("ddpui.management.commands.cleanup_trial_clone.drop_trial_database")
@patch("ddpui.management.commands.cleanup_trial_clone.delete_trial_org")
def test_cleanup_clears_activation_lock(mock_delete_org, mock_drop, mock_redis_cls):
    """cleanup must delete the per-email `trial-activating:<email>` lock so the email is
    immediately reusable for a fresh signup→activate (otherwise it 409s until the 10-min TTL)."""
    redis = Mock()
    mock_redis_cls.get_instance.return_value = redis

    call_command("cleanup_trial_clone", "--email", "gone@x.org")

    redis.delete.assert_called_once_with("trial-activating:gone@x.org")
