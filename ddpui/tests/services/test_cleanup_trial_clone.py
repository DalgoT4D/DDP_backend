"""tests for the cleanup_trial_clone management command"""

from datetime import timedelta
from unittest.mock import patch, call, Mock

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.contrib.auth.models import User
from django.utils import timezone

from ddpui.models.org import Org
from ddpui.models.org_plans import OrgPlans, OrgPlanType
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.trial.constants import TRIAL_REAP_STAGGER_SECONDS
from ddpui.core.trial.warehouse_provision import email_hash8

pytestmark = pytest.mark.django_db


def make_trial(
    email: str, *, expired: bool = True, base_plan=OrgPlanType.FREE_TRIAL.value, slug=None
):
    """A clone-shaped trial: `trial-<hash8>-` org + OrgUser + an OrgPlans window."""
    role, _ = Role.objects.get_or_create(
        slug=ACCOUNT_MANAGER_ROLE, defaults={"name": "admin", "level": 1}
    )
    org = Org.objects.create(
        name=f"Trial {email_hash8(email)} acme",
        slug=slug or f"trial-{email_hash8(email)}-acme",
    )
    user = User.objects.create_user(username=email, email=email)
    OrgUser.objects.create(user=user, org=org, new_role=role)
    now = timezone.now()
    OrgPlans.objects.create(
        org=org,
        base_plan=base_plan,
        start_date=now - timedelta(days=14),
        end_date=now - timedelta(hours=1) if expired else now + timedelta(days=7),
    )
    return org


@patch("ddpui.management.commands.cleanup_trial_clone.drop_trial_database")
@patch("ddpui.management.commands.cleanup_trial_clone.delete_trial_org")
def test_cleanup_deletes_org_db_and_user(mock_delete_org, mock_drop):
    role, _ = Role.objects.get_or_create(
        slug=ACCOUNT_MANAGER_ROLE, defaults={"name": "admin", "level": 1}
    )
    # a real clone slug, `trial-<8 hex>-<label>` — the shape the command matches on
    org = Org.objects.create(name="Trial x", slug=f"trial-{email_hash8('t@x.org')}-x")
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
def test_cleanup_clears_running_clone_lock(mock_delete_org, mock_drop, mock_redis_cls):
    """cleanup must delete the per-email running-clone lock so the email is immediately reusable
    for a fresh signup→activate (otherwise it 409s until the TTL backstop expires)."""
    from ddpui.core.trial.activation import CLONE_LOCK_PREFIX

    redis = Mock()
    mock_redis_cls.get_instance.return_value = redis

    call_command("cleanup_trial_clone", "--email", "gone@x.org")

    redis.delete.assert_called_once_with(f"{CLONE_LOCK_PREFIX}gone@x.org")


@patch("ddpui.management.commands.cleanup_trial_clone.drop_trial_database")
@patch("ddpui.management.commands.cleanup_trial_clone.delete_trial_org")
def test_expired_reaps_the_trial(mock_delete_org, mock_drop):
    """--expired deletes a trial whose end_date has passed, via the same path as --email."""
    org = make_trial("expired@x.org")

    call_command("cleanup_trial_clone", "--expired")

    mock_delete_org.assert_called_once_with(org)
    mock_drop.assert_called_once_with("expired@x.org")
    assert not User.objects.filter(username="expired@x.org").exists()


@patch("ddpui.management.commands.cleanup_trial_clone.drop_trial_database")
@patch("ddpui.management.commands.cleanup_trial_clone.delete_trial_org")
def test_expired_skips_non_clone_org_on_free_trial_plan(mock_delete_org, mock_drop):
    """THE footgun: an admin can put a real customer org on the Free Trial plan via
    createorgplan. Once that plan lapses it matches base_plan+end_date exactly like a trial —
    only the `trial-` slug tells them apart. Reaping it would delete a paying org's warehouse.
    """
    make_trial("customer@x.org", slug="acme-health")

    call_command("cleanup_trial_clone", "--expired")

    mock_delete_org.assert_not_called()
    mock_drop.assert_not_called()
    assert User.objects.filter(username="customer@x.org").exists()


@patch("ddpui.management.commands.cleanup_trial_clone.drop_trial_database")
@patch("ddpui.management.commands.cleanup_trial_clone.delete_trial_org")
def test_expired_skips_live_trial_and_non_trial_plan(mock_delete_org, mock_drop):
    """A trial still inside its window, and a clone-slugged org on a paid plan, both survive."""
    make_trial("live@x.org", expired=False)
    make_trial("paid@x.org", base_plan=OrgPlanType.DALGO.value)

    call_command("cleanup_trial_clone", "--expired")

    mock_delete_org.assert_not_called()
    mock_drop.assert_not_called()


@patch("ddpui.management.commands.cleanup_trial_clone.time.sleep")
@patch("ddpui.management.commands.cleanup_trial_clone.drop_trial_database")
@patch("ddpui.management.commands.cleanup_trial_clone.delete_trial_org")
def test_expired_continues_after_a_failure(mock_delete_org, mock_drop, mock_sleep):
    """One wedged org (Airbyte down, RDS refusing) must not strand the rest of the reap."""
    make_trial("first@x.org")
    make_trial("second@x.org")
    mock_delete_org.side_effect = [Exception("airbyte unreachable"), None]

    call_command("cleanup_trial_clone", "--expired")

    assert mock_delete_org.call_count == 2
    # the failing org never reached its RDS drop; the healthy one did
    mock_drop.assert_called_once_with("second@x.org")
    assert User.objects.filter(username="first@x.org").exists()
    assert not User.objects.filter(username="second@x.org").exists()


@patch("ddpui.management.commands.cleanup_trial_clone.time.sleep")
@patch("ddpui.management.commands.cleanup_trial_clone.drop_trial_database")
@patch("ddpui.management.commands.cleanup_trial_clone.delete_trial_org")
def test_expired_staggers_between_orgs_only(mock_delete_org, mock_drop, mock_sleep):
    """Three orgs → two gaps. The first org must not wait, or every nightly run starts 30s late."""
    make_trial("a@x.org")
    make_trial("b@x.org")
    make_trial("c@x.org")

    call_command("cleanup_trial_clone", "--expired")

    assert mock_sleep.call_count == 2
    assert mock_sleep.call_args_list == [
        call(TRIAL_REAP_STAGGER_SECONDS),
        call(TRIAL_REAP_STAGGER_SECONDS),
    ]


@patch("ddpui.management.commands.cleanup_trial_clone.drop_trial_database")
@patch("ddpui.management.commands.cleanup_trial_clone.delete_trial_org")
def test_expired_orphan_org_deletes_org_only(mock_delete_org, mock_drop):
    """No OrgUser → no email → the RDS db name can't be computed. Delete the org, leave the db."""
    org = Org.objects.create(name="Trial deadbeef orphan", slug="trial-deadbeef-orph")
    now = timezone.now()
    OrgPlans.objects.create(
        org=org,
        base_plan=OrgPlanType.FREE_TRIAL.value,
        start_date=now - timedelta(days=14),
        end_date=now - timedelta(hours=1),
    )

    call_command("cleanup_trial_clone", "--expired")

    mock_delete_org.assert_called_once_with(org)
    mock_drop.assert_not_called()


def test_email_and_expired_are_mutually_exclusive():
    with pytest.raises(CommandError):
        call_command("cleanup_trial_clone", "--email", "a@x.org", "--expired")
    with pytest.raises(CommandError):
        call_command("cleanup_trial_clone")


@patch("ddpui.celeryworkers.tasks.call_command")
def test_reap_task_delegates_to_the_command(mock_call_command):
    """the nightly task must stay a thin wrapper — no second copy of the selection logic"""
    from ddpui.celeryworkers.tasks import reap_expired_trial_orgs

    mock_call_command.return_value = "reaped 2 expired trial(s)"

    assert reap_expired_trial_orgs() == "reaped 2 expired trial(s)"
    mock_call_command.assert_called_once_with("cleanup_trial_clone", expired=True)


@patch("ddpui.management.commands.cleanup_trial_clone.drop_trial_database")
@patch("ddpui.management.commands.cleanup_trial_clone.delete_trial_org")
def test_reap_never_touches_the_users_other_org(mock_delete_org, mock_drop):
    """A trial email later invited into a REAL org must not take that org down with the trial.

    account_exists_for_email blocks a trial for an email that already has an OrgUser, but nothing
    stops the invitation going the other way afterwards — and this runs unattended at midnight.
    """
    role, _ = Role.objects.get_or_create(
        slug=ACCOUNT_MANAGER_ROLE, defaults={"name": "admin", "level": 1}
    )
    trial_org = make_trial("dual@x.org")
    real_org = Org.objects.create(name="Acme Health", slug="acme-health")
    user = User.objects.get(username="dual@x.org")
    OrgUser.objects.create(user=user, org=real_org, new_role=role)

    call_command("cleanup_trial_clone", "--expired")

    mock_delete_org.assert_called_once_with(trial_org)
    assert Org.objects.filter(slug="acme-health").exists()
    # and the login survives, because the user still belongs to the real org
    assert User.objects.filter(username="dual@x.org").exists()


@patch("ddpui.management.commands.cleanup_trial_clone.drop_trial_database")
@patch("ddpui.management.commands.cleanup_trial_clone.delete_trial_org")
def test_expired_skips_org_merely_named_trial(mock_delete_org, mock_drop):
    """ "Trial Foundation" slugs to `trial-foundation` — a prefix match would reap it."""
    make_trial("named@x.org", slug="trial-foundation")

    call_command("cleanup_trial_clone", "--expired")

    mock_delete_org.assert_not_called()
    assert Org.objects.filter(slug="trial-foundation").exists()
