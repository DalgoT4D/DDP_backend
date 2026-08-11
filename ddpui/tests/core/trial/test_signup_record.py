"""Tests for the durable TrialSignup record (ddpui/core/trial/signup_record.py).

Covers the three lifecycle write points — signup opens a record, a successful clone stamps
trial_start_date, the deletion stamps deleted_at — plus the rules that make the record survive:
repeat signups don't multiply rows, a closed record isn't reopened or re-stamped, and a fresh
signup after a deletion opens a new row instead of overwriting the old trial's history.
"""

from datetime import timedelta

import pytest
from django.utils import timezone

from ddpui.core.trial.signup_record import (
    open_record_for_email,
    record_deletion,
    record_signup,
    record_trial_start,
)
from ddpui.models.trial_signup import TrialSignup
from ddpui.schemas.trial_schema import TrialSignupSchema

pytestmark = pytest.mark.django_db


def signup_payload(email="a@b.org", org_name="Acme", role="data_technology"):
    return TrialSignupSchema(email=email, org_name=org_name, role=role)


def test_record_signup_creates_open_record():
    record = record_signup(signup_payload())

    assert record.email == "a@b.org"
    assert record.org_name == "Acme"
    assert record.role == "data_technology"
    assert record.signed_up_at is not None
    # not a trial yet, and not deleted — the open state
    assert record.trial_start_date is None
    assert record.deleted_at is None


def test_repeat_signup_updates_the_open_record_instead_of_adding_a_row():
    """Re-requesting the verification email is not a second trial."""
    first = record_signup(signup_payload(org_name="Acme", role="data_technology"))
    second = record_signup(signup_payload(org_name="Acme Renamed", role="leadership"))

    assert second.id == first.id
    assert TrialSignup.objects.filter(email="a@b.org").count() == 1
    assert second.org_name == "Acme Renamed"
    assert second.role == "leadership"


def test_record_trial_start_stamps_the_open_record():
    record_signup(signup_payload())
    started = timezone.now()

    record_trial_start("a@b.org", started)

    record = TrialSignup.objects.get(email="a@b.org")
    assert record.trial_start_date == started


def test_record_trial_start_is_a_noop_without_a_record():
    """A clone started by `manage.py clone_template_org` has no signup row to stamp."""
    record_trial_start("never-signed-up@b.org", timezone.now())

    assert TrialSignup.objects.count() == 0


def test_record_deletion_closes_the_open_record():
    record_signup(signup_payload())

    closed = record_deletion("a@b.org")

    assert closed == 1
    record = TrialSignup.objects.get(email="a@b.org")
    assert record.deleted_at is not None
    # the signup facts survive the deletion — that is the whole point of the table
    assert record.email == "a@b.org"
    assert record.org_name == "Acme"
    assert record.role == "data_technology"


def test_record_deletion_keeps_the_original_timestamp_on_a_rerun():
    record_signup(signup_payload())
    record_deletion("a@b.org")
    first_deleted_at = TrialSignup.objects.get(email="a@b.org").deleted_at

    assert record_deletion("a@b.org") == 0
    assert TrialSignup.objects.get(email="a@b.org").deleted_at == first_deleted_at


def test_signup_after_a_delete_opens_a_new_row_and_keeps_the_old_one():
    record_signup(signup_payload(org_name="First Org"))
    record_deletion("a@b.org")

    second = record_signup(signup_payload(org_name="Second Org"))

    assert TrialSignup.objects.filter(email="a@b.org").count() == 2
    assert second.deleted_at is None
    assert second.org_name == "Second Org"
    # the deleted trial's record is untouched
    assert TrialSignup.objects.filter(
        email="a@b.org", org_name="First Org", deleted_at__isnull=False
    ).exists()


def test_open_record_for_email_ignores_closed_records():
    record_signup(signup_payload())
    record_deletion("a@b.org")

    assert open_record_for_email("a@b.org") is None


def test_open_record_for_email_picks_the_newest_on_a_signup_race():
    """Two concurrent signups can both create; every caller must still be deterministic."""
    now = timezone.now()
    TrialSignup.objects.create(email="a@b.org", signed_up_at=now - timedelta(minutes=1))
    newest = TrialSignup.objects.create(email="a@b.org", signed_up_at=now)

    assert open_record_for_email("a@b.org").id == newest.id


def test_record_deletion_closes_every_open_record_for_the_email():
    """The account is deleted once, so a raced pair of records must both reflect it."""
    now = timezone.now()
    TrialSignup.objects.create(email="a@b.org", signed_up_at=now - timedelta(minutes=1))
    TrialSignup.objects.create(email="a@b.org", signed_up_at=now)

    assert record_deletion("a@b.org") == 2
    assert not TrialSignup.objects.filter(email="a@b.org", deleted_at__isnull=True).exists()
