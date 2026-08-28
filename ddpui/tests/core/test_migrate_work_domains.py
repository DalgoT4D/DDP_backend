"""Tests for `manage.py migrate_work_domains`."""

from io import StringIO

import pytest
from django.contrib.auth.models import User
from django.core.management import call_command
from django.utils import timezone

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.trial_signup import TrialSignup

pytestmark = pytest.mark.django_db


def make_orguser(email: str, work_domain: str | None) -> OrgUser:
    org = Org.objects.create(name=email, slug=email.replace("@", "-"))
    user = User.objects.create(username=email, email=email)
    return OrgUser.objects.create(org=org, user=user, work_domain=work_domain)


def run(dry_run: bool = False) -> str:
    out = StringIO()
    call_command("migrate_work_domains", stdout=out, **({"dry_run": True} if dry_run else {}))
    return out.getvalue()


def test_renames_and_retirements_are_applied():
    renamed = make_orguser("pm@x.org", "program_manager")
    tech = make_orguser("dt@x.org", "data_tech")
    consultant = make_orguser("c@x.org", "consultant")
    dropped = make_orguser("fw@x.org", "field_worker")
    nones = make_orguser("n@x.org", "none")
    untouched = make_orguser("me@x.org", "monitoring_evaluation")

    run()

    for orguser, expected in [
        (renamed, "program_implementation"),
        (tech, "data_technology"),
        (consultant, "external_consultant"),
        (dropped, None),
        (nones, None),
        (untouched, "monitoring_evaluation"),
    ]:
        orguser.refresh_from_db()
        assert orguser.work_domain == expected


def test_trial_signup_records_are_migrated_too():
    record = TrialSignup.objects.create(
        email="t@x.org", org_name="Acme", role="consultant", signed_up_at=timezone.now()
    )

    run()

    record.refresh_from_db()
    assert record.role == "external_consultant"


def test_dry_run_writes_nothing_but_reports():
    orguser = make_orguser("pm@x.org", "program_manager")

    output = run(dry_run=True)

    orguser.refresh_from_db()
    assert orguser.work_domain == "program_manager"
    assert "DRY RUN" in output
    assert "program_manager -> program_implementation: 1" in output


def test_unrecognised_values_are_reported_and_left_alone():
    orguser = make_orguser("x@x.org", "brand_new_option")

    output = run()

    orguser.refresh_from_db()
    assert orguser.work_domain == "brand_new_option"
    assert "unrecognised 'brand_new_option'" in output


def test_rerunning_is_a_no_op():
    orguser = make_orguser("pm@x.org", "program_manager")

    run()
    output = run()

    orguser.refresh_from_db()
    assert orguser.work_domain == "program_implementation"
    assert "->" not in output
