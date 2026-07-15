"""Tests for the 0169 data migration's `owner` <- `created_by` backfill
(Resource Sharing Task 1).

There is no django-test-migrations-style harness in this repo, so instead of
rolling the schema back to a pre-0168 state, this originally called the
migration's `backfill_owner_and_general_access` RunPython function directly
against the real (current) models -- behaviorally equivalent to running it
as part of `migrate`, AS LONG AS the columns it touches still exist on the
live model.

D1 (permission-model rework, migration 0177) REMOVES `general_audience`/
`general_level` from the live model -- calling 0169's function (which also
backfills those two columns in the same pass) against `real_apps.get_model`
now raises `FieldError`, even though a real `manage.py migrate` replay is
unaffected (each migration's RunPython receives a project-state snapshot
frozen at that point in the graph, not the live/current model). So this file
inlines just the owner-backfill fragment instead of calling the full historical
function -- 0169 itself is left untouched, since editing an already-applied
migration is the wrong fix; only this test's shortcut needed updating.
"""

import os

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from django.db.models import F

from ddpui.models.dashboard import Dashboard
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


def _backfill_owner(model_cls):
    """The owner <- created_by fragment of 0169's
    `backfill_owner_and_general_access`, replayed against the live model --
    see module docstring for why the full historical function can't be
    called directly anymore."""
    model_cls.objects.filter(owner_id__isnull=True, created_by_id__isnull=False).update(
        owner_id=F("created_by_id")
    )


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="backfillmodeluser", email="backfillmodeluser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Test Org", slug="test-org-backfill", airbyte_workspace_id="workspace-id"
    )
    yield org
    org.delete()


@pytest.fixture
def orguser(authuser, org):
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


def test_backfill_sets_owner_to_created_by_on_preexisting_row(orguser, org, seed_db):
    """A row created before 0169 ran (owner still null) should end up with
    owner == created_by after the migration's forward function runs."""
    dashboard = Dashboard.objects.create(
        title="Pre-existing Dashboard",
        created_by=orguser,
        org=org,
    )
    # Force the pre-migration state: owner null even though created_by is set.
    Dashboard.objects.filter(id=dashboard.id).update(owner=None)
    dashboard.refresh_from_db()
    assert dashboard.owner_id is None

    _backfill_owner(Dashboard)

    dashboard.refresh_from_db()
    assert dashboard.owner_id == dashboard.created_by_id

    dashboard.delete()


def test_backfill_does_not_overwrite_existing_owner(orguser, org, seed_db):
    """A row that already has an owner (e.g. transferred) must not be
    clobbered back to created_by by a re-run of the backfill."""
    other_user = User.objects.create(username="otherowner", email="otherowner@test.com")
    other_orguser = OrgUser.objects.create(
        user=other_user,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )

    dashboard = Dashboard.objects.create(
        title="Already Owned Dashboard",
        created_by=orguser,
        org=org,
        owner=other_orguser,
    )

    _backfill_owner(Dashboard)

    dashboard.refresh_from_db()
    assert dashboard.owner_id == other_orguser.id

    dashboard.delete()
    other_orguser.delete()
    other_user.delete()
