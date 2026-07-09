"""Tests for the 0169 data migration that backfills `owner` from `created_by`
and the `general_audience` / `general_level` general-access columns
(Resource Sharing Task 1).

There is no django-test-migrations-style harness in this repo, so instead of
rolling the schema back to a pre-0168 state, this calls the migration's
RunPython function directly against the real models — the migration's forward
function only reads/writes ordinary model fields, so this is behaviorally
equivalent to running it as part of `migrate`.
"""

import importlib
import os

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.apps import apps as real_apps
from django.contrib.auth.models import User

from ddpui.models.dashboard import Dashboard
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db

backfill_migration = importlib.import_module(
    "ddpui.migrations.0169_backfill_owner_and_general_access"
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

    backfill_migration.backfill_owner_and_general_access(real_apps, schema_editor=None)

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

    backfill_migration.backfill_owner_and_general_access(real_apps, schema_editor=None)

    dashboard.refresh_from_db()
    assert dashboard.owner_id == other_orguser.id

    dashboard.delete()
    other_orguser.delete()
    other_user.delete()
