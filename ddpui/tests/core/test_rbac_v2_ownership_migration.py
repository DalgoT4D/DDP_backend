"""
Access Control v2 — owner backfill verification.

Tests the post-migration invariant: owner is set on content resources.
We exercise the backfill logic directly using real model classes so this
stays independent of Django's historical-model machinery.
"""

import pytest
from django.contrib.auth.models import User
from django.core.management import call_command

from ddpui.models.dashboard import Dashboard
from ddpui.models.report import ReportSnapshot
from ddpui.models.org_user import OrgUser
from ddpui.models.org import Org
from ddpui.models.role_based_access import Role

pytestmark = pytest.mark.django_db


@pytest.fixture(scope="module")
def seed_roles(django_db_setup, django_db_blocker):
    with django_db_blocker.unblock():
        call_command("loaddata", "001_roles.json")
        call_command("loaddata", "002_permissions.json")
        call_command("loaddata", "003_role_permissions.json")


def _make_orguser(email: str, org: Org, role_slug: str) -> OrgUser:
    user, _ = User.objects.get_or_create(username=email, defaults={"email": email})
    role = Role.objects.get(slug=role_slug)
    return OrgUser.objects.create(user=user, org=org, new_role=role)


def _run_backfill():
    """Execute the backfill logic against the live DB (mirrors migration 0166)."""
    admin_role = Role.objects.filter(slug="admin").first()

    def oldest_admin(org_id):
        if admin_role is None:
            return None
        return OrgUser.objects.filter(org_id=org_id, new_role=admin_role).order_by("id").first()

    for Model in [Dashboard, ReportSnapshot]:
        for resource in Model.objects.select_related("created_by", "org").iterator():
            if resource.created_by_id is not None:
                resource.owner_id = resource.created_by_id
            else:
                fallback = oldest_admin(resource.org_id)
                resource.owner_id = fallback.id if fallback else None
            resource.save(update_fields=["owner"])


def test_owner_set_to_created_by(seed_roles):
    org = Org.objects.create(name="BackfillOrg1", slug="backfillorg1")
    analyst = _make_orguser("analyst@test.com", org, "analyst")
    dashboard = Dashboard.objects.create(title="D1", org=org, created_by=analyst, tabs=[])
    assert dashboard.owner_id is None  # not yet set

    _run_backfill()
    dashboard.refresh_from_db()
    assert dashboard.owner_id == analyst.id


def test_owner_falls_back_to_oldest_admin_when_no_created_by(seed_roles):
    org = Org.objects.create(name="BackfillOrg2", slug="backfillorg2")
    admin = _make_orguser("admin@test2.com", org, "admin")
    snapshot = ReportSnapshot.objects.create(
        title="R1", org=org, created_by=None, period_start=None, period_end=None
    )
    assert snapshot.owner_id is None

    _run_backfill()
    snapshot.refresh_from_db()
    assert snapshot.owner_id == admin.id
