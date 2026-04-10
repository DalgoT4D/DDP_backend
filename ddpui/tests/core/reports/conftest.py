"""
Shared fixtures for report-related tests (core/reports/).
"""

import pytest
from datetime import date

from django.contrib.auth.models import User

from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.report import ReportSnapshot
from ddpui.auth import ACCOUNT_MANAGER_ROLE


@pytest.fixture
def other_authuser():
    """A second django User for multi-user report tests."""
    user = User.objects.create(
        username="other_testuser",
        email="other_testuser@example.com",
        first_name="Other",
        last_name="User",
    )
    yield user
    user.delete()


@pytest.fixture
def other_orguser(other_authuser, org, seed_db):
    """An OrgUser for the other user in the same org."""
    orguser = OrgUser.objects.create(
        user=other_authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def snapshot(org, orguser):
    """A ReportSnapshot for comment/mention tests."""
    snapshot = ReportSnapshot.objects.create(
        title="Test Report",
        date_column={},
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        frozen_dashboard={},
        frozen_chart_configs={"10": {"title": "Chart A"}, "20": {"title": "Chart B"}},
        created_by=orguser,
        org=org,
    )
    yield snapshot
    snapshot.delete()
