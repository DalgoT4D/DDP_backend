"""Verify the Metric/KPI consumers endpoints surface alerts in their response."""

import os
import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User

from ddpui.api.alert_api import create_alert
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.kpi.kpi_service import KPIService
from ddpui.core.metric.metric_service import MetricService
from ddpui.models.metric import KPI, Metric
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.schemas.alert_schema import (
    AlertCreate,
    RagCondition,
    RecipientIn,
    ThresholdCondition,
)
from ddpui.tests.api_tests.test_user_org_api import mock_request, seed_db


pytestmark = pytest.mark.django_db


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="consext", email="consext@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    o = Org.objects.create(name="Cons Org", slug="cons-org", airbyte_workspace_id="ws")
    yield o
    o.delete()


@pytest.fixture
def orguser(authuser, org):
    ou = OrgUser.objects.create(
        user=authuser, org=org, new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first()
    )
    yield ou
    ou.delete()


@pytest.fixture
def sample_metric(orguser, org):
    m = Metric.objects.create(
        name="Cons Metric",
        schema_name="public",
        table_name="t",
        column="id",
        aggregation="count",
        org=org,
        created_by=orguser,
    )
    yield m
    try:
        m.refresh_from_db()
        m.delete()
    except Metric.DoesNotExist:
        pass


@pytest.fixture
def sample_kpi(orguser, org, sample_metric):
    k = KPI.objects.create(
        name="Cons KPI",
        metric=sample_metric,
        target_value=100.0,
        direction="increase",
        time_grain="monthly",
        org=org,
        created_by=orguser,
    )
    yield k
    try:
        k.refresh_from_db()
        k.delete()
    except KPI.DoesNotExist:
        pass


def test_metric_consumers_lists_alerts(seed_db, orguser, sample_metric, org):
    request = mock_request(orguser)
    create_alert(
        request,
        AlertCreate(
            name="Alert on metric",
            alert_type="metric_threshold",
            metric_id=sample_metric.id,
            condition=ThresholdCondition(operator="lt", value=10),
            schedule_cron="0 9 * * *",
            delivery_channels=["email"],
            message_template="hi {{current_value}}",
            recipients=[RecipientIn(type="orguser", orguser_id=orguser.id)],
        ),
    )

    consumers = MetricService.get_metric_consumers(sample_metric.id, org)

    assert "alerts" in consumers
    assert len(consumers["alerts"]) == 1
    assert consumers["alerts"][0]["name"] == "Alert on metric"
    assert consumers["alerts"][0]["alert_type"] == "metric_threshold"


def test_kpi_consumers_lists_alerts(seed_db, orguser, sample_kpi, org):
    request = mock_request(orguser)
    create_alert(
        request,
        AlertCreate(
            name="Alert on KPI",
            alert_type="kpi_rag",
            kpi_id=sample_kpi.id,
            condition=RagCondition(rag_states=["red"]),
            schedule_cron="0 9 * * *",
            delivery_channels=["email"],
            message_template="hi",
            recipients=[RecipientIn(type="orguser", orguser_id=orguser.id)],
        ),
    )

    consumers = KPIService.get_kpi_consumers(sample_kpi.id, org)

    assert "alerts" in consumers
    assert len(consumers["alerts"]) == 1
    assert consumers["alerts"][0]["name"] == "Alert on KPI"
    assert consumers["alerts"][0]["alert_type"] == "kpi_rag"
