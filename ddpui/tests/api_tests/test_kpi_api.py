"""API Tests for KPI endpoints"""

import os
import django
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.metric import Metric, KPI
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.kpi_api import (
    list_kpis,
    create_kpi,
    get_kpi,
    update_kpi,
    delete_kpi,
)
from ddpui.schemas.kpi_schema import KPICreate, KPIUpdate
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="kpiapiuser", email="kpiapiuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="KPI API Test Org",
        slug="kpi-api-test",
        airbyte_workspace_id="workspace-id",
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


@pytest.fixture
def sample_metric(orguser, org):
    metric = Metric.objects.create(
        name="API KPI Metric",
        schema_name="public",
        table_name="beneficiaries",
        column="amount",
        aggregation="sum",
        org=org,
        created_by=orguser,
    )
    yield metric
    try:
        metric.refresh_from_db()
        metric.delete()
    except Metric.DoesNotExist:
        pass


@pytest.fixture
def sample_kpi(orguser, org, sample_metric):
    kpi = KPI.objects.create(
        name="API Test KPI",
        metric=sample_metric,
        target_value=1000.0,
        direction="increase",
        time_grain="monthly",
        org=org,
        created_by=orguser,
    )
    yield kpi
    try:
        kpi.refresh_from_db()
        kpi.delete()
    except KPI.DoesNotExist:
        pass


# ── Tests ───────────────────────────────────────────────────────────────────


class TestListKPIs:
    def test_list_success(self, orguser, sample_kpi, seed_db):
        request = mock_request(orguser)
        response = list_kpis(request)
        assert response.total >= 1

    def test_list_search(self, orguser, sample_kpi, seed_db):
        request = mock_request(orguser)
        response = list_kpis(request, search="API Test")
        assert response.total >= 1

    def test_list_empty(self, orguser, seed_db):
        request = mock_request(orguser)
        response = list_kpis(request, search="nonexistent_xyz")
        assert response.total == 0


class TestCreateKPI:
    def test_create_success(self, orguser, sample_metric, seed_db):
        request = mock_request(orguser)
        payload = KPICreate(
            metric_id=sample_metric.id,
            direction="increase",
            time_grain="monthly",
            target_value=500.0,
        )
        response = create_kpi(request, payload)
        assert response.id is not None
        assert response.name == sample_metric.name
        KPI.objects.filter(id=response.id).delete()

    def test_create_invalid_metric(self, orguser, seed_db):
        request = mock_request(orguser)
        payload = KPICreate(
            metric_id=99999,
            direction="increase",
            time_grain="monthly",
        )
        with pytest.raises(HttpError) as exc_info:
            create_kpi(request, payload)
        assert exc_info.value.status_code == 404

    def test_create_invalid_direction(self, orguser, sample_metric, seed_db):
        request = mock_request(orguser)
        payload = KPICreate(
            metric_id=sample_metric.id,
            direction="sideways",
            time_grain="monthly",
        )
        with pytest.raises(HttpError) as exc_info:
            create_kpi(request, payload)
        assert exc_info.value.status_code == 400


class TestGetKPI:
    def test_get_success(self, orguser, sample_kpi, seed_db):
        request = mock_request(orguser)
        response = get_kpi(request, sample_kpi.id)
        assert response.id == sample_kpi.id
        assert response.metric.id == sample_kpi.metric_id

    def test_get_not_found(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc_info:
            get_kpi(request, 99999)
        assert exc_info.value.status_code == 404


class TestUpdateKPI:
    def test_update_success(self, orguser, sample_kpi, seed_db):
        request = mock_request(orguser)
        payload = KPIUpdate(name="Updated API KPI", target_value=2000.0)
        response = update_kpi(request, sample_kpi.id, payload)
        assert response.name == "Updated API KPI"
        assert response.target_value == 2000.0

    def test_update_not_found(self, orguser, seed_db):
        request = mock_request(orguser)
        payload = KPIUpdate(name="Nope")
        with pytest.raises(HttpError) as exc_info:
            update_kpi(request, 99999, payload)
        assert exc_info.value.status_code == 404


class TestDeleteKPI:
    def test_delete_success(self, orguser, sample_kpi, seed_db):
        request = mock_request(orguser)
        response = delete_kpi(request, sample_kpi.id)
        assert response["success"] is True

    def test_delete_not_found(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc_info:
            delete_kpi(request, 99999)
        assert exc_info.value.status_code == 404
