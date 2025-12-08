"""Test cases for charts API endpoints"""

import os
import json
import django
from unittest.mock import Mock, patch, MagicMock
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from django.core.management import call_command
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role, RolePermission, Permission
from ddpui.models.visualization import Chart
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.charts_api import (
    list_charts,
    get_chart,
    create_chart,
    update_chart,
    delete_chart,
    get_chart_data,
    bulk_delete_charts,
)
from ddpui.schemas.chart_schema import (
    ChartCreate,
    ChartUpdate,
    ChartDataPayload,
)
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db

# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    """A django User object"""
    user = User.objects.create(
        username="chartuser", email="chartuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    """An Org object"""
    org = Org.objects.create(
        name="Test Org",
        slug="test-org",
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


@pytest.fixture
def org_warehouse(org):
    """An OrgWarehouse object"""
    warehouse = OrgWarehouse.objects.create(
        org=org,
        wtype="postgres",
        name="Test Warehouse",
        airbyte_destination_id="dest-id",
    )
    yield warehouse
    warehouse.delete()


@pytest.fixture
def orguser(authuser, org):
    """An OrgUser with account manager role"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def sample_chart(orguser, org):
    """A sample chart for testing"""
    chart = Chart.objects.create(
        title="Test Chart",
        description="Test Description",
        chart_type="bar",
        computation_type="aggregated",
        schema_name="public",
        table_name="users",
        extra_config={
            "x_axis_column": "date",
            "y_axis_column": "count",
            "dimension_column": "category",
        },
        created_by=orguser,
        org=org,
    )
    yield chart
    chart.delete()


# ================================================================================
# Test list_charts endpoint
# ================================================================================


def test_list_charts_success(orguser, sample_chart, seed_db):
    """Test successfully listing charts"""
    request = mock_request(orguser)

    response = list_charts(request, page=1, page_size=10)

    assert response.total == 1
    assert len(response.data) == 1
    assert response.data[0].title == "Test Chart"
    assert response.page == 1
    assert response.page_size == 10


def test_list_charts_with_search(orguser, sample_chart, seed_db):
    """Test listing charts with search filter"""
    request = mock_request(orguser)

    response = list_charts(request, page=1, page_size=10, search="Test")

    assert response.total == 1
    assert response.data[0].title == "Test Chart"


def test_list_charts_with_search_no_results(orguser, sample_chart, seed_db):
    """Test listing charts with search that returns no results"""
    request = mock_request(orguser)

    response = list_charts(request, page=1, page_size=10, search="Nonexistent")

    assert response.total == 0
    assert len(response.data) == 0


def test_list_charts_with_chart_type_filter(orguser, sample_chart, seed_db):
    """Test listing charts with chart type filter"""
    request = mock_request(orguser)

    response = list_charts(request, page=1, page_size=10, chart_type="bar")

    assert response.total == 1
    assert response.data[0].chart_type == "bar"


def test_list_charts_pagination(orguser, org, seed_db):
    """Test chart pagination"""
    # Create multiple charts
    for i in range(15):
        Chart.objects.create(
            title=f"Chart {i}",
            chart_type="bar",
            computation_type="raw",
            schema_name="public",
            table_name="test",
            extra_config={},
            created_by=orguser,
            org=org,
        )

    request = mock_request(orguser)

    # Get first page
    response_page1 = list_charts(request, page=1, page_size=10)
    assert response_page1.total == 15
    assert len(response_page1.data) == 10
    assert response_page1.total_pages == 2

    # Get second page
    response_page2 = list_charts(request, page=2, page_size=10)
    assert len(response_page2.data) == 5


def test_list_charts_empty_org(orguser, seed_db):
    """Test listing charts when org has no charts"""
    request = mock_request(orguser)

    response = list_charts(request, page=1, page_size=10)

    assert response.total == 0
    assert len(response.data) == 0


# ================================================================================
# Test get_chart endpoint
# ================================================================================


def test_get_chart_success(orguser, sample_chart, seed_db):
    """Test successfully getting a single chart"""
    request = mock_request(orguser)

    response = get_chart(request, chart_id=sample_chart.id)

    assert response.id == sample_chart.id
    assert response.title == "Test Chart"
    assert response.chart_type == "bar"
    assert response.schema_name == "public"


def test_get_chart_not_found(orguser, seed_db):
    """Test getting a non-existent chart"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_chart(request, chart_id=99999)

    assert excinfo.value.status_code == 404


def test_get_chart_wrong_org(orguser, seed_db):
    """Test getting a chart from another org"""
    # Create another org and chart
    other_org = Org.objects.create(name="Other Org", slug="other-org")
    other_user = User.objects.create(username="otheruser", email="other@test.com")
    other_orguser = OrgUser.objects.create(
        user=other_user,
        org=other_org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    other_chart = Chart.objects.create(
        title="Other Chart",
        chart_type="bar",
        computation_type="raw",
        schema_name="public",
        table_name="test",
        extra_config={},
        created_by=other_orguser,
        org=other_org,
    )

    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_chart(request, chart_id=other_chart.id)

    assert excinfo.value.status_code == 404

    # Cleanup
    other_chart.delete()
    other_orguser.delete()
    other_user.delete()
    other_org.delete()


# ================================================================================
# Test create_chart endpoint
# ================================================================================


def test_create_chart_success(orguser, org_warehouse, seed_db):
    """Test successfully creating a chart"""
    request = mock_request(orguser)

    payload = ChartCreate(
        title="New Chart",
        description="New Description",
        chart_type="line",
        computation_type="aggregated",
        schema_name="public",
        table_name="sales",
        extra_config={
            "dimension_column": "month",  # Required for aggregated charts
            "metrics": [{"column": "revenue", "aggregation": "sum"}],  # lowercase
        },
    )

    response = create_chart(request, payload)

    assert response.title == "New Chart"
    assert response.chart_type == "line"
    assert response.schema_name == "public"
    assert response.extra_config["dimension_column"] == "month"

    # Cleanup
    Chart.objects.filter(id=response.id).delete()


def test_create_chart_no_warehouse(orguser, seed_db):
    """Test creating a chart when org has no warehouse - chart creation succeeds without warehouse"""
    request = mock_request(orguser)

    payload = ChartCreate(
        title="New Chart",
        chart_type="bar",
        computation_type="raw",
        schema_name="public",
        table_name="test",
        extra_config={
            "x_axis_column": "date",  # Required for bar chart with raw data
            "y_axis_column": "value",
        },
    )

    # Chart creation should succeed even without warehouse
    # Warehouse is only needed when fetching data
    response = create_chart(request, payload)

    assert response.title == "New Chart"
    assert response.chart_type == "bar"

    # Cleanup
    Chart.objects.filter(id=response.id).delete()


def test_create_chart_minimal_config(orguser, org_warehouse, seed_db):
    """Test creating a chart with minimal configuration"""
    request = mock_request(orguser)

    payload = ChartCreate(
        title="Minimal Chart",
        chart_type="number",
        computation_type="aggregated",  # Number charts require aggregated data
        schema_name="public",
        table_name="metrics",
        extra_config={
            "metrics": [{"column": "total_count", "aggregation": "sum"}],  # lowercase
        },
    )

    response = create_chart(request, payload)

    assert response.title == "Minimal Chart"
    assert response.description is None

    # Cleanup
    Chart.objects.filter(id=response.id).delete()


# ================================================================================
# Test update_chart endpoint
# ================================================================================


def test_update_chart_success(orguser, sample_chart, seed_db):
    """Test successfully updating a chart"""
    request = mock_request(orguser)

    payload = ChartUpdate(
        title="Updated Chart",
        description="Updated Description",
    )

    response = update_chart(request, chart_id=sample_chart.id, payload=payload)

    assert response.id == sample_chart.id
    assert response.title == "Updated Chart"
    assert response.description == "Updated Description"
    # Original values should remain
    assert response.chart_type == "bar"


def test_update_chart_partial_update(orguser, sample_chart, seed_db):
    """Test partial update of a chart"""
    request = mock_request(orguser)

    original_title = sample_chart.title

    payload = ChartUpdate(
        description="Only description updated",
    )

    response = update_chart(request, chart_id=sample_chart.id, payload=payload)

    assert response.title == original_title
    assert response.description == "Only description updated"


def test_update_chart_not_found(orguser, seed_db):
    """Test updating a non-existent chart"""
    request = mock_request(orguser)

    payload = ChartUpdate(title="Updated")

    with pytest.raises(HttpError) as excinfo:
        update_chart(request, chart_id=99999, payload=payload)

    assert excinfo.value.status_code == 404


def test_update_chart_extra_config(orguser, sample_chart, seed_db):
    """Test updating chart extra config"""
    request = mock_request(orguser)

    new_config = {
        "x_axis_column": "updated_date",
        "y_axis_column": "updated_count",
        "new_field": "new_value",
    }

    payload = ChartUpdate(extra_config=new_config)

    response = update_chart(request, chart_id=sample_chart.id, payload=payload)

    assert response.extra_config["x_axis_column"] == "updated_date"
    assert response.extra_config["new_field"] == "new_value"


# ================================================================================
# Test delete_chart endpoint
# ================================================================================


def test_delete_chart_success(orguser, sample_chart, seed_db):
    """Test successfully deleting a chart"""
    request = mock_request(orguser)
    chart_id = sample_chart.id

    response = delete_chart(request, chart_id=chart_id)

    assert response.get("success") is True
    assert not Chart.objects.filter(id=chart_id).exists()


def test_delete_chart_not_found(orguser, seed_db):
    """Test deleting a non-existent chart"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        delete_chart(request, chart_id=99999)

    assert excinfo.value.status_code == 404


def test_delete_chart_wrong_org(orguser, seed_db):
    """Test deleting a chart from another org"""
    # Create another org and chart
    other_org = Org.objects.create(name="Other Org", slug="other-org-del")
    other_user = User.objects.create(username="otheruser2", email="other2@test.com")
    other_orguser = OrgUser.objects.create(
        user=other_user,
        org=other_org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    other_chart = Chart.objects.create(
        title="Other Chart",
        chart_type="bar",
        computation_type="raw",
        schema_name="public",
        table_name="test",
        extra_config={},
        created_by=other_orguser,
        org=other_org,
    )

    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        delete_chart(request, chart_id=other_chart.id)

    assert excinfo.value.status_code == 404

    # Cleanup
    other_chart.delete()
    other_orguser.delete()
    other_user.delete()
    other_org.delete()


# ================================================================================
# Test get_chart_data endpoint
# ================================================================================


@patch("ddpui.api.charts_api.generate_chart_data_and_config")
def test_get_chart_data_success(mock_generate, orguser, sample_chart, org_warehouse, seed_db):
    """Test successfully getting chart data"""
    mock_generate.return_value = {
        "data": {"categories": ["A", "B"], "values": [10, 20]},
        "echarts_config": {"type": "bar"},
    }

    request = mock_request(orguser)

    payload = ChartDataPayload(
        chart_type="bar",
        computation_type="aggregated",
        schema_name="public",
        table_name="users",
        dimension_col="category",
        x_axis="date",
        y_axis="count",
    )

    response = get_chart_data(request, payload)

    assert hasattr(response, "data")
    assert hasattr(response, "echarts_config")
    assert response.data["categories"] == ["A", "B"]


@patch("ddpui.api.charts_api.generate_chart_data_and_config")
def test_get_chart_data_with_filters(mock_generate, orguser, org_warehouse, seed_db):
    """Test getting chart data with dashboard filters"""
    mock_generate.return_value = {
        "data": {"filtered": True},
        "echarts_config": {"type": "bar"},
    }

    request = mock_request(orguser)

    payload = ChartDataPayload(
        chart_type="bar",
        computation_type="aggregated",
        schema_name="public",
        table_name="users",
        dimension_col="category",
        dashboard_filters=[{"column": "status", "value": "active"}],
    )

    response = get_chart_data(request, payload)

    assert response.data["filtered"] is True
    mock_generate.assert_called_once()


def test_get_chart_data_no_warehouse(orguser, seed_db):
    """Test getting chart data when org has no warehouse"""
    request = mock_request(orguser)

    payload = ChartDataPayload(
        chart_type="bar",
        computation_type="raw",
        schema_name="public",
        table_name="test",
    )

    with pytest.raises(HttpError) as excinfo:
        get_chart_data(request, payload)

    assert excinfo.value.status_code == 404


@patch("ddpui.api.charts_api.generate_chart_data_and_config")
def test_get_chart_data_error_handling(mock_generate, orguser, org_warehouse, seed_db):
    """Test error handling in get_chart_data"""
    mock_generate.side_effect = Exception("Database error")

    request = mock_request(orguser)

    payload = ChartDataPayload(
        chart_type="bar",
        computation_type="raw",
        schema_name="public",
        table_name="test",
    )

    with pytest.raises(HttpError) as excinfo:
        get_chart_data(request, payload)

    assert excinfo.value.status_code == 500


# ================================================================================
# Test seed data
# ================================================================================


def test_seed_data(seed_db):
    """Test that seed data is loaded correctly"""
    assert Role.objects.count() == 5
    assert RolePermission.objects.count() > 5
    assert Permission.objects.count() > 5
