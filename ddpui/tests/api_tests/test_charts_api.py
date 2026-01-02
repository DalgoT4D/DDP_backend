"""API Tests for Charts endpoints

Tests the full request → response cycle:
1. list_charts - pagination, search, filter
2. get_chart - success, not found, wrong org
3. create_chart - success, validation errors
4. update_chart - success, partial, not found
5. delete_chart - success, not found, wrong org
6. bulk_delete_charts - success, partial
7. get_chart_dashboards - success
8. get_chart_data - success, no warehouse
"""

import os
import django
from unittest.mock import patch, MagicMock
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.visualization import Chart
from ddpui.models.dashboard import Dashboard, DashboardComponentType
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.charts_api import (
    list_charts,
    get_chart,
    create_chart,
    update_chart,
    delete_chart,
    bulk_delete_charts,
    get_chart_dashboards,
    get_chart_data,
    BulkDeleteRequest,
)
from ddpui.schemas.chart_schema import ChartCreate, ChartUpdate, ChartDataPayload
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    """A django User object"""
    user = User.objects.create(
        username="chartapiuser", email="chartapiuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    """An Org object"""
    org = Org.objects.create(
        name="Chart API Test Org",
        slug="chart-api-test-org",
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
        schema_name="public",
        table_name="users",
        extra_config={
            "dimension_column": "category",
            "metrics": [{"column": "revenue", "aggregation": "sum"}],
        },
        created_by=orguser,
        last_modified_by=orguser,
        org=org,
    )
    yield chart
    try:
        chart.refresh_from_db()
        chart.delete()
    except Chart.DoesNotExist:
        pass


# ================================================================================
# Test list_charts endpoint
# ================================================================================


class TestListCharts:
    """Tests for list_charts endpoint"""

    def test_list_charts_success(self, orguser, sample_chart, seed_db):
        """Test successfully listing charts"""
        request = mock_request(orguser)

        response = list_charts(request, page=1, page_size=10)

        assert response.total == 1
        assert len(response.data) == 1
        assert response.data[0].title == "Test Chart"
        assert response.page == 1
        assert response.page_size == 10

    def test_list_charts_with_search(self, orguser, sample_chart, seed_db):
        """Test listing charts with search filter"""
        request = mock_request(orguser)

        response = list_charts(request, page=1, page_size=10, search="Test")

        assert response.total == 1
        assert response.data[0].title == "Test Chart"

    def test_list_charts_search_no_results(self, orguser, sample_chart, seed_db):
        """Test search with no matching results"""
        request = mock_request(orguser)

        response = list_charts(request, page=1, page_size=10, search="Nonexistent")

        assert response.total == 0
        assert len(response.data) == 0

    def test_list_charts_filter_by_type(self, orguser, sample_chart, seed_db):
        """Test filtering by chart type"""
        request = mock_request(orguser)

        response = list_charts(request, page=1, page_size=10, chart_type="bar")

        assert response.total == 1
        assert response.data[0].chart_type == "bar"

    def test_list_charts_pagination(self, orguser, org, seed_db):
        """Test pagination"""
        # Create multiple charts
        for i in range(15):
            Chart.objects.create(
                title=f"Chart {i}",
                chart_type="bar",
                schema_name="public",
                table_name="test",
                extra_config={},
                created_by=orguser,
                last_modified_by=orguser,
                org=org,
            )

        request = mock_request(orguser)

        response_page1 = list_charts(request, page=1, page_size=10)
        assert response_page1.total == 15
        assert len(response_page1.data) == 10
        assert response_page1.total_pages == 2

        response_page2 = list_charts(request, page=2, page_size=10)
        assert len(response_page2.data) == 5

    def test_list_charts_empty_org(self, orguser, seed_db):
        """Test listing charts when org has no charts"""
        request = mock_request(orguser)

        response = list_charts(request, page=1, page_size=10)

        assert response.total == 0
        assert len(response.data) == 0


# ================================================================================
# Test get_chart endpoint
# ================================================================================


class TestGetChart:
    """Tests for get_chart endpoint"""

    def test_get_chart_success(self, orguser, sample_chart, seed_db):
        """Test successfully getting a chart"""
        request = mock_request(orguser)

        response = get_chart(request, chart_id=sample_chart.id)

        assert response.id == sample_chart.id
        assert response.title == "Test Chart"
        assert response.chart_type == "bar"

    def test_get_chart_not_found(self, orguser, seed_db):
        """Test getting non-existent chart returns 404"""
        request = mock_request(orguser)

        with pytest.raises(HttpError) as excinfo:
            get_chart(request, chart_id=99999)

        assert excinfo.value.status_code == 404

    def test_get_chart_wrong_org(self, orguser, seed_db):
        """Test getting chart from another org returns 404"""
        # Create another org and chart
        other_org = Org.objects.create(name="Other Org", slug="other-org-get")
        other_user = User.objects.create(username="otheruser", email="other@test.com")
        other_orguser = OrgUser.objects.create(
            user=other_user,
            org=other_org,
            new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
        )
        other_chart = Chart.objects.create(
            title="Other Chart",
            chart_type="bar",
            schema_name="public",
            table_name="test",
            extra_config={},
            created_by=other_orguser,
            last_modified_by=other_orguser,
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


class TestCreateChart:
    """Tests for create_chart endpoint"""

    def test_create_chart_success(self, orguser, org_warehouse, seed_db):
        """Test successfully creating a chart"""
        request = mock_request(orguser)

        payload = ChartCreate(
            title="New Chart",
            description="New Description",
            chart_type="line",
            schema_name="public",
            table_name="sales",
            extra_config={
                "dimension_column": "month",
                "metrics": [{"column": "revenue", "aggregation": "sum"}],
            },
        )

        response = create_chart(request, payload)

        assert response.title == "New Chart"
        assert response.chart_type == "line"
        assert response.id is not None

        # Cleanup
        Chart.objects.filter(id=response.id).delete()

    def test_create_chart_minimal(self, orguser, org_warehouse, seed_db):
        """Test creating chart with minimal config"""
        request = mock_request(orguser)

        payload = ChartCreate(
            title="Minimal Chart",
            chart_type="number",
            schema_name="public",
            table_name="metrics",
            extra_config={
                "metrics": [{"column": "total_count", "aggregation": "sum"}],
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


class TestUpdateChart:
    """Tests for update_chart endpoint"""

    def test_update_chart_success(self, orguser, sample_chart, seed_db):
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
        assert response.chart_type == "bar"  # Unchanged

    def test_update_chart_partial(self, orguser, sample_chart, seed_db):
        """Test partial update"""
        request = mock_request(orguser)
        original_title = sample_chart.title

        payload = ChartUpdate(description="Only description updated")

        response = update_chart(request, chart_id=sample_chart.id, payload=payload)

        assert response.title == original_title
        assert response.description == "Only description updated"

    def test_update_chart_not_found(self, orguser, seed_db):
        """Test updating non-existent chart returns 404"""
        request = mock_request(orguser)

        payload = ChartUpdate(title="Updated")

        with pytest.raises(HttpError) as excinfo:
            update_chart(request, chart_id=99999, payload=payload)

        assert excinfo.value.status_code == 404

    def test_update_chart_extra_config(self, orguser, sample_chart, seed_db):
        """Test updating extra_config"""
        request = mock_request(orguser)

        new_config = {
            "dimension_column": "updated_dim",
            "metrics": [{"column": "updated_metric", "aggregation": "avg"}],
        }

        payload = ChartUpdate(extra_config=new_config)

        response = update_chart(request, chart_id=sample_chart.id, payload=payload)

        assert response.extra_config["dimension_column"] == "updated_dim"


# ================================================================================
# Test delete_chart endpoint
# ================================================================================


class TestDeleteChart:
    """Tests for delete_chart endpoint"""

    def test_delete_chart_success(self, orguser, org, seed_db):
        """Test successfully deleting a chart"""
        # Create chart to delete
        chart = Chart.objects.create(
            title="Chart to Delete",
            chart_type="bar",
            schema_name="public",
            table_name="users",
            extra_config={},
            created_by=orguser,
            last_modified_by=orguser,
            org=org,
        )
        chart_id = chart.id

        request = mock_request(orguser)

        response = delete_chart(request, chart_id=chart_id)

        assert response.get("success") is True
        assert not Chart.objects.filter(id=chart_id).exists()

    def test_delete_chart_not_found(self, orguser, seed_db):
        """Test deleting non-existent chart returns 404"""
        request = mock_request(orguser)

        with pytest.raises(HttpError) as excinfo:
            delete_chart(request, chart_id=99999)

        assert excinfo.value.status_code == 404

    def test_delete_chart_wrong_org(self, orguser, seed_db):
        """Test deleting chart from another org returns 404"""
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
            schema_name="public",
            table_name="test",
            extra_config={},
            created_by=other_orguser,
            last_modified_by=other_orguser,
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
# Test bulk_delete_charts endpoint
# ================================================================================


class TestBulkDeleteCharts:
    """Tests for bulk_delete_charts endpoint"""

    def test_bulk_delete_success(self, orguser, org, seed_db):
        """Test successfully bulk deleting charts"""
        # Create charts to delete
        chart_ids = []
        for i in range(3):
            chart = Chart.objects.create(
                title=f"Bulk Chart {i}",
                chart_type="bar",
                schema_name="public",
                table_name="users",
                extra_config={},
                created_by=orguser,
                last_modified_by=orguser,
                org=org,
            )
            chart_ids.append(chart.id)

        request = mock_request(orguser)
        payload = BulkDeleteRequest(chart_ids=chart_ids)

        response = bulk_delete_charts(request, payload)

        assert response["success"] is True
        assert response["deleted_count"] == 3
        assert response["requested_count"] == 3

    def test_bulk_delete_partial(self, orguser, org, seed_db):
        """Test bulk delete with some non-existent charts"""
        chart = Chart.objects.create(
            title="Existing Chart",
            chart_type="bar",
            schema_name="public",
            table_name="users",
            extra_config={},
            created_by=orguser,
            last_modified_by=orguser,
            org=org,
        )

        request = mock_request(orguser)
        payload = BulkDeleteRequest(chart_ids=[chart.id, 99998, 99999])

        response = bulk_delete_charts(request, payload)

        assert response["success"] is True
        assert response["deleted_count"] == 1
        assert 99998 in response["missing_ids"]
        assert 99999 in response["missing_ids"]

    def test_bulk_delete_empty_list(self, orguser, seed_db):
        """Test bulk delete with empty list returns 400"""
        request = mock_request(orguser)
        payload = BulkDeleteRequest(chart_ids=[])

        with pytest.raises(HttpError) as excinfo:
            bulk_delete_charts(request, payload)

        assert excinfo.value.status_code == 400


# ================================================================================
# Test get_chart_dashboards endpoint
# ================================================================================


class TestGetChartDashboards:
    """Tests for get_chart_dashboards endpoint"""

    def test_get_chart_dashboards_with_dashboard(self, orguser, sample_chart, org, seed_db):
        """Test getting dashboards that use a chart"""
        # Create a dashboard that uses the chart
        dashboard = Dashboard.objects.create(
            title="Dashboard with Chart",
            dashboard_type="native",
            grid_columns=12,
            layout_config=[],
            components={
                "1": {
                    "type": DashboardComponentType.CHART.value,
                    "config": {"chartId": sample_chart.id},
                }
            },
            created_by=orguser,
            org=org,
        )

        request = mock_request(orguser)

        response = get_chart_dashboards(request, chart_id=sample_chart.id)

        assert len(response) == 1
        assert response[0]["id"] == dashboard.id
        assert response[0]["title"] == "Dashboard with Chart"

        # Cleanup
        dashboard.delete()

    def test_get_chart_dashboards_no_dashboards(self, orguser, sample_chart, seed_db):
        """Test when chart is not used in any dashboard"""
        request = mock_request(orguser)

        response = get_chart_dashboards(request, chart_id=sample_chart.id)

        assert len(response) == 0

    def test_get_chart_dashboards_not_found(self, orguser, seed_db):
        """Test getting dashboards for non-existent chart returns 404"""
        request = mock_request(orguser)

        with pytest.raises(HttpError) as excinfo:
            get_chart_dashboards(request, chart_id=99999)

        assert excinfo.value.status_code == 404


# ================================================================================
# Test get_chart_data endpoint
# ================================================================================


class TestGetChartData:
    """Tests for get_chart_data endpoint"""

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_get_chart_data_success(self, mock_generate, orguser, org_warehouse, seed_db):
        """Test successfully getting chart data"""
        mock_generate.return_value = {
            "data": {"categories": ["A", "B"], "values": [10, 20]},
            "echarts_config": {"type": "bar"},
        }

        request = mock_request(orguser)

        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="users",
            dimension_col="category",
        )

        response = get_chart_data(request, payload)

        assert response.data["categories"] == ["A", "B"]
        assert "echarts_config" in response.__dict__

    def test_get_chart_data_no_warehouse(self, orguser, seed_db):
        """Test getting chart data when org has no warehouse"""
        request = mock_request(orguser)

        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="test",
        )

        with pytest.raises(HttpError) as excinfo:
            get_chart_data(request, payload)

        assert excinfo.value.status_code == 404


# ================================================================================
# Test seed data
# ================================================================================


def test_seed_data(seed_db):
    """Test that seed data is loaded correctly"""
    assert Role.objects.count() == 5
