"""Test cases for Chart/Visualization models"""

import os
import django
import pytest
from datetime import datetime

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from django.db import IntegrityError
from django.core.exceptions import ValidationError
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.visualization import (
    Chart,
    CHART_TYPE_CHOICES,
    COMPUTATION_TYPE_CHOICES,
    AGGREGATE_FUNC_CHOICES,
)
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db

# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    """A django User object"""
    user = User.objects.create(
        username="chartmodeluser", email="chartmodeluser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    """An Org object"""
    org = Org.objects.create(
        name="Test Org",
        slug="test-org-model",
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


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


# ================================================================================
# Test Chart Model Creation
# ================================================================================


def test_chart_create_success(orguser, org, seed_db):
    """Test creating a chart with valid data"""
    chart = Chart.objects.create(
        title="Test Chart",
        description="Test Description",
        chart_type="bar",
        schema_name="public",
        table_name="users",
        extra_config={"x_axis_column": "date"},
        created_by=orguser,
        org=org,
    )

    assert chart.id is not None
    assert chart.title == "Test Chart"
    assert chart.chart_type == "bar"
    assert chart.extra_config == {"x_axis_column": "date"}
    assert chart.created_at is not None
    assert chart.updated_at is not None

    chart.delete()


def test_chart_create_all_chart_types(orguser, org, seed_db):
    """Test creating charts with all valid chart types"""
    for chart_type, display_name in CHART_TYPE_CHOICES:
        chart = Chart.objects.create(
            title=f"{display_name} Test",
            chart_type=chart_type,
            schema_name="public",
            table_name="test",
            extra_config={},
            created_by=orguser,
            org=org,
        )

        assert chart.chart_type == chart_type
        chart.delete()


def test_chart_create_without_description(orguser, org, seed_db):
    """Test creating a chart without description (optional field)"""
    chart = Chart.objects.create(
        title="No Description Chart",
        chart_type="pie",
        schema_name="public",
        table_name="stats",
        extra_config={},
        created_by=orguser,
        org=org,
    )

    assert chart.description is None
    chart.delete()


def test_chart_create_with_empty_extra_config(orguser, org, seed_db):
    """Test creating a chart with empty extra_config"""
    chart = Chart.objects.create(
        title="Empty Config Chart",
        chart_type="number",
        schema_name="public",
        table_name="metrics",
        extra_config={},
        created_by=orguser,
        org=org,
    )

    assert chart.extra_config == {}
    chart.delete()


def test_chart_create_with_complex_extra_config(orguser, org, seed_db):
    """Test creating a chart with complex extra_config"""
    complex_config = {
        "x_axis_column": "date",
        "y_axis_column": "revenue",
        "metrics": [
            {"column": "sales", "aggregation": "sum"},
            {"column": "orders", "aggregation": "count"},
        ],
        "customizations": {
            "colors": ["#FF0000", "#00FF00"],
            "title": {"show": True},
        },
    }

    chart = Chart.objects.create(
        title="Complex Config Chart",
        chart_type="line",
        schema_name="analytics",
        table_name="sales_data",
        extra_config=complex_config,
        created_by=orguser,
        org=org,
    )

    assert chart.extra_config == complex_config
    assert chart.extra_config["metrics"][0]["aggregation"] == "sum"
    chart.delete()


# ================================================================================
# Test Chart Model String Representation
# ================================================================================


def test_chart_str_representation(orguser, org, seed_db):
    """Test the __str__ method of Chart model"""
    chart = Chart.objects.create(
        title="My Chart",
        chart_type="bar",
        schema_name="public",
        table_name="test",
        extra_config={},
        created_by=orguser,
        org=org,
    )

    assert str(chart) == "My Chart (bar)"
    chart.delete()


# ================================================================================
# Test Chart Model Relationships
# ================================================================================


def test_chart_org_relationship(orguser, org, seed_db):
    """Test the relationship between Chart and Org"""
    chart = Chart.objects.create(
        title="Org Related Chart",
        chart_type="bar",
        schema_name="public",
        table_name="test",
        extra_config={},
        created_by=orguser,
        org=org,
    )

    assert chart.org == org
    assert chart.org.name == "Test Org"

    # Test reverse relationship
    org_charts = Chart.objects.filter(org=org)
    assert chart in org_charts

    chart.delete()


def test_chart_created_by_relationship(orguser, org, seed_db):
    """Test the relationship between Chart and OrgUser (created_by)"""
    chart = Chart.objects.create(
        title="User Related Chart",
        chart_type="bar",
        schema_name="public",
        table_name="test",
        extra_config={},
        created_by=orguser,
        org=org,
    )

    assert chart.created_by == orguser
    assert chart.created_by.user.email == "chartmodeluser@test.com"

    chart.delete()


def test_chart_last_modified_by_relationship(orguser, org, seed_db):
    """Test the last_modified_by relationship"""
    chart = Chart.objects.create(
        title="Modified Chart",
        chart_type="bar",
        schema_name="public",
        table_name="test",
        extra_config={},
        created_by=orguser,
        org=org,
        last_modified_by=orguser,
    )

    assert chart.last_modified_by == orguser
    chart.delete()


def test_chart_last_modified_by_null(orguser, org, seed_db):
    """Test that last_modified_by can be null"""
    chart = Chart.objects.create(
        title="Not Modified Chart",
        chart_type="bar",
        schema_name="public",
        table_name="test",
        extra_config={},
        created_by=orguser,
        org=org,
    )

    assert chart.last_modified_by is None
    chart.delete()


# ================================================================================
# Test Chart Model Updates
# ================================================================================


def test_chart_update_title(orguser, org, seed_db):
    """Test updating chart title"""
    chart = Chart.objects.create(
        title="Original Title",
        chart_type="bar",
        schema_name="public",
        table_name="test",
        extra_config={},
        created_by=orguser,
        org=org,
    )

    original_updated_at = chart.updated_at

    chart.title = "Updated Title"
    chart.save()

    chart.refresh_from_db()
    assert chart.title == "Updated Title"
    assert chart.updated_at >= original_updated_at

    chart.delete()


def test_chart_update_extra_config(orguser, org, seed_db):
    """Test updating chart extra_config"""
    chart = Chart.objects.create(
        title="Config Update Chart",
        chart_type="bar",
        schema_name="public",
        table_name="test",
        extra_config={"original": "value"},
        created_by=orguser,
        org=org,
    )

    chart.extra_config = {"updated": "new_value", "new_field": True}
    chart.save()

    chart.refresh_from_db()
    assert chart.extra_config["updated"] == "new_value"
    assert chart.extra_config["new_field"] is True
    assert "original" not in chart.extra_config

    chart.delete()


# ================================================================================
# Test Chart Model Deletion
# ================================================================================


def test_chart_delete(orguser, org, seed_db):
    """Test deleting a chart"""
    chart = Chart.objects.create(
        title="Delete Me",
        chart_type="bar",
        schema_name="public",
        table_name="test",
        extra_config={},
        created_by=orguser,
        org=org,
    )

    chart_id = chart.id
    chart.delete()

    assert not Chart.objects.filter(id=chart_id).exists()


def test_chart_cascade_delete_on_org_delete(authuser, seed_db):
    """Test that charts are deleted when org is deleted"""
    # Create a new org for this test
    test_org = Org.objects.create(name="Cascade Test Org", slug="cascade-test-org")
    test_orguser = OrgUser.objects.create(
        user=authuser,
        org=test_org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )

    chart = Chart.objects.create(
        title="Cascade Delete Chart",
        chart_type="bar",
        schema_name="public",
        table_name="test",
        extra_config={},
        created_by=test_orguser,
        org=test_org,
    )

    chart_id = chart.id
    test_orguser.delete()
    test_org.delete()

    assert not Chart.objects.filter(id=chart_id).exists()


# ================================================================================
# Test Chart Model Queries
# ================================================================================


def test_chart_filter_by_chart_type(orguser, org, seed_db):
    """Test filtering charts by chart_type"""
    bar_chart = Chart.objects.create(
        title="Bar Chart",
        chart_type="bar",
        schema_name="public",
        table_name="test",
        extra_config={},
        created_by=orguser,
        org=org,
    )

    pie_chart = Chart.objects.create(
        title="Pie Chart",
        chart_type="pie",
        schema_name="public",
        table_name="test",
        extra_config={},
        created_by=orguser,
        org=org,
    )

    bar_charts = Chart.objects.filter(org=org, chart_type="bar")
    pie_charts = Chart.objects.filter(org=org, chart_type="pie")

    assert bar_charts.count() == 1
    assert pie_charts.count() == 1
    assert bar_charts.first().title == "Bar Chart"
    assert pie_charts.first().title == "Pie Chart"

    bar_chart.delete()
    pie_chart.delete()


def test_chart_filter_by_schema_and_table(orguser, org, seed_db):
    """Test filtering charts by schema and table name"""
    chart1 = Chart.objects.create(
        title="Public Users Chart",
        chart_type="bar",
        schema_name="public",
        table_name="users",
        extra_config={},
        created_by=orguser,
        org=org,
    )

    chart2 = Chart.objects.create(
        title="Analytics Orders Chart",
        chart_type="line",
        schema_name="analytics",
        table_name="orders",
        extra_config={},
        created_by=orguser,
        org=org,
    )

    public_charts = Chart.objects.filter(org=org, schema_name="public")
    analytics_charts = Chart.objects.filter(org=org, schema_name="analytics")

    assert public_charts.count() == 1
    assert analytics_charts.count() == 1

    chart1.delete()
    chart2.delete()


# ================================================================================
# Test Chart Type and Aggregate Function Choices
# ================================================================================


def test_chart_type_choices():
    """Test that chart type choices are correctly defined"""
    expected_types = ["bar", "pie", "line", "number", "map"]
    actual_types = [choice[0] for choice in CHART_TYPE_CHOICES]

    assert set(expected_types) == set(actual_types)


def test_aggregate_func_choices():
    """Test that aggregate function choices are correctly defined"""
    expected_funcs = ["sum", "avg", "count", "min", "max", "count_distinct"]
    actual_funcs = [choice[0] for choice in AGGREGATE_FUNC_CHOICES]

    assert set(expected_funcs) == set(actual_funcs)


# ================================================================================
# Test Seed Data
# ================================================================================


def test_seed_data(seed_db):
    """Test that seed data is loaded correctly"""
    assert Role.objects.count() == 5
