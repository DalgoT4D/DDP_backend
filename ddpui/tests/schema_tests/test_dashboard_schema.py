"""Schema Tests for Dashboard schemas

Tests schema-specific functionality NOT tested by API tests:
1. LockResponse schema
2. Share schemas (DashboardShareToggle, DashboardShareResponse, DashboardShareStatus)
3. Landing page schemas
4. FilterOptionResponse / FilterOptionsResponse
5. Schema serialization (.dict())
6. Edge cases (unicode, nested structures)
"""

import pytest
from datetime import datetime
from pydantic import ValidationError

from ddpui.schemas.dashboard_schema import (
    DashboardCreate,
    DashboardUpdate,
    DashboardResponse,
    DashboardFilterResponse,
    FilterCreate,
    FilterUpdate,
    FilterOptionResponse,
    FilterOptionsResponse,
    LockResponse,
    DashboardShareToggle,
    DashboardShareResponse,
    DashboardShareStatus,
    LandingPageResponse,
    LandingPageResolveResponse,
)


# ================================================================================
# Test LockResponse Schema (NOT tested by API tests)
# ================================================================================


class TestLockResponseSchema:
    """Tests for LockResponse schema"""

    def test_lock_response_valid(self):
        """Test valid lock response"""
        now = datetime.now()
        response = LockResponse(
            lock_token="abc-123-def-456",
            expires_at=now,
            locked_by="user@example.com",
        )

        assert response.lock_token == "abc-123-def-456"
        assert response.expires_at == now
        assert response.locked_by == "user@example.com"

    def test_lock_response_missing_required_fields(self):
        """Test that all fields are required"""
        with pytest.raises(ValidationError):
            LockResponse(lock_token="token")  # Missing expires_at and locked_by


# ================================================================================
# Test Share Schemas (NOT tested by API tests)
# ================================================================================


class TestShareSchemas:
    """Tests for dashboard sharing schemas"""

    def test_share_toggle_true(self):
        """Test share toggle with is_public=True"""
        toggle = DashboardShareToggle(is_public=True)
        assert toggle.is_public is True

    def test_share_toggle_false(self):
        """Test share toggle with is_public=False"""
        toggle = DashboardShareToggle(is_public=False)
        assert toggle.is_public is False

    def test_share_response_public(self):
        """Test share response when public"""
        response = DashboardShareResponse(
            is_public=True,
            public_url="https://example.com/dashboard/abc123",
            public_share_token="abc123",
            message="Dashboard is now public",
        )

        assert response.is_public is True
        assert response.public_url == "https://example.com/dashboard/abc123"
        assert response.public_share_token == "abc123"

    def test_share_response_private(self):
        """Test share response when private"""
        response = DashboardShareResponse(
            is_public=False,
            public_url=None,
            public_share_token=None,
            message="Dashboard is now private",
        )

        assert response.is_public is False
        assert response.public_url is None
        assert response.public_share_token is None

    def test_share_status_with_all_fields(self):
        """Test share status response with all fields"""
        now = datetime.now()
        status = DashboardShareStatus(
            is_public=True,
            public_url="https://example.com/dashboard/abc",
            public_access_count=100,
            last_public_accessed=now,
            public_shared_at=now,
        )

        assert status.is_public is True
        assert status.public_access_count == 100
        assert status.last_public_accessed == now

    def test_share_status_private(self):
        """Test share status when private"""
        status = DashboardShareStatus(
            is_public=False,
            public_url=None,
            public_access_count=0,
            last_public_accessed=None,
            public_shared_at=None,
        )

        assert status.is_public is False
        assert status.public_access_count == 0


# ================================================================================
# Test Landing Page Schemas (NOT tested by API tests)
# ================================================================================


class TestLandingPageSchemas:
    """Tests for landing page schemas"""

    def test_landing_page_response_success(self):
        """Test landing page response success"""
        response = LandingPageResponse(
            success=True,
            message="Landing page set successfully",
        )

        assert response.success is True
        assert "successfully" in response.message

    def test_landing_page_response_failure(self):
        """Test landing page response failure"""
        response = LandingPageResponse(
            success=False,
            message="Dashboard not found",
        )

        assert response.success is False

    def test_landing_page_response_default_message(self):
        """Test landing page response default message"""
        response = LandingPageResponse(success=True)

        assert response.message == ""

    def test_landing_page_resolve_personal(self):
        """Test resolved landing page from personal preference"""
        response = LandingPageResolveResponse(
            dashboard_id=1,
            dashboard_title="My Dashboard",
            dashboard_type="native",
            source="personal",
        )

        assert response.dashboard_id == 1
        assert response.source == "personal"

    def test_landing_page_resolve_org_default(self):
        """Test resolved landing page from org default"""
        response = LandingPageResolveResponse(
            dashboard_id=2,
            dashboard_title="Org Dashboard",
            dashboard_type="native",
            source="org_default",
        )

        assert response.source == "org_default"

    def test_landing_page_resolve_none(self):
        """Test resolved landing page when none set"""
        response = LandingPageResolveResponse(
            dashboard_id=None,
            dashboard_title=None,
            dashboard_type=None,
            source="none",
        )

        assert response.dashboard_id is None
        assert response.source == "none"


# ================================================================================
# Test FilterOption Schemas (NOT tested by API tests)
# ================================================================================


class TestFilterOptionSchemas:
    """Tests for filter option schemas"""

    def test_filter_option_response_with_count(self):
        """Test filter option with count"""
        option = FilterOptionResponse(
            label="Active",
            value="active",
            count=42,
        )

        assert option.label == "Active"
        assert option.value == "active"
        assert option.count == 42

    def test_filter_option_response_without_count(self):
        """Test filter option without optional count"""
        option = FilterOptionResponse(label="Test", value="test")

        assert option.label == "Test"
        assert option.count is None

    def test_filter_options_response(self):
        """Test filter options response with multiple options"""
        options = FilterOptionsResponse(
            options=[
                FilterOptionResponse(label="A", value="a", count=10),
                FilterOptionResponse(label="B", value="b", count=20),
            ],
            total_count=30,
        )

        assert len(options.options) == 2
        assert options.total_count == 30


# ================================================================================
# Test Schema Serialization (NOT tested by API tests)
# ================================================================================


class TestSchemaSerialization:
    """Tests for schema serialization (.dict())"""

    def test_dashboard_create_to_dict(self):
        """Test DashboardCreate can be converted to dict"""
        dashboard = DashboardCreate(
            title="Test Dashboard",
            description="Description",
            grid_columns=24,
        )

        data = dashboard.dict()

        assert isinstance(data, dict)
        assert data["title"] == "Test Dashboard"
        assert data["grid_columns"] == 24

    def test_dashboard_update_to_dict_excludes_none(self):
        """Test DashboardUpdate dict excludes None values when specified"""
        update = DashboardUpdate(title="New Title")

        data = update.dict(exclude_none=True)

        assert "title" in data
        assert "description" not in data
        assert "grid_columns" not in data

    def test_filter_create_to_dict(self):
        """Test FilterCreate can be converted to dict"""
        filter_create = FilterCreate(
            name="Test Filter",
            filter_type="value",
            schema_name="public",
            table_name="users",
            column_name="status",
        )

        data = filter_create.dict()

        assert data["filter_type"] == "value"
        assert data["column_name"] == "status"


# ================================================================================
# Test Edge Cases (NOT tested by API tests)
# ================================================================================


class TestEdgeCases:
    """Tests for edge cases and special values"""

    def test_dashboard_create_with_unicode_title(self):
        """Test dashboard creation with unicode characters"""
        dashboard = DashboardCreate(title="日本語ダッシュボード 📊")

        assert dashboard.title == "日本語ダッシュボード 📊"

    def test_dashboard_update_with_complex_components(self):
        """Test dashboard update with complex nested components"""
        components = {
            "chart-1": {
                "type": "chart",
                "config": {
                    "chartId": 1,
                    "settings": {
                        "showLegend": True,
                        "colors": ["#ff0000", "#00ff00"],
                    },
                },
            },
            "text-1": {
                "type": "text",
                "config": {"content": "Hello World", "fontSize": 16},
            },
        }

        update = DashboardUpdate(components=components)

        assert update.components["chart-1"]["config"]["settings"]["showLegend"] is True
        assert update.components["text-1"]["config"]["fontSize"] == 16

    def test_filter_response_with_nested_settings(self):
        """Test filter response with deeply nested settings"""
        now = datetime.now()
        settings = {
            "range": {"min": 0, "max": 100},
            "display": {"format": "currency", "prefix": "$"},
        }

        response = DashboardFilterResponse(
            id=1,
            dashboard_id=1,
            name="Price Filter",
            filter_type="numerical",
            schema_name="public",
            table_name="products",
            column_name="price",
            settings=settings,
            order=0,
            created_at=now,
            updated_at=now,
        )

        assert response.settings["range"]["min"] == 0
        assert response.settings["display"]["prefix"] == "$"

    def test_dashboard_response_with_filters(self):
        """Test dashboard response with embedded filters"""
        now = datetime.now()
        filter_resp = DashboardFilterResponse(
            id=1,
            dashboard_id=1,
            name="Status",
            filter_type="value",
            schema_name="public",
            table_name="orders",
            column_name="status",
            settings={},
            order=0,
            created_at=now,
            updated_at=now,
        )

        response = DashboardResponse(
            id=1,
            title="Dashboard with Filters",
            description=None,
            dashboard_type="native",
            grid_columns=12,
            target_screen_size="desktop",
            filter_layout="horizontal",
            layout_config=[],
            components={},
            is_published=False,
            published_at=None,
            is_locked=False,
            locked_by=None,
            created_by="user@example.com",
            org_id=1,
            last_modified_by=None,
            created_at=now,
            updated_at=now,
            filters=[filter_resp],
        )

        assert len(response.filters) == 1
        assert response.filters[0].name == "Status"
