"""Service Tests for DashboardService

Tests business logic NOT covered by API tests:
1. update_dashboard lock checking (locked by other user)
2. delete_dashboard permission checks (creator-only, org default, landing page, locked)
3. create_filter invalid type validation
4. Exception classes
5. Data classes (DashboardData, FilterData)
"""

import os
import django
import pytest
from datetime import timedelta
from unittest.mock import patch, MagicMock

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from django.utils import timezone
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.dashboard import Dashboard, DashboardFilter, DashboardLock
from ddpui.auth import ACCOUNT_MANAGER_ROLE, ANALYST_ROLE
from ddpui.services.dashboard_service import (
    DashboardService,
    DashboardData,
    FilterData,
    DashboardNotFoundError,
    DashboardLockedError,
    DashboardPermissionError,
    DashboardServiceError,
    FilterNotFoundError,
    FilterValidationError,
    WidgetImageValidationError,
    WidgetImagePermissionError,
    WidgetImageStorageError,
    upload_widget_image,
    delete_widget_image,
)
from ddpui.schemas.dashboard_schema import DashboardCreate, DashboardUpdate, DashboardTabSchema
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    """A django User object"""
    user = User.objects.create(
        username="dashserviceuser", email="dashserviceuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def authuser2():
    """A second django User object for permission testing"""
    user = User.objects.create(
        username="dashserviceuser2", email="dashserviceuser2@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    """An Org object"""
    org = Org.objects.create(
        name="Dashboard Service Test Org",
        slug="dash-svc-test-org",  # max_length=20
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


@pytest.fixture
def orguser2(authuser2, org):
    """A second OrgUser for permission testing"""
    orguser = OrgUser.objects.create(
        user=authuser2,
        org=org,
        new_role=Role.objects.filter(slug=ANALYST_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def sample_dashboard(orguser, org):
    """A sample dashboard for testing"""
    dashboard = Dashboard.objects.create(
        title="Test Dashboard",
        description="Test Description",
        dashboard_type="native",
        grid_columns=12,
        created_by=orguser,
        org=org,
    )
    yield dashboard
    try:
        dashboard.refresh_from_db()
        dashboard.delete()
    except Dashboard.DoesNotExist:
        pass


# ================================================================================
# Test update_dashboard lock checking (NOT in API tests)
# ================================================================================


class TestUpdateDashboardLockChecking:
    """Tests for DashboardService.update_dashboard() lock behavior"""

    def test_update_dashboard_locked_by_other_user(
        self, orguser, orguser2, sample_dashboard, seed_db
    ):
        """Test that updating locked dashboard raises error"""
        # Create a lock by another user
        DashboardLock.objects.create(
            dashboard=sample_dashboard,
            locked_by=orguser2,
            lock_token="test-token",
            expires_at=timezone.now() + timedelta(minutes=2),
        )

        with pytest.raises(DashboardLockedError) as excinfo:
            DashboardService.update_dashboard(
                sample_dashboard.id,
                orguser.org,
                orguser,
                DashboardUpdate(title="New Title"),
            )

        assert orguser2.user.email in excinfo.value.locked_by_email

        # Cleanup
        DashboardLock.objects.filter(dashboard=sample_dashboard).delete()

    def test_update_dashboard_locked_by_same_user_succeeds(
        self, orguser, sample_dashboard, seed_db
    ):
        """Test that updating own locked dashboard succeeds"""
        DashboardLock.objects.create(
            dashboard=sample_dashboard,
            locked_by=orguser,
            lock_token="test-token",
            expires_at=timezone.now() + timedelta(minutes=2),
        )

        dashboard = DashboardService.update_dashboard(
            sample_dashboard.id,
            orguser.org,
            orguser,
            DashboardUpdate(title="Updated by Lock Owner"),
        )

        assert dashboard.title == "Updated by Lock Owner"

        # Cleanup
        DashboardLock.objects.filter(dashboard=sample_dashboard).delete()

    def test_update_dashboard_expired_lock_succeeds(
        self, orguser, orguser2, sample_dashboard, seed_db
    ):
        """Test that updating with expired lock succeeds"""
        DashboardLock.objects.create(
            dashboard=sample_dashboard,
            locked_by=orguser2,
            lock_token="test-token",
            expires_at=timezone.now() - timedelta(minutes=1),  # Expired
        )

        dashboard = DashboardService.update_dashboard(
            sample_dashboard.id,
            orguser.org,
            orguser,
            DashboardUpdate(title="Updated After Expiry"),
        )

        assert dashboard.title == "Updated After Expiry"

        # Cleanup
        DashboardLock.objects.filter(dashboard=sample_dashboard).delete()


# ================================================================================
# Test delete_dashboard permission checks (NOT in API tests)
# ================================================================================


class TestDeleteDashboardPermissions:
    """Tests for DashboardService.delete_dashboard() permission logic"""

    def test_delete_dashboard_permission_denied_not_creator(self, orguser, orguser2, org, seed_db):
        """Test that only creator can delete dashboard"""
        dashboard = Dashboard.objects.create(
            title="Protected Dashboard",
            dashboard_type="native",
            grid_columns=12,
            created_by=orguser,
            org=org,
        )

        with pytest.raises(DashboardPermissionError) as excinfo:
            DashboardService.delete_dashboard(dashboard.id, org, orguser2)

        assert "Only the owner or an admin can delete this dashboard." in excinfo.value.message

        # Cleanup
        dashboard.delete()

    def test_delete_dashboard_org_default_fails(self, orguser, org, seed_db):
        """Test that org default dashboard cannot be deleted"""
        dashboard = Dashboard.objects.create(
            title="Org Default Dashboard",
            dashboard_type="native",
            grid_columns=12,
            is_org_default=True,
            created_by=orguser,
            org=org,
        )

        with pytest.raises(DashboardPermissionError) as excinfo:
            DashboardService.delete_dashboard(dashboard.id, org, orguser)

        assert "default dashboard" in excinfo.value.message.lower()

        # Cleanup
        dashboard.delete()

    def test_delete_dashboard_with_landing_page_fails(self, orguser, org, seed_db):
        """Test that dashboard set as landing page cannot be deleted"""
        dashboard = Dashboard.objects.create(
            title="Landing Page Dashboard",
            dashboard_type="native",
            grid_columns=12,
            created_by=orguser,
            org=org,
        )
        orguser.landing_dashboard = dashboard
        orguser.save()

        with pytest.raises(DashboardPermissionError) as excinfo:
            DashboardService.delete_dashboard(dashboard.id, org, orguser)

        assert "landing page" in excinfo.value.message.lower()

        # Cleanup
        orguser.landing_dashboard = None
        orguser.save()
        dashboard.delete()

    def test_delete_dashboard_locked_fails(self, orguser, orguser2, org, seed_db):
        """Test that locked dashboard cannot be deleted"""
        dashboard = Dashboard.objects.create(
            title="Locked Dashboard",
            dashboard_type="native",
            grid_columns=12,
            created_by=orguser,
            org=org,
        )
        DashboardLock.objects.create(
            dashboard=dashboard,
            locked_by=orguser2,
            lock_token="test-token",
            expires_at=timezone.now() + timedelta(minutes=2),
        )

        with pytest.raises(DashboardLockedError):
            DashboardService.delete_dashboard(dashboard.id, org, orguser)

        # Cleanup
        DashboardLock.objects.filter(dashboard=dashboard).delete()
        dashboard.delete()


# ================================================================================
# Test create_filter validation (NOT in API tests)
# ================================================================================


class TestCreateFilterValidation:
    """Tests for DashboardService.create_filter() validation"""

    def test_create_filter_invalid_type(self, sample_dashboard, org, seed_db):
        """Test creating filter with invalid type raises error"""
        filter_data = FilterData(
            filter_type="invalid_type",
            schema_name="public",
            table_name="users",
            column_name="status",
        )

        with pytest.raises(FilterValidationError) as excinfo:
            DashboardService.create_filter(sample_dashboard.id, org, filter_data)

        assert "invalid_type" in excinfo.value.message.lower()


# ================================================================================
# Test Exception Classes
# ================================================================================


class TestExceptionClasses:
    """Tests for custom exception classes"""

    def test_dashboard_not_found_error(self):
        """Test DashboardNotFoundError attributes"""
        error = DashboardNotFoundError(123)

        assert error.dashboard_id == 123
        assert error.error_code == "DASHBOARD_NOT_FOUND"
        assert "123" in error.message

    def test_dashboard_locked_error(self):
        """Test DashboardLockedError attributes"""
        error = DashboardLockedError("user@example.com")

        assert error.locked_by_email == "user@example.com"
        assert error.error_code == "DASHBOARD_LOCKED"
        assert "user@example.com" in error.message

    def test_dashboard_permission_error(self):
        """Test DashboardPermissionError attributes"""
        error = DashboardPermissionError("You cannot do this")

        assert error.error_code == "PERMISSION_DENIED"
        assert error.message == "You cannot do this"

    def test_dashboard_permission_error_default_message(self):
        """Test DashboardPermissionError default message"""
        error = DashboardPermissionError()

        assert error.message == "Permission denied"

    def test_filter_not_found_error(self):
        """Test FilterNotFoundError attributes"""
        error = FilterNotFoundError(456)

        assert error.filter_id == 456
        assert error.error_code == "FILTER_NOT_FOUND"
        assert "456" in error.message

    def test_filter_validation_error(self):
        """Test FilterValidationError attributes"""
        error = FilterValidationError("Invalid filter type")

        assert error.error_code == "FILTER_VALIDATION_ERROR"
        assert error.message == "Invalid filter type"


# ================================================================================
# Test Data Classes
# ================================================================================


class TestDataClasses:
    """Tests for data classes"""

    def test_dashboard_data_all_fields(self):
        """Test DashboardData with all fields"""
        data = DashboardData(
            title="Test Dashboard",
            description="Description",
            grid_columns=24,
        )

        assert data.title == "Test Dashboard"
        assert data.description == "Description"
        assert data.grid_columns == 24

    def test_dashboard_data_optional_fields(self):
        """Test DashboardData without optional fields"""
        data = DashboardData(title="Minimal Dashboard")

        assert data.title == "Minimal Dashboard"
        assert data.description is None
        assert data.grid_columns == 12  # Default

    def test_filter_data_all_fields(self):
        """Test FilterData with all fields"""
        data = FilterData(
            filter_type="value",
            schema_name="public",
            table_name="users",
            column_name="status",
            name="Status Filter",
            settings={"key": "value"},
            order=1,
        )

        assert data.filter_type == "value"
        assert data.name == "Status Filter"
        assert data.order == 1

    def test_filter_data_optional_fields(self):
        """Test FilterData without optional fields"""
        data = FilterData(
            filter_type="value",
            schema_name="public",
            table_name="users",
            column_name="status",
        )

        assert data.name is None
        assert data.settings is None
        assert data.order == 0  # Default


# ================================================================================
# Test resolve_dashboard_filters_for_chart
# ================================================================================


class TestResolveDashboardFiltersForChart:
    """Tests for DashboardService.resolve_dashboard_filters_for_chart()"""

    def _make_filter_def(
        self,
        filter_id,
        column_name,
        filter_type="value",
        schema_name="public",
        table_name="orders",
        settings=None,
    ):
        """Helper to build a filter definition dict (same shape as DashboardFilter.to_json())"""
        return {
            "id": filter_id,
            "dashboard_id": 1,
            "name": f"Filter {filter_id}",
            "filter_type": filter_type,
            "schema_name": schema_name,
            "table_name": table_name,
            "column_name": column_name,
            "settings": settings or {},
            "order": 0,
            "created_at": None,
            "updated_at": None,
        }

    def test_resolves_matching_filter_with_warehouse_client(self):
        """With warehouse_client, resolves filter when column_exists returns True"""
        warehouse_client = MagicMock()
        warehouse_client.column_exists.return_value = True

        filter_defs = [self._make_filter_def(1, "status")]
        result = DashboardService.resolve_dashboard_filters_for_chart(
            {"1": "active"}, filter_defs, "public", "orders", warehouse_client
        )

        assert result is not None
        assert len(result) == 1
        assert result[0]["column"] == "status"
        assert result[0]["type"] == "value"
        assert result[0]["value"] == "active"
        warehouse_client.column_exists.assert_called_once_with("public", "orders", "status")

    def test_skips_filter_when_column_not_exists(self):
        """With warehouse_client, skips filter when column_exists returns False"""
        warehouse_client = MagicMock()
        warehouse_client.column_exists.return_value = False

        filter_defs = [self._make_filter_def(1, "status")]
        result = DashboardService.resolve_dashboard_filters_for_chart(
            {"1": "active"}, filter_defs, "public", "orders", warehouse_client
        )

        assert result is None

    def test_resolves_matching_filter_with_schema_table_match(self):
        """Without warehouse_client, resolves filter when schema/table matches"""
        filter_defs = [
            self._make_filter_def(1, "status", schema_name="public", table_name="orders")
        ]
        result = DashboardService.resolve_dashboard_filters_for_chart(
            {"1": "active"}, filter_defs, "public", "orders"
        )

        assert result is not None
        assert len(result) == 1
        assert result[0]["column"] == "status"

    def test_skips_filter_when_schema_table_mismatch(self):
        """Without warehouse_client, skips filter when schema/table doesn't match"""
        filter_defs = [self._make_filter_def(1, "status", schema_name="public", table_name="users")]
        result = DashboardService.resolve_dashboard_filters_for_chart(
            {"1": "active"}, filter_defs, "public", "orders"
        )

        assert result is None

    def test_skips_none_values(self):
        """Filters with None values are skipped"""
        warehouse_client = MagicMock()
        warehouse_client.column_exists.return_value = True

        filter_defs = [self._make_filter_def(1, "status")]
        result = DashboardService.resolve_dashboard_filters_for_chart(
            {"1": None}, filter_defs, "public", "orders", warehouse_client
        )

        assert result is None
        warehouse_client.column_exists.assert_not_called()

    def test_skips_unknown_filter_ids(self):
        """Filter IDs not found in definitions are skipped"""
        warehouse_client = MagicMock()

        filter_defs = [self._make_filter_def(1, "status")]
        result = DashboardService.resolve_dashboard_filters_for_chart(
            {"999": "active"}, filter_defs, "public", "orders", warehouse_client
        )

        assert result is None

    def test_multiple_filters_mixed_results(self):
        """Multiple filters: some resolve, some don't"""
        warehouse_client = MagicMock()
        warehouse_client.column_exists.side_effect = [True, False]

        filter_defs = [
            self._make_filter_def(1, "status"),
            self._make_filter_def(2, "missing_col"),
        ]
        result = DashboardService.resolve_dashboard_filters_for_chart(
            {"1": "active", "2": "value"}, filter_defs, "public", "orders", warehouse_client
        )

        assert result is not None
        assert len(result) == 1
        assert result[0]["filter_id"] == "1"

    def test_settings_included_in_result(self):
        """Filter settings are passed through to resolved dict"""
        warehouse_client = MagicMock()
        warehouse_client.column_exists.return_value = True

        filter_defs = [
            self._make_filter_def(
                1,
                "created_at",
                filter_type="datetime",
                settings={"default_start_date": "2025-01-01"},
            )
        ]
        result = DashboardService.resolve_dashboard_filters_for_chart(
            {"1": {"start_date": "2025-01-01", "end_date": "2025-01-31"}},
            filter_defs,
            "public",
            "orders",
            warehouse_client,
        )

        assert result is not None
        assert result[0]["settings"] == {"default_start_date": "2025-01-01"}
        assert result[0]["type"] == "datetime"

    def test_empty_filter_values(self):
        """Empty filter_values returns None"""
        result = DashboardService.resolve_dashboard_filters_for_chart(
            {}, [self._make_filter_def(1, "status")], "public", "orders"
        )

        assert result is None

    def test_empty_filter_definitions(self):
        """Empty filter_definitions returns None (all IDs unmatched)"""
        result = DashboardService.resolve_dashboard_filters_for_chart(
            {"1": "active"}, [], "public", "orders"
        )

        assert result is None

    def test_non_dict_filter_values_raises(self):
        """Non-dict filter_values raises ValueError"""
        with pytest.raises(ValueError, match="filter_values must be a dict"):
            DashboardService.resolve_dashboard_filters_for_chart(
                [1, 2, 3], [self._make_filter_def(1, "status")], "public", "orders"
            )

    def test_string_filter_values_raises(self):
        """String filter_values raises ValueError"""
        with pytest.raises(ValueError, match="filter_values must be a dict"):
            DashboardService.resolve_dashboard_filters_for_chart(
                "not a dict", [self._make_filter_def(1, "status")], "public", "orders"
            )


# ================================================================================
# Test create_dashboard default tab (NEW in feature/dashboard_tabs)
# ================================================================================


class TestCreateDashboardDefaultTab:
    """Tests for DashboardService.create_dashboard() default tab generation"""

    def test_create_dashboard_has_default_tab(self, orguser, seed_db):
        """Test that a new dashboard is created with exactly one default tab"""
        dashboard = DashboardService.create_dashboard(
            DashboardCreate(title="Tab Test Dashboard"),
            orguser,
        )

        assert len(dashboard.tabs) == 1

        # Cleanup
        dashboard.delete()

    def test_create_dashboard_default_tab_structure(self, orguser, seed_db):
        """Test that the default tab has the correct structure"""
        dashboard = DashboardService.create_dashboard(
            DashboardCreate(title="Tab Structure Dashboard"),
            orguser,
        )

        tab = dashboard.tabs[0]
        assert tab["title"] == "Untitled Tab 1"
        assert tab["id"].startswith("tab-")
        assert tab["layout_config"] == []
        assert tab["components"] == {}

        # Cleanup
        dashboard.delete()


# ================================================================================
# Test update_dashboard tabs (NEW in feature/dashboard_tabs)
# ================================================================================


class TestUpdateDashboardTabs:
    """Tests for DashboardService.update_dashboard() tabs handling"""

    def test_update_dashboard_tabs_saves_correctly(self, orguser, sample_dashboard, seed_db):
        """Test that providing tabs in update saves them as dicts"""
        new_tabs = [
            DashboardTabSchema(
                id="tab-111",
                title="My Tab",
                layout_config=[{"i": "chart-1", "x": 0, "y": 0, "w": 4, "h": 3}],
                components={"chart-1": {"type": "chart"}},
            )
        ]

        updated = DashboardService.update_dashboard(
            sample_dashboard.id,
            orguser.org,
            orguser,
            DashboardUpdate(tabs=new_tabs),
        )

        assert len(updated.tabs) == 1
        assert updated.tabs[0]["id"] == "tab-111"
        assert updated.tabs[0]["title"] == "My Tab"
        assert updated.tabs[0]["layout_config"] == [
            {"i": "chart-1", "x": 0, "y": 0, "w": 4, "h": 3}
        ]
        assert updated.tabs[0]["components"] == {"chart-1": {"type": "chart"}}

    def test_update_dashboard_without_tabs_preserves_existing(
        self, orguser, sample_dashboard, seed_db
    ):
        """Test that omitting tabs in update does not overwrite existing tabs"""
        sample_dashboard.tabs = [
            {"id": "tab-existing", "title": "Existing Tab", "layout_config": [], "components": {}}
        ]
        sample_dashboard.save()

        updated = DashboardService.update_dashboard(
            sample_dashboard.id,
            orguser.org,
            orguser,
            DashboardUpdate(title="New Title Only"),
        )

        assert updated.title == "New Title Only"
        assert len(updated.tabs) == 1
        assert updated.tabs[0]["id"] == "tab-existing"


# ================================================================================
# Test upload_widget_image / delete_widget_image (dashboard text/image widgets)
# ================================================================================


class TestUploadWidgetImage:
    """Tests for upload_widget_image()"""

    def test_upload_widget_image_invalid_content_type(self, org):
        """Test that a disallowed content type is rejected before touching S3"""
        with pytest.raises(WidgetImageValidationError) as excinfo:
            upload_widget_image(b"not-an-image", "application/pdf", org)

        assert "Invalid file type" in excinfo.value.message

    def test_upload_widget_image_oversized(self, org, monkeypatch):
        """Test that a file over the 5MB limit is rejected before touching S3"""
        monkeypatch.setenv("S3_IMAGES_BUCKET", "test-bucket")
        oversized_bytes = b"0" * (5 * 1024 * 1024 + 1)

        with pytest.raises(WidgetImageValidationError) as excinfo:
            upload_widget_image(oversized_bytes, "image/png", org)

        assert "5MB" in excinfo.value.message

    def test_upload_widget_image_missing_bucket_env(self, org, monkeypatch):
        """Test that a missing S3_IMAGES_BUCKET env var surfaces as a storage error"""
        monkeypatch.delenv("S3_IMAGES_BUCKET", raising=False)

        with pytest.raises(WidgetImageStorageError):
            upload_widget_image(b"fake-bytes", "image/png", org)

    def test_upload_widget_image_success(self, org, monkeypatch):
        """Test a successful upload returns the S3 url/key and scopes the key to the org"""
        monkeypatch.setenv("S3_IMAGES_BUCKET", "test-bucket")

        with patch(
            "ddpui.services.dashboard_service.upload_file",
            return_value="https://test-bucket.s3.ap-south-1.amazonaws.com/fake-key.png",
        ) as mock_upload:
            image_url, image_key = upload_widget_image(b"fake-bytes", "image/png", org)

        assert image_url == "https://test-bucket.s3.ap-south-1.amazonaws.com/fake-key.png"
        assert image_key.startswith(f"orgs/{org.pk}/dashboards/images/")
        assert image_key.endswith(".png")

        mock_upload.assert_called_once()
        called_bucket, called_key, called_bytes, called_content_type = mock_upload.call_args[0]
        assert called_bucket == "test-bucket"
        assert called_key == image_key
        assert called_bytes == b"fake-bytes"
        assert called_content_type == "image/png"

    def test_upload_widget_image_s3_failure(self, org, monkeypatch):
        """Test that an S3 upload failure surfaces as a storage error"""
        monkeypatch.setenv("S3_IMAGES_BUCKET", "test-bucket")

        with patch(
            "ddpui.services.dashboard_service.upload_file",
            side_effect=Exception("S3 unavailable"),
        ):
            with pytest.raises(WidgetImageStorageError):
                upload_widget_image(b"fake-bytes", "image/png", org)


class TestDeleteWidgetImage:
    """Tests for delete_widget_image()"""

    def test_delete_widget_image_wrong_org_prefix(self, org):
        """Test that a key outside this org's own prefix is rejected without touching S3"""
        with pytest.raises(WidgetImagePermissionError):
            delete_widget_image("orgs/some-other-org/dashboards/images/file.png", org)

    def test_delete_widget_image_success(self, org, monkeypatch):
        """Test a successful delete calls S3 with the right bucket/key"""
        monkeypatch.setenv("S3_IMAGES_BUCKET", "test-bucket")
        image_key = f"orgs/{org.pk}/dashboards/images/file.png"

        with patch("ddpui.services.dashboard_service.delete_file") as mock_delete:
            delete_widget_image(image_key, org)

        mock_delete.assert_called_once_with("test-bucket", image_key)

    def test_delete_widget_image_s3_failure(self, org, monkeypatch):
        """Test that an S3 delete failure surfaces as a storage error"""
        monkeypatch.setenv("S3_IMAGES_BUCKET", "test-bucket")
        image_key = f"orgs/{org.pk}/dashboards/images/file.png"

        with patch(
            "ddpui.services.dashboard_service.delete_file",
            side_effect=Exception("S3 unavailable"),
        ):
            with pytest.raises(WidgetImageStorageError):
                delete_widget_image(image_key, org)

    def test_delete_widget_image_duplicate_slug_cannot_access_other_orgs_image(self, org):
        """Two orgs sharing the same slug (slug is nullable and has no uniqueness
        constraint — see models/org.py) must NOT be able to delete each other's
        images. The ownership check is keyed by org.pk precisely to prevent this."""
        other_org = Org.objects.create(
            name="Another Org With Same Slug",
            slug=org.slug,  # deliberately identical — slug is not unique-enforced
            airbyte_workspace_id="workspace-id-dup-slug",
        )
        image_key = f"orgs/{org.pk}/dashboards/images/file.png"

        try:
            with pytest.raises(WidgetImagePermissionError):
                delete_widget_image(image_key, other_org)
        finally:
            other_org.delete()

    def test_delete_widget_image_survives_org_slug_change(self, org, monkeypatch):
        """Changing an org's slug after an image was uploaded must not break
        deleting that image later — the key is scoped by the immutable org.pk,
        not the mutable slug."""
        monkeypatch.setenv("S3_IMAGES_BUCKET", "test-bucket")
        image_key = f"orgs/{org.pk}/dashboards/images/file.png"

        org.slug = "a-brand-new-slug"
        org.save()

        with patch("ddpui.services.dashboard_service.delete_file") as mock_delete:
            delete_widget_image(image_key, org)

        mock_delete.assert_called_once_with("test-bucket", image_key)
