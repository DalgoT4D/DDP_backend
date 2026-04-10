"""Role-based permission tests for Report and Comment API endpoints

Tests that each user role (Super Admin, Account Manager, Pipeline Manager,
Analyst, Guest) can only access the endpoints their permissions allow.

Actual permission matrix (from seed data):
┌─────────────────────────────┬───────────┬─────────┬──────────┬─────────┬───────┐
│ Endpoint                    │ SuperAdmin│ AcctMgr │ PipeMgr  │ Analyst │ Guest │
├─────────────────────────────┼───────────┼─────────┼──────────┼─────────┼───────┤
│ list_snapshots (view)       │     ✓     │    ✓    │    ✓     │    ✓    │   ✓   │
│ create_snapshot (create)    │     ✓     │    ✓    │    ✓     │    ✓    │   ✗   │
│ get_snapshot_view (view)    │     ✓     │    ✓    │    ✓     │    ✓    │   ✓   │
│ update_snapshot (edit)      │     ✓     │    ✓    │    ✓     │    ✓    │   ✗   │
│ delete_snapshot (delete)    │     ✓     │    ✓    │    ✓     │    ✓    │   ✗   │
│ toggle_sharing (share)      │     ✓     │    ✓    │    ✓     │    ✓    │   ✗   │
│ get_sharing_status (view)   │     ✓     │    ✓    │    ✓     │    ✓    │   ✓   │
│ list_comments (view)        │     ✓     │    ✓    │    ✓     │    ✓    │   ✓   │
│ create_comment (edit)       │     ✓     │    ✓    │    ✓     │    ✓    │   ✗   │
│ update_comment (edit)       │     ✓     │    ✓    │    ✓     │    ✓    │   ✗   │
│ delete_comment (edit)       │     ✓     │    ✓    │    ✓     │    ✓    │   ✗   │
│ get_comment_states (view)   │     ✓     │    ✓    │    ✓     │    ✓    │   ✓   │
│ mark_as_read (view)         │     ✓     │    ✓    │    ✓     │    ✓    │   ✓   │
│ get_mentionable_users (view)│     ✓     │    ✓    │    ✓     │    ✓    │   ✓   │
│ export_pdf (view)           │     ✓     │    ✓    │    ✓     │    ✓    │   ✓   │
│ datetime_columns (view)     │     ✓     │    ✓    │    ✓     │    ✓    │   ✓   │
└─────────────────────────────┴───────────┴─────────┴──────────┴─────────┴───────┘

Note: Only Guest lacks create/edit/delete/share permissions for dashboards.
"""

import os
import django
from datetime import date
from unittest.mock import patch

import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.report import ReportSnapshot
from ddpui.models.comment import Comment, CommentTargetType
from ddpui.auth import (
    SUPER_ADMIN_ROLE,
    ACCOUNT_MANAGER_ROLE,
    PIPELINE_MANAGER_ROLE,
    ANALYST_ROLE,
    GUEST_ROLE,
)
from ddpui.api.report_api import (
    list_snapshots,
    create_snapshot,
    get_snapshot_view,
    update_snapshot,
    delete_snapshot,
    toggle_report_sharing,
    get_report_sharing_status,
    list_dashboard_datetime_columns,
    get_mentionable_users,
    get_comment_states,
    mark_as_read,
    list_comments,
    create_comment,
    update_comment,
    delete_comment,
    export_report_pdf,
)
from ddpui.schemas.report_schema import (
    SnapshotCreate,
    SnapshotUpdate,
    DateColumnSchema,
    CommentCreate,
    CommentUpdate,
    MarkReadRequest,
)
from ddpui.schemas.dashboard_schema import ShareToggle
from ddpui.tests.api_tests.test_user_org_api import mock_request

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Permission Test Org",
        slug="perm-test-org",
        airbyte_workspace_id="perm-ws-id",
    )
    yield org
    org.delete()


def _create_orguser(username, email, org, role_slug):
    """Helper to create a User + OrgUser with a specific role."""
    user = User.objects.create(username=username, email=email, password="testpassword")
    orguser = OrgUser.objects.create(
        user=user,
        org=org,
        new_role=Role.objects.filter(slug=role_slug).first(),
    )
    return user, orguser


@pytest.fixture
def super_admin_user(org, seed_db):
    user, orguser = _create_orguser("superadmin", "superadmin@test.com", org, SUPER_ADMIN_ROLE)
    yield orguser
    orguser.delete()
    user.delete()


@pytest.fixture
def account_manager_user(org, seed_db):
    user, orguser = _create_orguser("acctmgr", "acctmgr@test.com", org, ACCOUNT_MANAGER_ROLE)
    yield orguser
    orguser.delete()
    user.delete()


@pytest.fixture
def pipeline_manager_user(org, seed_db):
    user, orguser = _create_orguser("pipemgr", "pipemgr@test.com", org, PIPELINE_MANAGER_ROLE)
    yield orguser
    orguser.delete()
    user.delete()


@pytest.fixture
def analyst_user(org, seed_db):
    user, orguser = _create_orguser("analyst", "analyst@test.com", org, ANALYST_ROLE)
    yield orguser
    orguser.delete()
    user.delete()


@pytest.fixture
def guest_user(org, seed_db):
    user, orguser = _create_orguser("guest", "guest@test.com", org, GUEST_ROLE)
    yield orguser
    orguser.delete()
    user.delete()


@pytest.fixture
def snapshot(org, account_manager_user):
    """A pre-created snapshot owned by account_manager_user."""
    snapshot = ReportSnapshot.objects.create(
        title="Permission Test Report",
        date_column={"schema_name": "public", "table_name": "orders", "column_name": "created_at"},
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        frozen_dashboard={
            "title": "Test Dashboard",
            "grid_columns": 12,
            "layout_config": [],
            "components": {},
            "filters": [],
        },
        frozen_chart_configs={"10": {"title": "Chart A"}, "20": {"title": "Chart B"}},
        created_by=account_manager_user,
        org=org,
    )
    yield snapshot
    try:
        snapshot.refresh_from_db()
        snapshot.delete()
    except ReportSnapshot.DoesNotExist:
        pass


@pytest.fixture
def comment(org, account_manager_user, snapshot):
    """A comment owned by account_manager_user."""
    comment = Comment.objects.create(
        target_type=CommentTargetType.SUMMARY,
        snapshot=snapshot,
        content="Test comment for permissions",
        author=account_manager_user,
        org=org,
    )
    yield comment
    try:
        comment.refresh_from_db()
        comment.delete()
    except Comment.DoesNotExist:
        pass


# ================================================================================
# Helper: assert permission denied (403 or 404 from the has_permission decorator)
# ================================================================================


def assert_permission_denied(func, *args, **kwargs):
    """Assert that calling func raises HttpError with 403 or 404 (unauthorized)."""
    with pytest.raises(HttpError) as exc_info:
        func(*args, **kwargs)
    # The has_permission decorator returns 404 with "unauthorized" or 403 with "not allowed"
    assert exc_info.value.status_code in (403, 404)


# ================================================================================
# Test: Verify permissions are loaded correctly for each role (from seed data)
# ================================================================================


class TestRolePermissionsLoaded:
    """Verify that the seed data gives each role the expected dashboard permissions."""

    def test_super_admin_has_all_dashboard_permissions(self, super_admin_user):
        request = mock_request(super_admin_user)
        assert "can_view_dashboards" in request.permissions
        assert "can_create_dashboards" in request.permissions
        assert "can_edit_dashboards" in request.permissions
        assert "can_delete_dashboards" in request.permissions
        assert "can_share_dashboards" in request.permissions

    def test_account_manager_has_all_dashboard_permissions(self, account_manager_user):
        request = mock_request(account_manager_user)
        assert "can_view_dashboards" in request.permissions
        assert "can_create_dashboards" in request.permissions
        assert "can_edit_dashboards" in request.permissions
        assert "can_delete_dashboards" in request.permissions
        assert "can_share_dashboards" in request.permissions

    def test_pipeline_manager_has_all_dashboard_permissions(self, pipeline_manager_user):
        request = mock_request(pipeline_manager_user)
        assert "can_view_dashboards" in request.permissions
        assert "can_create_dashboards" in request.permissions
        assert "can_edit_dashboards" in request.permissions
        assert "can_delete_dashboards" in request.permissions
        assert "can_share_dashboards" in request.permissions

    def test_analyst_has_all_dashboard_permissions(self, analyst_user):
        request = mock_request(analyst_user)
        assert "can_view_dashboards" in request.permissions
        assert "can_create_dashboards" in request.permissions
        assert "can_edit_dashboards" in request.permissions
        assert "can_delete_dashboards" in request.permissions
        assert "can_share_dashboards" in request.permissions

    def test_guest_has_only_view_dashboard_permission(self, guest_user):
        request = mock_request(guest_user)
        assert "can_view_dashboards" in request.permissions
        assert "can_create_dashboards" not in request.permissions
        assert "can_edit_dashboards" not in request.permissions
        assert "can_delete_dashboards" not in request.permissions
        assert "can_share_dashboards" not in request.permissions


# ================================================================================
# Test Report Endpoints - View Permission (can_view_dashboards)
# All roles have this permission
# ================================================================================


class TestReportViewPermissions:
    """Test that all roles can access view endpoints."""

    def test_super_admin_can_list_snapshots(self, super_admin_user):
        request = mock_request(super_admin_user)
        response = list_snapshots(request)
        assert response["success"] is True

    def test_account_manager_can_list_snapshots(self, account_manager_user):
        request = mock_request(account_manager_user)
        response = list_snapshots(request)
        assert response["success"] is True

    def test_pipeline_manager_can_list_snapshots(self, pipeline_manager_user):
        request = mock_request(pipeline_manager_user)
        response = list_snapshots(request)
        assert response["success"] is True

    def test_analyst_can_list_snapshots(self, analyst_user):
        request = mock_request(analyst_user)
        response = list_snapshots(request)
        assert response["success"] is True

    def test_guest_can_list_snapshots(self, guest_user):
        request = mock_request(guest_user)
        response = list_snapshots(request)
        assert response["success"] is True

    @patch("ddpui.core.reports.report_service.ReportService._inject_period_into_chart_configs")
    def test_guest_can_view_snapshot(self, mock_inject, guest_user, snapshot):
        request = mock_request(guest_user)
        response = get_snapshot_view(request, snapshot.id)
        assert response["success"] is True

    @patch("ddpui.core.reports.report_service.ReportService._inject_period_into_chart_configs")
    def test_analyst_can_view_snapshot(self, mock_inject, analyst_user, snapshot):
        request = mock_request(analyst_user)
        response = get_snapshot_view(request, snapshot.id)
        assert response["success"] is True

    @patch("ddpui.core.reports.report_service.ReportService._inject_period_into_chart_configs")
    def test_pipeline_manager_can_view_snapshot(self, mock_inject, pipeline_manager_user, snapshot):
        request = mock_request(pipeline_manager_user)
        response = get_snapshot_view(request, snapshot.id)
        assert response["success"] is True


# ================================================================================
# Test Report Endpoints - Guest Cannot Create/Edit/Delete/Share
# Guest is the ONLY role lacking these permissions
# ================================================================================


class TestGuestReportRestrictions:
    """Test that Guest cannot create, edit, delete, or share reports."""

    def test_guest_cannot_create_snapshot(self, guest_user):
        request = mock_request(guest_user)
        payload = SnapshotCreate(
            title="Guest Report",
            dashboard_id=1,
            date_column=DateColumnSchema(
                schema_name="public", table_name="orders", column_name="created_at"
            ),
            period_end=date(2026, 3, 31),
        )
        assert_permission_denied(create_snapshot, request, payload)

    def test_guest_cannot_update_snapshot(self, guest_user, snapshot):
        request = mock_request(guest_user)
        payload = SnapshotUpdate(summary="Guest edit attempt")
        assert_permission_denied(update_snapshot, request, snapshot.id, payload)

    def test_guest_cannot_delete_snapshot(self, guest_user, snapshot):
        request = mock_request(guest_user)
        assert_permission_denied(delete_snapshot, request, snapshot.id)

    def test_guest_cannot_toggle_sharing(self, guest_user, snapshot):
        request = mock_request(guest_user)
        payload = ShareToggle(is_public=True)
        assert_permission_denied(toggle_report_sharing, request, snapshot.id, payload)


# ================================================================================
# Test Report Endpoints - Non-Guest roles CAN create/edit/delete/share
# ================================================================================


class TestNonGuestReportAccess:
    """Test that non-Guest roles can access write endpoints."""

    def test_super_admin_can_update_snapshot(self, super_admin_user, snapshot):
        request = mock_request(super_admin_user)
        payload = SnapshotUpdate(summary="Admin update")
        response = update_snapshot(request, snapshot.id, payload)
        assert response["success"] is True

    def test_account_manager_can_update_snapshot(self, account_manager_user, snapshot):
        request = mock_request(account_manager_user)
        payload = SnapshotUpdate(summary="AcctMgr update")
        response = update_snapshot(request, snapshot.id, payload)
        assert response["success"] is True

    def test_pipeline_manager_can_update_snapshot(self, pipeline_manager_user, snapshot):
        request = mock_request(pipeline_manager_user)
        payload = SnapshotUpdate(summary="PipeMgr update")
        response = update_snapshot(request, snapshot.id, payload)
        assert response["success"] is True

    def test_analyst_can_update_snapshot(self, analyst_user, snapshot):
        request = mock_request(analyst_user)
        payload = SnapshotUpdate(summary="Analyst update")
        response = update_snapshot(request, snapshot.id, payload)
        assert response["success"] is True


# ================================================================================
# Test Comment Endpoints - Guest Cannot Create/Update/Delete (can_edit_dashboards)
# ================================================================================


class TestGuestCommentRestrictions:
    """Test that Guest cannot create, update, or delete comments."""

    def test_guest_cannot_create_comment(self, guest_user, snapshot):
        request = mock_request(guest_user)
        payload = CommentCreate(target_type="summary", content="Guest comment")
        assert_permission_denied(create_comment, request, snapshot.id, payload)

    def test_guest_cannot_update_comment(self, guest_user, snapshot, comment):
        request = mock_request(guest_user)
        payload = CommentUpdate(content="Guest edit")
        assert_permission_denied(update_comment, request, snapshot.id, comment.id, payload)

    def test_guest_cannot_delete_comment(self, guest_user, snapshot, comment):
        request = mock_request(guest_user)
        assert_permission_denied(delete_comment, request, snapshot.id, comment.id)


# ================================================================================
# Test Comment Endpoints - Guest CAN read comments (can_view_dashboards)
# ================================================================================


class TestGuestCommentReadAccess:
    """Test that Guest can read comments, view states, and mark as read."""

    def test_guest_can_list_comments(self, guest_user, snapshot):
        request = mock_request(guest_user)
        response = list_comments(request, snapshot.id, target_type="summary")
        assert response["success"] is True

    def test_guest_can_get_comment_states(self, guest_user, snapshot):
        request = mock_request(guest_user)
        response = get_comment_states(request, snapshot.id)
        assert response["success"] is True

    def test_guest_can_mark_as_read(self, guest_user, snapshot):
        request = mock_request(guest_user)
        payload = MarkReadRequest(target_type="summary")
        response = mark_as_read(request, snapshot.id, payload)
        assert response["success"] is True

    def test_guest_can_get_mentionable_users(self, guest_user):
        request = mock_request(guest_user)
        response = get_mentionable_users(request)
        assert response["success"] is True


# ================================================================================
# Test Comment Endpoints - Non-Guest roles CAN create/update/delete comments
# ================================================================================


class TestNonGuestCommentAccess:
    """Test that non-Guest roles can create, update, and delete comments."""

    @patch("ddpui.core.reports.mention_service.MentionService.process_mentions")
    def test_super_admin_can_create_comment(self, mock_mentions, super_admin_user, snapshot):
        request = mock_request(super_admin_user)
        payload = CommentCreate(target_type="summary", content="Admin comment")
        response = create_comment(request, snapshot.id, payload)
        assert response["success"] is True
        Comment.objects.filter(id=response["data"]["id"]).delete()

    @patch("ddpui.core.reports.mention_service.MentionService.process_mentions")
    def test_account_manager_can_create_comment(
        self, mock_mentions, account_manager_user, snapshot
    ):
        request = mock_request(account_manager_user)
        payload = CommentCreate(target_type="summary", content="AcctMgr comment")
        response = create_comment(request, snapshot.id, payload)
        assert response["success"] is True
        Comment.objects.filter(id=response["data"]["id"]).delete()

    @patch("ddpui.core.reports.mention_service.MentionService.process_mentions")
    def test_pipeline_manager_can_create_comment(
        self, mock_mentions, pipeline_manager_user, snapshot
    ):
        request = mock_request(pipeline_manager_user)
        payload = CommentCreate(target_type="summary", content="PipeMgr comment")
        response = create_comment(request, snapshot.id, payload)
        assert response["success"] is True
        Comment.objects.filter(id=response["data"]["id"]).delete()

    @patch("ddpui.core.reports.mention_service.MentionService.process_mentions")
    def test_analyst_can_create_comment(self, mock_mentions, analyst_user, snapshot):
        request = mock_request(analyst_user)
        payload = CommentCreate(target_type="summary", content="Analyst comment")
        response = create_comment(request, snapshot.id, payload)
        assert response["success"] is True
        Comment.objects.filter(id=response["data"]["id"]).delete()


# ================================================================================
# Test Sharing Status - Owner-only enforcement at service layer
# ================================================================================


class TestSharingStatusOwnerCheck:
    """Test that non-owners get 403 from the service layer (not the permission decorator)."""

    def test_guest_gets_403_from_service(self, guest_user, snapshot):
        """Guest passes permission check (can_view_dashboards) but fails owner-only."""
        request = mock_request(guest_user)
        with pytest.raises(HttpError) as exc_info:
            get_report_sharing_status(request, snapshot.id)
        assert exc_info.value.status_code == 403

    def test_non_owner_analyst_gets_403_from_service(self, analyst_user, snapshot):
        """Analyst has can_view_dashboards but isn't the owner → 403."""
        request = mock_request(analyst_user)
        with pytest.raises(HttpError) as exc_info:
            get_report_sharing_status(request, snapshot.id)
        assert exc_info.value.status_code == 403

    def test_owner_can_view_sharing_status(self, account_manager_user, snapshot):
        """Owner (creator) can view sharing status."""
        request = mock_request(account_manager_user)
        response = get_report_sharing_status(request, snapshot.id)
        assert response["success"] is True


# ================================================================================
# Test Delete - Owner-only enforcement at service layer
# ================================================================================


class TestDeleteOwnerCheck:
    """Test that non-owners get 403 from the service layer even with delete permission."""

    def test_non_owner_with_delete_permission_gets_403(self, pipeline_manager_user, snapshot):
        """Pipeline Manager has can_delete_dashboards but isn't the creator → 403."""
        request = mock_request(pipeline_manager_user)
        with pytest.raises(HttpError) as exc_info:
            delete_snapshot(request, snapshot.id)
        assert exc_info.value.status_code == 403

    def test_owner_can_delete(self, account_manager_user, snapshot):
        """Owner (creator) can delete the snapshot."""
        snapshot_id = snapshot.id
        request = mock_request(account_manager_user)
        response = delete_snapshot(request, snapshot_id)
        assert response["success"] is True


# ================================================================================
# Test PDF Export - View Permission (can_view_dashboards)
# All roles should be able to export
# ================================================================================


class TestPdfExportPermissions:
    """Test PDF export permission — requires can_view_dashboards only."""

    def test_guest_can_access_pdf_export(self, guest_user, snapshot):
        """Guest has can_view_dashboards, so the permission decorator passes.
        The actual PDF generation may fail (no Playwright), but permission is granted."""
        request = mock_request(guest_user)
        try:
            export_report_pdf(request, snapshot.id)
        except HttpError as e:
            # 500 = PDF generation failure (expected without Playwright)
            # 403/404 = permission denied (should NOT happen)
            assert e.status_code == 500, f"Expected 500 (PDF gen failure), got {e.status_code}"

    def test_analyst_can_access_pdf_export(self, analyst_user, snapshot):
        request = mock_request(analyst_user)
        try:
            export_report_pdf(request, snapshot.id)
        except HttpError as e:
            assert e.status_code == 500, f"Expected 500 (PDF gen failure), got {e.status_code}"


# ================================================================================
# Test Datetime Columns - View Permission (can_view_dashboards)
# ================================================================================


class TestDatetimeColumnsPermissions:
    """Test datetime columns endpoint — requires can_view_dashboards only."""

    def test_guest_can_access_datetime_columns(self, guest_user):
        """Guest has can_view_dashboards, so permission passes.
        Will get 404 because dashboard doesn't exist, not 403."""
        request = mock_request(guest_user)
        with pytest.raises(HttpError) as exc_info:
            list_dashboard_datetime_columns(request, 99999)
        # 404 = dashboard not found (permission passed), not 403
        assert exc_info.value.status_code == 404
