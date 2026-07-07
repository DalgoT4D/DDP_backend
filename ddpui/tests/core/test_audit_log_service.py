"""
Tests for the audit log service.

These tests verify that:
1. compute_changes() correctly finds differences between two dicts
2. _write_audit_log() correctly writes to the database
3. create_audit_log() starts a background thread and never crashes
"""

from unittest.mock import patch, MagicMock

import pytest
from django.contrib.auth.models import User

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.audit_log import AuditLog, AuditLogResourceType, AuditLogAction
from ddpui.core.audit_log_service import (
    create_audit_log,
    _write_audit_log,
    compute_changes,
)

# This tells pytest to use the test database
pytestmark = pytest.mark.django_db


# ============================================================================
# FIXTURES - These create test data that our tests can use
# ============================================================================

@pytest.fixture
def test_user():
    """Creates a test Django user."""
    user = User.objects.create_user(
        username="testuser",
        email="test@example.com",
        password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def test_org():
    """Creates a test organization."""
    org = Org.objects.create(name="Test Org", slug="test-org")
    yield org
    org.delete()


@pytest.fixture
def test_orguser(test_user, test_org):
    """Creates a test org user (links a user to an org)."""
    orguser = OrgUser.objects.create(user=test_user, org=test_org)
    yield orguser
    orguser.delete()


# ============================================================================
# TESTS FOR compute_changes()
# ============================================================================

class TestComputeChanges:
    """Tests for the compute_changes helper function."""

    def test_no_changes_returns_empty_dict(self):
        """When before and after are identical, returns empty dict."""
        before = {"name": "Test", "value": 123}
        after = {"name": "Test", "value": 123}

        result = compute_changes(before, after)

        assert result == {}

    def test_detects_single_field_change(self):
        """Detects when one field changes."""
        before = {"name": "Old Name", "value": 123}
        after = {"name": "New Name", "value": 123}

        result = compute_changes(before, after)

        assert result == {"name": {"old": "Old Name", "new": "New Name"}}

    def test_detects_multiple_field_changes(self):
        """Detects when multiple fields change."""
        before = {"name": "Old", "value": 100, "active": True}
        after = {"name": "New", "value": 200, "active": True}

        result = compute_changes(before, after)

        assert result == {
            "name": {"old": "Old", "new": "New"},
            "value": {"old": 100, "new": 200},
        }

    def test_detects_field_added(self):
        """Detects when a new field is added."""
        before = {"name": "Test"}
        after = {"name": "Test", "description": "A description"}

        result = compute_changes(before, after)

        assert result == {"description": {"old": None, "new": "A description"}}

    def test_detects_field_removed(self):
        """Detects when a field is removed."""
        before = {"name": "Test", "description": "A description"}
        after = {"name": "Test"}

        result = compute_changes(before, after)

        assert result == {"description": {"old": "A description", "new": None}}

    def test_excludes_specified_fields(self):
        """Does not include excluded fields in the diff."""
        before = {"name": "Old", "password": "secret1", "token": "abc123"}
        after = {"name": "New", "password": "secret2", "token": "xyz789"}

        result = compute_changes(before, after, exclude_fields=["password", "token"])

        # Only name should appear, not password or token
        assert result == {"name": {"old": "Old", "new": "New"}}

    def test_handles_none_values(self):
        """Handles None values correctly."""
        before = {"name": None}
        after = {"name": "New Name"}

        result = compute_changes(before, after)

        assert result == {"name": {"old": None, "new": "New Name"}}


# ============================================================================
# TESTS FOR _write_audit_log()
# We mock django.db.connection.close() because closing the connection
# in tests breaks pytest's database cleanup.
# ============================================================================

class TestWriteAuditLog:
    """Tests for the _write_audit_log internal function."""

    @patch("ddpui.core.audit_log_service.django.db.connection.close")
    def test_creates_audit_log_in_database(self, mock_close, test_org, test_orguser):
        """_write_audit_log creates an AuditLog row in the database."""
        initial_count = AuditLog.objects.count()

        _write_audit_log(
            org_id=test_org.id,
            orguser_id=test_orguser.id,
            orguser_email=test_orguser.user.email,
            resource_type=AuditLogResourceType.DASHBOARD,
            resource_id="123",
            resource_name="Test Dashboard",
            action=AuditLogAction.CREATE,
            field_changes={},
        )

        # Should have one more record now
        assert AuditLog.objects.count() == initial_count + 1

        # Check the record has correct values
        log = AuditLog.objects.latest("timestamp")
        assert log.org_id == test_org.id
        assert log.orguser_id == test_orguser.id
        assert log.orguser_email == "test@example.com"
        assert log.resource_type == AuditLogResourceType.DASHBOARD
        assert log.resource_id == "123"
        assert log.resource_name == "Test Dashboard"
        assert log.action == AuditLogAction.CREATE

        # Verify close was called (important for real usage)
        mock_close.assert_called_once()

    @patch("ddpui.core.audit_log_service.django.db.connection.close")
    def test_stores_field_changes(self, mock_close, test_org, test_orguser):
        """_write_audit_log stores field_changes correctly."""
        changes = {"name": {"old": "Old", "new": "New"}}

        _write_audit_log(
            org_id=test_org.id,
            orguser_id=test_orguser.id,
            orguser_email=test_orguser.user.email,
            resource_type=AuditLogResourceType.DASHBOARD,
            resource_id="123",
            resource_name="Test Dashboard",
            action=AuditLogAction.UPDATE,
            field_changes=changes,
        )

        log = AuditLog.objects.latest("timestamp")
        assert log.field_changes == changes

    @patch("ddpui.core.audit_log_service.django.db.connection.close")
    def test_handles_null_orguser(self, mock_close, test_org):
        """_write_audit_log works when orguser is None (system actions)."""
        _write_audit_log(
            org_id=test_org.id,
            orguser_id=None,
            orguser_email="",
            resource_type=AuditLogResourceType.ORG,
            resource_id=str(test_org.id),
            resource_name=test_org.name,
            action=AuditLogAction.CREATE,
            field_changes={},
        )

        log = AuditLog.objects.latest("timestamp")
        assert log.orguser_id is None
        assert log.orguser_email == ""

    @patch("ddpui.core.audit_log_service.django.db.connection.close")
    @patch("ddpui.core.audit_log_service.AuditLog.objects.create")
    @patch("ddpui.core.audit_log_service.logger")
    def test_logs_error_on_db_failure(self, mock_logger, mock_create, mock_close, test_org):
        """_write_audit_log logs errors instead of crashing on DB failure."""
        mock_create.side_effect = Exception("Database error")

        # Should NOT raise an exception
        _write_audit_log(
            org_id=test_org.id,
            orguser_id=None,
            orguser_email="",
            resource_type=AuditLogResourceType.ORG,
            resource_id="123",
            resource_name="Test",
            action=AuditLogAction.CREATE,
            field_changes={},
        )

        # Should have logged the error
        mock_logger.error.assert_called_once()
        assert "failed to write audit log" in mock_logger.error.call_args[0][0]

        # Connection.close should still be called (in finally block)
        mock_close.assert_called_once()


# ============================================================================
# TESTS FOR create_audit_log()
# We mock _write_audit_log because background threads can't see test data
# (Django tests use transactions that other threads can't access)
# ============================================================================

class TestCreateAuditLog:
    """Tests for the create_audit_log public function."""

    @patch("ddpui.core.audit_log_service._write_audit_log")
    def test_starts_background_thread_with_correct_args(self, mock_write, test_org, test_orguser):
        """create_audit_log starts a thread that calls _write_audit_log with correct args."""

        create_audit_log(
            org=test_org,
            orguser=test_orguser,
            resource_type=AuditLogResourceType.CHART,
            resource_id="456",
            resource_name="Test Chart",
            action=AuditLogAction.CREATE,
            field_changes={"title": {"old": None, "new": "Test Chart"}},
        )

        # Give the thread a moment to execute
        import time
        time.sleep(0.1)

        # Verify _write_audit_log was called with correct arguments
        mock_write.assert_called_once_with(
            org_id=test_org.id,
            orguser_id=test_orguser.id,
            orguser_email="test@example.com",
            resource_type=AuditLogResourceType.CHART,
            resource_id="456",
            resource_name="Test Chart",
            action=AuditLogAction.CREATE,
            field_changes={"title": {"old": None, "new": "Test Chart"}},
        )

    @patch("ddpui.core.audit_log_service._write_audit_log")
    def test_handles_null_orguser(self, mock_write, test_org):
        """create_audit_log correctly passes None orguser."""
        create_audit_log(
            org=test_org,
            orguser=None,
            resource_type=AuditLogResourceType.ORG,
            resource_id=str(test_org.id),
            resource_name=test_org.name,
            action=AuditLogAction.CREATE,
        )

        import time
        time.sleep(0.1)

        mock_write.assert_called_once()
        call_kwargs = mock_write.call_args[1]
        assert call_kwargs["orguser_id"] is None
        assert call_kwargs["orguser_email"] == ""

    @patch("ddpui.core.audit_log_service._write_audit_log")
    def test_defaults_field_changes_to_empty_dict(self, mock_write, test_org, test_orguser):
        """create_audit_log defaults field_changes to empty dict when not provided."""
        create_audit_log(
            org=test_org,
            orguser=test_orguser,
            resource_type=AuditLogResourceType.METRIC,
            resource_id="999",
            action=AuditLogAction.DELETE,
        )

        import time
        time.sleep(0.1)

        mock_write.assert_called_once()
        call_kwargs = mock_write.call_args[1]
        assert call_kwargs["field_changes"] == {}

    @patch("ddpui.core.audit_log_service.threading.Thread")
    @patch("ddpui.core.audit_log_service.logger")
    def test_never_raises_exception(self, mock_logger, mock_thread_class, test_org, test_orguser):
        """create_audit_log catches exceptions and logs them instead of crashing."""
        mock_thread_class.side_effect = RuntimeError("Thread pool exhausted")

        # Should NOT raise an exception
        create_audit_log(
            org=test_org,
            orguser=test_orguser,
            resource_type=AuditLogResourceType.DASHBOARD,
            resource_id="123",
            action=AuditLogAction.DELETE,
        )

        # Should have logged the error
        mock_logger.error.assert_called_once()
        assert "failed to start write thread" in mock_logger.error.call_args[0][0]
