"""
Pytest test cases for dashboard lock functionality
"""

import pytest
from datetime import datetime, timedelta
from django.utils import timezone
from django.test import Client
from django.urls import reverse
from unittest.mock import patch
import uuid
import json

from ddpui.models.dashboard import Dashboard, DashboardLock
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.user import User


@pytest.fixture
def test_org():
    """Create a test organization"""
    return Org.objects.create(name="Test Org", slug="test-org")


@pytest.fixture
def test_user():
    """Create a test user"""
    return User.objects.create_user(
        username="testuser@example.com", email="testuser@example.com", password="testpass123"
    )


@pytest.fixture
def test_orguser(test_org, test_user):
    """Create a test orguser"""
    return OrgUser.objects.create(
        user=test_user, org=test_org, role=1  # Assuming 1 is a valid role
    )


@pytest.fixture
def test_dashboard(test_org, test_orguser):
    """Create a test dashboard"""
    return Dashboard.objects.create(
        title="Test Dashboard",
        description="Test description",
        created_by=test_orguser,
        org=test_org,
        last_modified_by=test_orguser,
    )


@pytest.fixture
def authenticated_client(test_orguser):
    """Create an authenticated client"""
    client = Client()
    # Mock authentication - adjust based on your auth implementation
    client.defaults["HTTP_AUTHORIZATION"] = "Bearer test-token"
    client.defaults["HTTP_X_DALGO_ORG"] = str(test_orguser.org.id)
    return client


class TestDashboardLockAPI:
    """Test cases for dashboard lock API endpoints"""

    @patch("ddpui.auth.has_permission")
    @patch("ddpui.api.dashboard_native_api.request")
    def test_create_lock_2_minute_duration(
        self, mock_request, mock_permission, test_dashboard, test_orguser
    ):
        """Test that new locks are created with 2-minute duration"""
        mock_request.orguser = test_orguser
        mock_permission.return_value = True

        from ddpui.api.dashboard_native_api import lock_dashboard

        # Call the lock function
        response = lock_dashboard(mock_request, test_dashboard.id)

        # Verify lock was created
        lock = DashboardLock.objects.get(dashboard=test_dashboard)
        assert lock is not None
        assert lock.locked_by == test_orguser

        # Verify 2-minute duration (allow 5 second tolerance for test execution time)
        expected_expiry = timezone.now() + timedelta(minutes=2)
        time_diff = abs((lock.expires_at - expected_expiry).total_seconds())
        assert (
            time_diff < 5
        ), f"Lock duration should be ~2 minutes, got {time_diff} seconds difference"

        # Verify response
        assert response.lock_token == lock.lock_token
        assert response.locked_by == test_orguser.user.email

    @patch("ddpui.auth.has_permission")
    @patch("ddpui.api.dashboard_native_api.request")
    def test_refresh_lock_extends_duration(
        self, mock_request, mock_permission, test_dashboard, test_orguser
    ):
        """Test that lock refresh extends the duration by 2 minutes"""
        mock_request.orguser = test_orguser
        mock_permission.return_value = True

        from ddpui.api.dashboard_native_api import lock_dashboard, refresh_dashboard_lock

        # Create initial lock
        lock_dashboard(mock_request, test_dashboard.id)
        initial_lock = DashboardLock.objects.get(dashboard=test_dashboard)
        initial_expiry = initial_lock.expires_at

        # Wait a moment to ensure time difference
        import time

        time.sleep(1)

        # Refresh the lock
        response = refresh_dashboard_lock(mock_request, test_dashboard.id)

        # Verify lock was refreshed
        refreshed_lock = DashboardLock.objects.get(dashboard=test_dashboard)
        assert refreshed_lock.expires_at > initial_expiry

        # Verify new expiry is ~2 minutes from now
        expected_expiry = timezone.now() + timedelta(minutes=2)
        time_diff = abs((refreshed_lock.expires_at - expected_expiry).total_seconds())
        assert time_diff < 5, f"Refreshed lock should expire ~2 minutes from now"

    @patch("ddpui.auth.has_permission")
    @patch("ddpui.api.dashboard_native_api.request")
    def test_refresh_expired_lock_fails(
        self, mock_request, mock_permission, test_dashboard, test_orguser
    ):
        """Test that refreshing an expired lock fails"""
        mock_request.orguser = test_orguser
        mock_permission.return_value = True

        from ddpui.api.dashboard_native_api import refresh_dashboard_lock
        from ninja.errors import HttpError

        # Create expired lock manually
        expired_lock = DashboardLock.objects.create(
            dashboard=test_dashboard,
            locked_by=test_orguser,
            lock_token=str(uuid.uuid4()),
            expires_at=timezone.now() - timedelta(minutes=1),
        )

        # Try to refresh expired lock
        with pytest.raises(HttpError) as exc_info:
            refresh_dashboard_lock(mock_request, test_dashboard.id)

        assert exc_info.value.status_code == 410
        assert "expired" in str(exc_info.value.message).lower()

    @patch("ddpui.auth.has_permission")
    @patch("ddpui.api.dashboard_native_api.request")
    def test_auto_refresh_on_dashboard_update(
        self, mock_request, mock_permission, test_dashboard, test_orguser
    ):
        """Test that dashboard updates automatically refresh the lock"""
        mock_request.orguser = test_orguser
        mock_permission.return_value = True

        from ddpui.api.dashboard_native_api import lock_dashboard, update_dashboard
        from ddpui.api.dashboard_native_api import DashboardUpdate

        # Create initial lock
        lock_dashboard(mock_request, test_dashboard.id)
        initial_lock = DashboardLock.objects.get(dashboard=test_dashboard)
        initial_expiry = initial_lock.expires_at

        # Wait a moment
        import time

        time.sleep(1)

        # Update dashboard
        update_data = DashboardUpdate(title="Updated Title")
        update_dashboard(mock_request, test_dashboard.id, update_data)

        # Verify lock was auto-refreshed
        updated_lock = DashboardLock.objects.get(dashboard=test_dashboard)
        assert updated_lock.expires_at > initial_expiry

    @patch("ddpui.auth.has_permission")
    @patch("ddpui.api.dashboard_native_api.request")
    def test_cannot_refresh_other_users_lock(
        self, mock_request, mock_permission, test_dashboard, test_org
    ):
        """Test that users cannot refresh locks owned by other users"""
        mock_permission.return_value = True

        # Create two different users
        user1 = User.objects.create_user(username="user1@example.com", email="user1@example.com")
        user2 = User.objects.create_user(username="user2@example.com", email="user2@example.com")
        orguser1 = OrgUser.objects.create(user=user1, org=test_org, role=1)
        orguser2 = OrgUser.objects.create(user=user2, org=test_org, role=1)

        from ddpui.api.dashboard_native_api import refresh_dashboard_lock
        from ninja.errors import HttpError

        # User1 creates a lock
        lock = DashboardLock.objects.create(
            dashboard=test_dashboard,
            locked_by=orguser1,
            lock_token=str(uuid.uuid4()),
            expires_at=timezone.now() + timedelta(minutes=2),
        )

        # User2 tries to refresh user1's lock
        mock_request.orguser = orguser2
        with pytest.raises(HttpError) as exc_info:
            refresh_dashboard_lock(mock_request, test_dashboard.id)

        assert exc_info.value.status_code == 403
        assert "refresh your own" in str(exc_info.value.message).lower()

    def test_lock_model_is_expired_method(self, test_dashboard, test_orguser):
        """Test the DashboardLock.is_expired() method"""

        # Test active lock
        active_lock = DashboardLock.objects.create(
            dashboard=test_dashboard,
            locked_by=test_orguser,
            lock_token=str(uuid.uuid4()),
            expires_at=timezone.now() + timedelta(minutes=1),
        )
        assert not active_lock.is_expired()

        # Test expired lock
        expired_lock = DashboardLock.objects.create(
            dashboard=test_dashboard,
            locked_by=test_orguser,
            lock_token=str(uuid.uuid4()),
            expires_at=timezone.now() - timedelta(minutes=1),
        )
        assert expired_lock.is_expired()

    @patch("ddpui.auth.has_permission")
    @patch("ddpui.api.dashboard_native_api.request")
    def test_lock_refresh_existing_lock_extends_duration(
        self, mock_request, mock_permission, test_dashboard, test_orguser
    ):
        """Test that requesting lock on existing active lock refreshes it"""
        mock_request.orguser = test_orguser
        mock_permission.return_value = True

        from ddpui.api.dashboard_native_api import lock_dashboard

        # Create initial lock
        response1 = lock_dashboard(mock_request, test_dashboard.id)
        lock1 = DashboardLock.objects.get(dashboard=test_dashboard)
        initial_expiry = lock1.expires_at

        # Wait a moment
        import time

        time.sleep(1)

        # Request lock again (should refresh existing)
        response2 = lock_dashboard(mock_request, test_dashboard.id)
        lock2 = DashboardLock.objects.get(dashboard=test_dashboard)

        # Should be same lock but with extended expiry
        assert lock1.id == lock2.id
        assert lock2.expires_at > initial_expiry
        assert response1.lock_token == response2.lock_token


class TestDashboardLockCleanup:
    """Test cases for lock cleanup and expiry handling"""

    def test_expired_lock_cleanup_on_new_lock_request(self, test_dashboard, test_orguser):
        """Test that expired locks are cleaned up when requesting new lock"""

        # Create expired lock
        expired_lock = DashboardLock.objects.create(
            dashboard=test_dashboard,
            locked_by=test_orguser,
            lock_token=str(uuid.uuid4()),
            expires_at=timezone.now() - timedelta(minutes=1),
        )

        # Verify expired lock exists
        assert DashboardLock.objects.filter(dashboard=test_dashboard).count() == 1

        from ddpui.api.dashboard_native_api import lock_dashboard
        from unittest.mock import Mock

        # Request new lock (should clean up expired one)
        mock_request = Mock()
        mock_request.orguser = test_orguser

        with patch("ddpui.auth.has_permission", return_value=True):
            lock_dashboard(mock_request, test_dashboard.id)

        # Should have one new lock (expired one deleted, new one created)
        locks = DashboardLock.objects.filter(dashboard=test_dashboard)
        assert locks.count() == 1

        new_lock = locks.first()
        assert new_lock.id != expired_lock.id
        assert not new_lock.is_expired()


@pytest.mark.integration
class TestDashboardLockIntegration:
    """Integration tests for dashboard lock system"""

    def test_full_lock_workflow(self, test_dashboard, test_orguser):
        """Test complete lock workflow: create -> refresh -> update -> unlock"""

        from ddpui.api.dashboard_native_api import (
            lock_dashboard,
            refresh_dashboard_lock,
            update_dashboard,
            unlock_dashboard,
            DashboardUpdate,
        )
        from unittest.mock import Mock

        mock_request = Mock()
        mock_request.orguser = test_orguser

        with patch("ddpui.auth.has_permission", return_value=True):
            # 1. Create lock
            lock_response = lock_dashboard(mock_request, test_dashboard.id)
            assert lock_response.lock_token

            initial_lock = DashboardLock.objects.get(dashboard=test_dashboard)
            initial_expiry = initial_lock.expires_at

            # 2. Refresh lock
            import time

            time.sleep(1)
            refresh_response = refresh_dashboard_lock(mock_request, test_dashboard.id)

            refreshed_lock = DashboardLock.objects.get(dashboard=test_dashboard)
            assert refreshed_lock.expires_at > initial_expiry

            # 3. Update dashboard (should auto-refresh lock)
            time.sleep(1)
            update_data = DashboardUpdate(title="Updated in workflow")
            update_dashboard(mock_request, test_dashboard.id, update_data)

            updated_lock = DashboardLock.objects.get(dashboard=test_dashboard)
            assert updated_lock.expires_at > refreshed_lock.expires_at

            # 4. Unlock
            unlock_response = unlock_dashboard(mock_request, test_dashboard.id)
            assert unlock_response["success"] is True

            # Verify lock is deleted
            assert not DashboardLock.objects.filter(dashboard=test_dashboard).exists()
