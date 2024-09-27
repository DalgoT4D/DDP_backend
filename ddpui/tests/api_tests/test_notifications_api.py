import os
import django
import pytest
from unittest.mock import patch, MagicMock
from ninja.errors import HttpError
from ddpui.models.org import Org
from ddpui.models.role_based_access import Role
from ddpui.models.org_user import OrgUser, OrgUserRole
from ddpui import auth
from django.contrib.auth.models import User
from ddpui.api.notifications_api import (
    create_notification,
    get_notification_history,
    get_notification_recipients,
    get_user_notifications,
    delete_notification,
    mark_as_read,
    get_unread_notifications_count,
)
from ddpui.schemas.notifications_api_schemas import (
    CreateNotificationPayloadSchema,
    UpdateReadStatusSchema,
)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

pytestmark = pytest.mark.django_db


@pytest.fixture
def authuser():
    """a django User object"""
    user = User.objects.create(
        username="tempusername", email="tempuseremail", password="tempuserpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org_without_workspace():
    """a pytest fixture which creates an Org without an airbyte workspace"""
    org = Org.objects.create(airbyte_workspace_id=None, slug="test-org-slug")
    yield org
    org.delete()


@pytest.fixture
def orguser(authuser, org_without_workspace):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org_without_workspace,
        role=OrgUserRole.ACCOUNT_MANAGER,
        new_role=Role.objects.filter(slug=auth.ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@patch("ddpui.core.notifications_service.get_recipients")
@patch("ddpui.core.notifications_service.create_notification")
def test_create_notification_success(mock_create_notification, mock_get_recipients):
    """tests the success of api endpoint for create notification"""
    payload = {
        "author": "test_author",
        "message": "test_message",
        "urgent": True,
        "scheduled_time": None,
        "sent_to": "all_users",
        "org_slug": "slug",
        "user_email": "test@gmail.com",
        "manager_or_above": False,
    }
    create_notification_payload = CreateNotificationPayloadSchema(**payload)
    mock_get_recipients.return_value = (None, [1, 2, 3])
    mock_create_notification.return_value = (None, {"res": [], "errors": []})
    response = create_notification(MagicMock(), create_notification_payload)
    assert isinstance(response["res"], list)
    assert isinstance(response["errors"], list)
    mock_get_recipients.assert_called_once_with(
        payload["sent_to"],
        payload["org_slug"],
        payload["user_email"],
        payload["manager_or_above"],
    )
    mock_create_notification.assert_called_once_with(
        {
            "author": payload["author"],
            "message": payload["message"],
            "urgent": payload["urgent"],
            "scheduled_time": payload["scheduled_time"],
            "recipients": [1, 2, 3],
        }
    )


@patch("ddpui.core.notifications_service.get_recipients")
@patch("ddpui.core.notifications_service.create_notification")
def test_create_notification_no_recipients(mock_create_notification, mock_get_recipients):
    """
    tests the failure of api endpoint for create notification
    when no recipients were found
    """
    payload = {
        "author": "test_author",
        "message": "test_message",
        "urgent": True,
        "scheduled_time": None,
        "sent_to": "all_users",
        "org_slug": None,
        "user_email": None,
        "manager_or_above": False,
    }
    create_notification_payload = CreateNotificationPayloadSchema(**payload)
    mock_get_recipients.return_value = ("No users found for the given information", [])
    with pytest.raises(HttpError) as excinfo:
        create_notification(MagicMock(), create_notification_payload)
    assert "No users found for the given information" in str(excinfo.value)
    mock_get_recipients.assert_called_once_with(
        payload["sent_to"],
        payload["org_slug"],
        payload["user_email"],
        payload["manager_or_above"],
    )
    mock_create_notification.assert_not_called()


@patch("ddpui.core.notifications_service.get_recipients")
@patch("ddpui.core.notifications_service.create_notification")
def test_create_notification_no_org_slug(mock_create_notification, mock_get_recipients):
    """
    tests the failure of api endpoint for create notification
    when no org_slug is passed for all org users
    """
    payload = {
        "author": "test_author",
        "message": "test_message",
        "urgent": True,
        "scheduled_time": None,
        "sent_to": "all_org_users",
        "org_slug": None,
        "user_email": None,
        "manager_or_above": False,
    }
    create_notification_payload = CreateNotificationPayloadSchema(**payload)
    mock_get_recipients.return_value = (
        "org_slug is required to sent notification to all org users.",
        [],
    )
    with pytest.raises(HttpError) as excinfo:
        create_notification(MagicMock(), create_notification_payload)
    assert "org_slug is required to sent notification to all org users." in str(excinfo.value)
    mock_get_recipients.assert_called_once_with(
        payload["sent_to"],
        payload["org_slug"],
        payload["user_email"],
        payload["manager_or_above"],
    )
    mock_create_notification.assert_not_called()


@patch("ddpui.core.notifications_service.get_recipients")
@patch("ddpui.core.notifications_service.create_notification")
def test_create_notification_no_user_email(mock_create_notification, mock_get_recipients):
    """
    tests the failure of api endpoint for create notification
    when no email is passed for single user
    """
    payload = {
        "author": "test_author",
        "message": "test_message",
        "urgent": True,
        "scheduled_time": None,
        "sent_to": "single_user",
        "org_slug": None,
        "user_email": None,
        "manager_or_above": False,
    }
    create_notification_payload = CreateNotificationPayloadSchema(**payload)
    mock_get_recipients.return_value = (
        "user email is required to sent notification to a user.",
        [],
    )
    with pytest.raises(HttpError) as excinfo:
        create_notification(MagicMock(), create_notification_payload)
    assert "user email is required to sent notification to a user." in str(excinfo.value)
    mock_get_recipients.assert_called_once_with(
        payload["sent_to"],
        payload["org_slug"],
        payload["user_email"],
        payload["manager_or_above"],
    )
    mock_create_notification.assert_not_called()


@patch("ddpui.core.notifications_service.get_recipients")
@patch("ddpui.core.notifications_service.create_notification")
def test_create_notification_user_does_not_exist(mock_create_notification, mock_get_recipients):
    """
    tests the failure of api endpoint for create notification
    when user does not exists with provided email
    """
    payload = {
        "author": "test_author",
        "message": "test_message",
        "urgent": True,
        "scheduled_time": None,
        "sent_to": "single_user",
        "org_slug": None,
        "user_email": "nonexisting@gmail.com",
        "manager_or_above": False,
    }
    create_notification_payload = CreateNotificationPayloadSchema(**payload)
    mock_get_recipients.return_value = (
        "User with the provided email does not exist",
        [],
    )
    with pytest.raises(HttpError) as excinfo:
        create_notification(MagicMock(), create_notification_payload)
    assert "User with the provided email does not exist" in str(excinfo.value)
    mock_get_recipients.assert_called_once_with(
        payload["sent_to"],
        payload["org_slug"],
        payload["user_email"],
        payload["manager_or_above"],
    )
    mock_create_notification.assert_not_called()


def test_get_notification_history_success():
    """tests the success of api endpoint for fetching notification history"""
    with patch(
        "ddpui.core.notifications_service.get_notification_history"
    ) as mock_get_notification_history:
        mock_get_notification_history.return_value = (
            None,
            {"success": True, "res": []},
        )
        response = get_notification_history(MagicMock(), 1, 10)
        assert response["success"] is True
        assert isinstance(response["res"], list)
        assert all(isinstance(notification, dict) for notification in response["res"])
        mock_get_notification_history.assert_called_once_with(1, 10, read_status=None)


def test_get_notification_recipients_success():
    """tests the success of api endpoint for fetching notification recipients"""
    with patch(
        "ddpui.core.notifications_service.get_notification_recipients"
    ) as mock_get_notification_recipients:
        mock_get_notification_recipients.return_value = (
            None,
            {"success": True, "res": []},
        )
        response = get_notification_recipients(MagicMock(), 1)
        assert response["success"] is True
        assert isinstance(response["res"], list)
        assert all(isinstance(recipient, dict) for recipient in response["res"])
        mock_get_notification_recipients.assert_called_once_with(1)


def test_get_user_notifications_success(orguser):
    """tests the success of api endpoint for fetching user notifications for the OrgUser"""
    request = MagicMock()
    request.orguser = orguser
    with patch(
        "ddpui.core.notifications_service.fetch_user_notifications"
    ) as mock_get_user_notifications:
        mock_get_user_notifications.return_value = (None, {"success": True, "res": []})
        response = get_user_notifications(request, 1, 10)
        assert response["success"] is True
        assert isinstance(response["res"], list)
        assert all(isinstance(notification, dict) for notification in response["res"])
        mock_get_user_notifications.assert_called_once_with(orguser, 1, 10)


def test_mark_as_read_success(orguser):
    """tests the success of api endpoint for marking notification as read for the OrgUser"""
    request = MagicMock()
    request.orguser = orguser
    payload = UpdateReadStatusSchema(notification_id=1, read_status=True)
    with patch(
        "ddpui.core.notifications_service.mark_notification_as_read_or_unread"
    ) as mock_mark_as_read:
        mock_mark_as_read.return_value = (
            None,
            {"success": True, "message": "Notification updated successfully"},
        )
        response = mark_as_read(request, payload)
        assert response["success"] is True
        assert response["message"] is "Notification updated successfully"
        mock_mark_as_read.assert_called_once_with(orguser.id, 1, True)


def test_mark_as_read_invalid_notification_id(orguser):
    """
    tests the failure of api endpoint for marking notification
    as read with invalid notification id
    """
    request = MagicMock()
    request.orguser = orguser
    payload = UpdateReadStatusSchema(notification_id=0, read_status=True)  # Invalid ID

    with patch(
        "ddpui.core.notifications_service.mark_notification_as_read_or_unread"
    ) as mock_mark_as_read:
        mock_mark_as_read.return_value = (
            "Notification not found for the given user",
            None,
        )
        with pytest.raises(HttpError) as excinfo:
            mark_as_read(request, payload)
        assert "Notification not found for the given user" in str(excinfo.value)
        mock_mark_as_read.assert_called_once_with(orguser.id, 0, True)


def test_delete_notification_success():
    """tests the success of api endpoint for deleting scheduled notification"""

    with patch(
        "ddpui.core.notifications_service.delete_scheduled_notification"
    ) as mock_delete_notification:
        mock_delete_notification.return_value = (
            None,
            {
                "success": True,
                "message": "Notification with id: 1 has been successfully deleted",
            },
        )
        response = delete_notification(MagicMock(), notification_id=1)
        assert response["success"] is True
        assert response["message"] is "Notification with id: 1 has been successfully deleted"
        mock_delete_notification.assert_called_once_with(1)


def test_delete_notification_does_not_exist():
    """
    tests the failure of api endpoint for deleting a
    notification that does not exist
    """
    with patch(
        "ddpui.core.notifications_service.delete_scheduled_notification"
    ) as mock_delete_notification:
        mock_delete_notification.return_value = ("Notification does not exist.", None)
        with pytest.raises(HttpError) as excinfo:
            delete_notification(MagicMock(), notification_id=0)
        assert "Notification does not exist." in str(excinfo.value)
        mock_delete_notification.assert_called_once_with(0)


def test_delete_already_sent_notification():
    """
    tests the failure of api endpoint for deleting a
    notification that has already been sent
    """
    with patch(
        "ddpui.core.notifications_service.delete_scheduled_notification"
    ) as mock_delete_notification:
        mock_delete_notification.return_value = (
            "Notification has already been sent and cannot be deleted.",
            None,
        )
        with pytest.raises(HttpError) as excinfo:
            delete_notification(MagicMock(), notification_id=1)
        assert "Notification has already been sent and cannot be deleted." in str(excinfo.value)
        mock_delete_notification.assert_called_once_with(1)


def test_get_unread_notifications_count_success(orguser):
    """tests the success of api endpoint for fetching unread notification count"""
    request = MagicMock()
    request.orguser = orguser
    with patch(
        "ddpui.core.notifications_service.get_unread_notifications_count"
    ) as mock_get_unread_notifications_count:
        mock_get_unread_notifications_count.return_value = (
            None,
            {
                "success": True,
                "res": 0,
            },
        )
        response = get_unread_notifications_count(request)
        assert response["success"] is True
        assert response["res"] == 0
        mock_get_unread_notifications_count.assert_called_once_with(orguser)
