import os
from datetime import datetime
from unittest.mock import patch
import django
import pytest
from django.utils import timezone
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.models.notifications import Notification, NotificationRecipient
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.userpreferences import UserPreferences
from ddpui.models.role_based_access import Permission, Role, RolePermission
from ddpui.core.notifications.notifications_functions import (
    get_recipients,
    handle_recipient,
    create_notification,
    get_notification_history,
    get_notification_recipients,
    fetch_user_notifications,
    fetch_user_notifications_v1,
    mark_notification_as_read_or_unread,
    delete_scheduled_notification,
    get_unread_notifications_count,
    mark_all_notifications_as_read,
)
from ddpui.schemas.notifications_api_schemas import SentToEnum, NotificationDataSchema
from ddpui.tests.api_tests.test_user_org_api import mock_request, seed_db

from django.contrib.auth.models import User


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
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def unsent_notification(orguser):
    """a pytest fixture representing a already sent notification"""
    notification = Notification.objects.create(
        author="test_author",
        message="Test message",
        urgent=False,
        scheduled_time=None,
    )
    NotificationRecipient.objects.create(
        notification=notification,
        recipient=orguser,
    )
    yield notification
    notification.delete()


@pytest.fixture
def sent_notification(orguser):
    """a pytest fixture representing a future scheduled notification"""
    notification = Notification.objects.create(
        author="test_author",
        message="Test message",
        urgent=False,
        scheduled_time=None,
        sent_time=datetime.now(),
    )
    NotificationRecipient.objects.create(
        notification=notification,
        recipient=orguser,
    )
    yield notification
    notification.delete()


@pytest.fixture
def scheduled_notification(orguser):
    """Create a Notification fixture for testing"""
    notification = Notification.objects.create(
        author="test_author",
        message="test_message",
        urgent=True,
        scheduled_time=timezone.now() + timezone.timedelta(days=1),
    )
    NotificationRecipient.objects.create(
        notification=notification,
        recipient=orguser,
    )
    yield notification
    notification.delete()


def test_seed_data(seed_db):
    """a test to seed the database"""
    assert Role.objects.count() == 4
    assert RolePermission.objects.count() > 5
    assert Permission.objects.count() > 5


def test_get_recipients_all_users(orguser):
    """test success get all users as recipients"""
    error, recipients = get_recipients(SentToEnum.ALL_USERS, None, None, False)
    assert error is None
    assert len(recipients) > 0


def test_get_recipients_all_users_no_user_found():
    """test failure no recipients found"""
    error, recipients = get_recipients(SentToEnum.ALL_USERS, None, None, False)
    assert error is not None
    assert recipients is None


def test_get_recipients_all_org_users(orguser):
    """test success get all users of an org as recipients"""
    error, recipients = get_recipients(SentToEnum.ALL_ORG_USERS, "test-org-slug", None, False)
    assert error is None
    assert len(recipients) > 0


def test_get_recipients_no_org_slug():
    """test failure get all users of an org as recipients when no slug is provided"""
    error, recipients = get_recipients(SentToEnum.ALL_ORG_USERS, None, None, False)
    assert error is not None
    assert recipients is None


def test_get_recipients_org_slugs_merges_several_orgs(orguser):
    """the admin multi-org audience: org_slugs merges recipients across every listed
    org into one recipient_ids list -- never a per-org breakdown (plan.md §4.3)"""
    other_org = Org.objects.create(airbyte_workspace_id=None, slug="test-org-slug-2")
    other_user = User.objects.create(
        username="tempusername2", email="tempuseremail2", password="tempuserpassword"
    )
    other_orguser = OrgUser.objects.create(
        user=other_user,
        org=other_org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )

    error, recipients = get_recipients(
        SentToEnum.ALL_ORG_USERS, None, None, False, org_slugs=["test-org-slug", "test-org-slug-2"]
    )

    assert error is None
    assert set(recipients) == {orguser.id, other_orguser.id}

    other_orguser.delete()
    other_org.delete()
    other_user.delete()


def test_get_recipients_org_slugs_bogus_slug_contributes_nothing(orguser):
    """a nonexistent slug inside a multi-org selection just matches nothing -- it
    does not error, mirroring the bulk-flag bogus-org_id behavior"""
    error, recipients = get_recipients(
        SentToEnum.ALL_ORG_USERS, None, None, False, org_slugs=["test-org-slug", "no-such-org"]
    )

    assert error is None
    assert recipients == [orguser.id]


def test_get_recipients_single_user(orguser):
    """test success get single user as recipient"""
    error, recipients = get_recipients(SentToEnum.SINGLE_USER, None, "tempuseremail", False)
    assert error is None
    assert len(recipients) == 1


def test_get_recipients_single_user_no_email(orguser):
    """test failure get single user as recipient when no user email is provided"""
    error, recipients = get_recipients(SentToEnum.SINGLE_USER, None, None, False)
    assert error is not None
    assert recipients is None


def test_get_recipients_invalid_user_email():
    """test failure get single user as recipient when user email is invalid"""
    error, recipients = get_recipients(SentToEnum.SINGLE_USER, None, "invalid@example.com", False)
    assert error is not None
    assert recipients is None


def test_handle_recipient_success(orguser, unsent_notification):
    error = handle_recipient(orguser.id, None, unsent_notification)
    assert error is None


@patch("ddpui.core.notifications.notifications_functions.send_text_message")
def test_handle_recipient_send_email_false_skips_email_even_if_opted_in(
    mock_send_text_message, orguser, unsent_notification
):
    """the admin's send_email=False narrows further -- it never overrides the
    recipient's own opt-in (plan.md §4.3)"""
    UserPreferences.objects.update_or_create(
        orguser=orguser, defaults={"enable_email_notifications": True}
    )
    unsent_notification.send_email = False
    unsent_notification.save()

    error = handle_recipient(orguser.id, None, unsent_notification)

    assert error is None
    mock_send_text_message.assert_not_called()


@patch("ddpui.core.notifications.notifications_functions.send_text_message")
def test_handle_recipient_send_email_true_still_requires_recipient_opt_in(
    mock_send_text_message, orguser, unsent_notification
):
    """send_email=True narrows nothing -- the recipient's own opt-out still wins"""
    UserPreferences.objects.update_or_create(
        orguser=orguser, defaults={"enable_email_notifications": False}
    )
    unsent_notification.send_email = True
    unsent_notification.save()

    error = handle_recipient(orguser.id, None, unsent_notification)

    assert error is None
    mock_send_text_message.assert_not_called()


def test_handle_recipient_with_scheduled_time(orguser, scheduled_notification):
    scheduled_time = timezone.now() + timezone.timedelta(days=1)
    error = handle_recipient(orguser.id, scheduled_time, scheduled_notification)
    assert error is None


def test_create_notification_success(orguser):
    notification_data = NotificationDataSchema(
        author="test_author",
        message="test_message",
        email_subject="test_subject",
        urgent=True,
        scheduled_time=None,
        recipients=[orguser.id],
    )

    error, result = create_notification(notification_data)
    assert error is None
    assert result is not None


def test_create_notification_persists_audience_and_channel_choice(orguser):
    """target_org_ids/send_in_app/send_email are additive on NotificationDataSchema
    and land on the Notification row at creation time -- before handle_recipient
    runs, so the channel gate sees the admin's real choice (plan.md §4.1, §4.3)"""
    notification_data = NotificationDataSchema(
        author="test_author",
        message="test_message",
        email_subject="test_subject",
        recipients=[orguser.id],
        target_org_ids=[orguser.org.id],
        send_in_app=False,
        send_email=True,
    )

    error, result = create_notification(notification_data)

    assert error is None
    notification = Notification.objects.get(id=result["res"]["notification_id"])
    assert notification.target_org_ids == [orguser.org.id]
    assert notification.send_in_app is False
    assert notification.send_email is True


def test_get_notification_history(unsent_notification):
    error, result = get_notification_history(1, 10)
    assert error is None
    assert result["success"] is True
    assert len(result["res"]) > 0


def test_get_notification_recipients(unsent_notification):
    error, result = get_notification_recipients(unsent_notification.id)
    assert error is None
    assert result["success"] is True
    assert len(result["res"]) >= 0


def test_get_notification_recipients_not_exist():
    error, result = get_notification_recipients(9999)
    assert error is not None
    assert result is None


def test_get_user_notifications(orguser):
    error, result = fetch_user_notifications(orguser, 1, 10)
    assert error is None
    assert result["success"] is True
    assert len(result["res"]) >= 0


def test_fetch_user_notifications_excludes_email_only_broadcast(orguser):
    """send_in_app=False never appears in the in-app list, even once sent
    (plan.md §4.3) -- an email-only broadcast is invisible here"""
    email_only = Notification.objects.create(
        author="test_author",
        message="email only",
        urgent=False,
        sent_time=datetime.now(),
        send_in_app=False,
    )
    NotificationRecipient.objects.create(notification=email_only, recipient=orguser)

    error, result = fetch_user_notifications(orguser, 1, 10)

    assert error is None
    assert email_only.id not in {n["id"] for n in result["res"]}

    email_only.delete()


def test_fetch_user_notifications_v1_excludes_email_only_broadcast(orguser):
    """same send_in_app exclusion for the v1 list variant"""
    email_only = Notification.objects.create(
        author="test_author",
        message="email only",
        urgent=False,
        sent_time=datetime.now(),
        send_in_app=False,
    )
    NotificationRecipient.objects.create(notification=email_only, recipient=orguser)

    error, result = fetch_user_notifications_v1(orguser, 1, 10)

    assert error is None
    assert email_only.id not in {n["id"] for n in result["res"]}

    email_only.delete()


def test_fetch_user_notifications_still_includes_a_default_channel_notification(orguser):
    """regression: a normal notification -- created without touching
    send_in_app/send_email at all, exactly like the management command and the
    pre-existing broken HTTP route do -- still appears, unaffected by the new
    filter (plan.md §4.1: defaults preserve today's behavior)"""
    normal = Notification.objects.create(
        author="test_author",
        message="normal broadcast",
        urgent=False,
        sent_time=datetime.now(),
    )
    NotificationRecipient.objects.create(notification=normal, recipient=orguser)

    error, result = fetch_user_notifications(orguser, 1, 10)

    assert error is None
    assert normal.id in {n["id"] for n in result["res"]}

    normal.delete()


def test_fetch_user_notifications_v1_still_includes_a_default_channel_notification(orguser):
    """same regression check for the v1 list variant"""
    normal = Notification.objects.create(
        author="test_author",
        message="normal broadcast",
        urgent=False,
        sent_time=datetime.now(),
    )
    NotificationRecipient.objects.create(notification=normal, recipient=orguser)

    error, result = fetch_user_notifications_v1(orguser, 1, 10)

    assert error is None
    assert normal.id in {n["id"] for n in result["res"]}

    normal.delete()


def test_mark_notification_as_read(orguser, unsent_notification):
    error, result = mark_notification_as_read_or_unread(orguser.id, unsent_notification.id, True)
    assert error is None
    assert result["success"] is True


def test_mark_notification_as_read_not_exist():
    error, result = mark_notification_as_read_or_unread(9999, 9999, True)
    assert error is not None
    assert result is None


def test_delete_scheduled_notification(unsent_notification):
    error, result = delete_scheduled_notification(unsent_notification.id)
    assert error is None
    assert result["success"] is True


def test_delete_scheduled_notification_already_sent(sent_notification):
    error, result = delete_scheduled_notification(sent_notification.id)
    assert error is not None
    assert result is None


def test_delete_scheduled_notification_not_exist():
    error, result = delete_scheduled_notification(9999)
    assert error is not None
    assert result is None


def test_count_unread_notifications_success(orguser):
    error, result = get_unread_notifications_count(orguser)
    assert error is None
    assert result["success"] is True
    assert result["res"] >= 0


def test_count_unread_notifications_excludes_email_only_broadcast(orguser):
    """an unread email-only broadcast (send_in_app=False) never inflates the
    in-app unread count (plan.md §4.3)"""
    before = get_unread_notifications_count(orguser)[1]["res"]

    email_only = Notification.objects.create(
        author="test_author",
        message="email only",
        urgent=False,
        sent_time=datetime.now(),
        send_in_app=False,
    )
    NotificationRecipient.objects.create(
        notification=email_only, recipient=orguser, read_status=False
    )

    error, result = get_unread_notifications_count(orguser)

    assert error is None
    assert result["res"] == before

    email_only.delete()


def test_count_unread_notifications_still_counts_a_default_channel_notification(orguser):
    """regression: an unread notification created without touching
    send_in_app at all still increments the count -- the new filter is a
    no-op for the default (plan.md §4.1)"""
    before = get_unread_notifications_count(orguser)[1]["res"]

    normal = Notification.objects.create(
        author="test_author",
        message="normal broadcast",
        urgent=False,
        sent_time=datetime.now(),
    )
    NotificationRecipient.objects.create(notification=normal, recipient=orguser, read_status=False)

    error, result = get_unread_notifications_count(orguser)

    assert error is None
    assert result["res"] == before + 1

    normal.delete()


def test_mark_all_notifications_as_read_success(orguser, unsent_notification):
    """test success mark all notifications as read"""
    # Create additional unread notifications for the same user
    notification2 = Notification.objects.create(
        author="test_author_2",
        message="Test message 2",
        urgent=False,
        scheduled_time=None,
        sent_time=datetime.now(),
    )
    NotificationRecipient.objects.create(
        notification=notification2,
        recipient=orguser,
        read_status=False,
    )

    # Ensure the first notification is also unread
    NotificationRecipient.objects.filter(
        notification=unsent_notification, recipient=orguser
    ).update(read_status=False)

    error, result = mark_all_notifications_as_read(orguser.id)
    assert error is None
    assert result["success"] is True
    assert result["updated_count"] >= 1

    # Clean up
    notification2.delete()


def test_mark_all_notifications_as_read_leaves_email_only_broadcast_unread(orguser):
    """the 'mark all as read' sweep never touches a send_in_app=False broadcast --
    it isn't part of the in-app inbox at all (plan.md §4.3)"""
    email_only = Notification.objects.create(
        author="test_author",
        message="email only",
        urgent=False,
        sent_time=datetime.now(),
        send_in_app=False,
    )
    recipient_row = NotificationRecipient.objects.create(
        notification=email_only, recipient=orguser, read_status=False
    )

    error, result = mark_all_notifications_as_read(orguser.id)

    assert error is None
    recipient_row.refresh_from_db()
    assert recipient_row.read_status is False

    email_only.delete()


def test_mark_all_notifications_as_read_no_unread_notifications(orguser):
    """test mark all notifications as read when no unread notifications exist"""
    error, result = mark_all_notifications_as_read(orguser.id)
    assert error is None
    assert result["success"] is True
    assert result["updated_count"] == 0


def test_mark_all_notifications_as_read_user_not_exist():
    """test failure mark all notifications as read when user doesn't exist"""
    error, result = mark_all_notifications_as_read(9999)
    assert error is None  # Function doesn't check if user exists, just updates matching records
    assert result["success"] is True
    assert result["updated_count"] == 0
