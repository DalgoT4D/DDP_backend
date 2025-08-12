from typing import Tuple, Optional, Dict, Any, List
from datetime import datetime
from celery.result import AsyncResult
from django.core.paginator import Paginator
from ddpui.models.notifications import (
    Notification,
    NotificationRecipient,
)
from ddpui.models.userpreferences import UserPreferences
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.org_preferences import OrgPreferences
from ddpui.utils import timezone
from ddpui.utils.discord import send_discord_notification
from ddpui.utils.awsses import send_text_message
from ddpui.schemas.notifications_api_schemas import SentToEnum, NotificationDataSchema
from ddpui.celeryworkers.moretasks import schedule_notification_task


def get_recipients(
    sent_to: str,
    org_slug: str,
    user_email: str,
    manager_or_above: bool,
    category: Optional[str] = None,
) -> Tuple[Optional[str], Optional[List[int]]]:
    """Returns the list of recipients based on the request parameters"""

    recipients = []
    # send to all users
    if sent_to == SentToEnum.ALL_USERS:
        recipients = OrgUser.objects.all().values_list("id", flat=True)

    # send to all users in an org
    elif sent_to == SentToEnum.ALL_ORG_USERS:
        if org_slug:
            recipients = OrgUser.objects.filter(org__slug=org_slug).values_list("id", flat=True)
        else:
            return "org_slug is required to sent notification to all org users.", None

    # send to a single user
    elif sent_to == SentToEnum.SINGLE_USER:
        if user_email:
            try:
                recipients = OrgUser.objects.filter(user__email=user_email).values_list(
                    "id", flat=True
                )
            except OrgUser.DoesNotExist:
                return "User with the provided email does not exist", None
        else:
            return "user email is required to sent notification to a user.", None

    # role based filtering
    if manager_or_above and sent_to != SentToEnum.SINGLE_USER:
        recipients = OrgUser.objects.filter(new_role_id__lte=3, id__in=recipients).values_list(
            "id", flat=True
        )

    # Map category string to the correct field name
    category_field_mapping = {
        "incident": "subscribe_incident_notifications",
        "schema_change": "subscribe_schema_change_notifications",
        "job_failure": "subscribe_job_failure_notifications",
        "late_runs": "subscribe_late_runs_notifications",
        "dbt_test_failure": "subscribe_dbt_test_failure_notifications",
    }

    preference_field = category_field_mapping.get(category)

    if preference_field:
        #  Filter recipients by subscription preference
        recipients = (
            OrgUser.objects.filter(
                id__in=recipients,
                preferences__isnull=False,
            )
            .filter(**{f"preferences__{preference_field}": True})
            .values_list("id", flat=True)
        )

    # If no recipients found, return an error message
    if not recipients:
        return "No users found for the given information", None

    return None, list(recipients.distinct())


# manage recipients for a notification
def handle_recipient(
    recipient_id: int, scheduled_time: Optional[datetime], notification: Notification
) -> Optional[Dict[str, str]]:
    """
    Add recipients to the recipients table and
    sent notification through email and discord
    """
    recipient = OrgUser.objects.get(id=recipient_id)
    user_preference, created = UserPreferences.objects.get_or_create(orguser=recipient)

    # Check if user is subscribed to this notification category
    if not user_preference.is_subscribed_to_category(notification.category):
        return None  # Skip sending notification if user is not subscribed

    notification_recipient = NotificationRecipient.objects.create(
        notification=notification, recipient=recipient
    )
    if scheduled_time:
        result = schedule_notification_task.apply_async(
            (notification.id, recipient_id), eta=scheduled_time
        )
        notification_recipient.task_id = result.task_id
        notification_recipient.save()
    else:
        notification.sent_time = timezone.as_utc(datetime.now())
        notification.save()

        if user_preference.enable_email_notifications:
            try:
                send_text_message(
                    user_preference.orguser.user.email,
                    notification.email_subject,
                    notification.message,
                )
            except Exception as e:
                return {
                    "recipient": notification_recipient.recipient.user.email,
                    "error": f"Error sending email notification: {str(e)}",
                }

    return None


# main function for sending notification
def create_notification(
    notification_data: NotificationDataSchema,
) -> Tuple[Optional[Dict[str, str]], Optional[Dict[str, Any]]]:
    """
    main function for creating notification.
    Add notification to the notification table.
    """

    author = notification_data.author
    message = notification_data.message
    email_subject = notification_data.email_subject
    urgent = notification_data.urgent
    scheduled_time = notification_data.scheduled_time
    recipients = notification_data.recipients
    category = notification_data.category

    errors = []
    notification = Notification.objects.create(
        author=author,
        message=message,
        email_subject=email_subject,
        urgent=urgent,
        scheduled_time=scheduled_time,
        category=category,
    )

    if not notification:
        return {"message": "Failed to sent notification."}, None

    org_ids = set()
    for recipient_id in recipients:
        recipient_orguser = OrgUser.objects.get(id=recipient_id)
        if recipient_orguser.org:
            org_ids.add(recipient_orguser.org.id)
            error = handle_recipient(recipient_id, scheduled_time, notification)
            if error:
                errors.append(error)

    for org_id in org_ids:
        org = Org.objects.get(id=org_id)
        if hasattr(org, "preferences"):
            orgpreferences: OrgPreferences = org.preferences
            if orgpreferences.enable_discord_notifications and orgpreferences.discord_webhook:
                try:
                    send_discord_notification(orgpreferences.discord_webhook, notification.message)
                except Exception as e:
                    errors.append(f"Error sending discord message: {e}")

    response = {
        "notification_id": notification.id,
        "message": notification.message,
        "urgent": notification.urgent,
        "sent_time": notification.sent_time,
        "scheduled_time": notification.scheduled_time,
        "author": notification.author,
        "category": notification.category,
    }

    return None, {
        "res": response,
        "errors": errors,
    }


# get notification history
def get_notification_history(
    page: int, limit: int, read_status: Optional[int] = None, category: Optional[str] = None
) -> Tuple[Optional[None], Dict[str, Any]]:
    """returns history of sent notifications"""
    notifications = Notification.objects

    if read_status:
        notifications = notifications.filter(read_status=(read_status == 1))

    if category:
        notifications = notifications.filter(category=category)

    notifications = notifications.all().order_by("-timestamp")

    paginator = Paginator(notifications, limit)
    paginated_notifications: list[Notification] = paginator.get_page(page)

    notification_history = [
        {
            "id": notification.id,
            "author": notification.author,
            "message": notification.message,
            "timestamp": notification.timestamp,
            "urgent": notification.urgent,
            "scheduled_time": notification.scheduled_time,
            "sent_time": notification.sent_time,
            "category": notification.category,
        }
        for notification in paginated_notifications
    ]

    return None, {
        "success": True,
        "res": notification_history,
        "page": paginated_notifications.number,
        "total_pages": paginated_notifications.paginator.num_pages,
        "total_notifications": paginated_notifications.paginator.count,
    }


# get notification recipients
def get_notification_recipients(
    notification_id: int,
) -> Tuple[Optional[None], Dict[str, Any]]:
    """returns recipients for a particular notification"""
    try:
        notification = Notification.objects.get(id=notification_id)

        recipients = NotificationRecipient.objects.filter(notification=notification).distinct()

        recipient_list = [
            {
                "username": recipient.recipient.user.username,
                "read_status": recipient.read_status,
            }
            for recipient in recipients
        ]

        return None, {"success": True, "res": recipient_list}

    except Notification.DoesNotExist:
        return "Notification does not exist.", None


# get notification data
def fetch_user_notifications(
    orguser: OrgUser, page: int, limit: int
) -> Tuple[Optional[None], Dict[str, Any]]:
    """returns all notifications for a specific user"""

    notifications = (
        NotificationRecipient.objects.filter(
            recipient=orguser, notification__sent_time__isnull=False
        )
        .select_related("notification")
        .order_by("-notification__timestamp")
    )

    paginator = Paginator(notifications, limit)
    paginated_notifications = paginator.get_page(page)

    user_notifications = []

    for recipient in paginated_notifications:
        notification = recipient.notification
        user_notifications.append(
            {
                "id": notification.id,
                "author": notification.author,
                "message": notification.message,
                "timestamp": notification.timestamp,
                "urgent": notification.urgent,
                "scheduled_time": notification.scheduled_time,
                "sent_time": notification.sent_time,
                "read_status": recipient.read_status,
            }
        )

    return None, {
        "success": True,
        "res": user_notifications,
        "page": paginated_notifications.number,
        "total_pages": paginated_notifications.paginator.num_pages,
        "total_notifications": paginated_notifications.paginator.count,
    }


def fetch_user_notifications_v1(
    orguser: OrgUser, page: int, limit: int, read_status: int = None, category: Optional[str] = None
) -> Tuple[Optional[None], Dict[str, Any]]:
    """returns all notifications for a specific user"""

    filter_kwargs = {
        "recipient": orguser,
        "notification__sent_time__isnull": False,
    }

    if read_status is not None:
        filter_kwargs["read_status"] = read_status == 1

    if category:
        filter_kwargs["notification__category"] = category

    notifications = (
        NotificationRecipient.objects.filter(**filter_kwargs)
        .select_related("notification")
        .order_by("-notification__timestamp")
    )

    paginator = Paginator(notifications, limit)
    paginated_notifications = paginator.get_page(page)

    user_notifications = []

    for recipient in paginated_notifications:
        notification = recipient.notification
        user_notifications.append(
            {
                "id": notification.id,
                "author": notification.author,
                "message": notification.message,
                "timestamp": notification.timestamp,
                "urgent": notification.urgent,
                "scheduled_time": notification.scheduled_time,
                "sent_time": notification.sent_time,
                "read_status": recipient.read_status,
                "category": notification.category,
            }
        )

    return None, {
        "success": True,
        "res": user_notifications,
        "page": paginated_notifications.number,
        "total_pages": paginated_notifications.paginator.num_pages,
        "total_notifications": paginated_notifications.paginator.count,
    }


# mark notificaiton as read
def mark_notification_as_read_or_unread(
    orguser_id: int, notification_id: int, read_status: bool
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """update the read status of a recipient for a notification"""
    try:
        notification_recipient = NotificationRecipient.objects.get(
            recipient__id=orguser_id, notification__id=notification_id
        )
        notification_recipient.read_status = read_status
        notification_recipient.save()
        return None, {"success": True, "message": "Notification updated successfully"}
    except NotificationRecipient.DoesNotExist:
        return "Notification not found for the given user", None


def mark_notifications_as_read_or_unread(
    orguser_id: int, notification_ids: int, read_status: bool
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """bulk update  of the read status of a recipient for notifications"""
    try:
        NotificationRecipient.objects.filter(
            recipient__id=orguser_id,
            notification__id__in=notification_ids,
        ).update(read_status=read_status)
        return None, {"success": True, "message": "Notifications updated successfully"}
    except NotificationRecipient.DoesNotExist:
        return "Something went wrong updating the notifications", None


# delete notification
def delete_scheduled_notification(
    notification_id: int,
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """deletes the futute scheduled notifications"""
    try:
        notification = Notification.objects.get(id=notification_id)

        if notification.sent_time is not None:
            return "Notification has already been sent and cannot be deleted.", None

        notification_recipients = NotificationRecipient.objects.filter(notification=notification)

        # removing notification from celery queue
        for recipient in notification_recipients:
            task_id = recipient.task_id
            async_result = AsyncResult(task_id)
            async_result.revoke(terminate=True)

        notification.delete()
        notification_recipients.delete()

        return None, {
            "success": True,
            "message": f"Notification with id: {notification_id} has been successfully deleted",
        }

    except Notification.DoesNotExist:
        return "Notification does not exist.", None


# get count of unread notifications
def get_unread_notifications_count(
    orguser: OrgUser,
) -> Tuple[Optional[None], Dict[str, Any]]:
    """
    Returns the count of unread notifications for a specific user.
    """
    unread_count = NotificationRecipient.objects.filter(
        recipient=orguser, read_status=False
    ).count()

    return None, {"success": True, "res": unread_count}


# get urgent notifications that haven't been dismissed
def get_urgent_notifications(
    orguser: OrgUser,
) -> Tuple[Optional[None], Dict[str, Any]]:
    """
    Returns urgent notifications that haven't been dismissed by the user.
    """
    urgent_notifications = (
        NotificationRecipient.objects.filter(
            recipient=orguser,
            notification__urgent=True,
            notification__sent_time__isnull=False,
            read_status=False,
        )
        .select_related("notification")
        .order_by("-notification__timestamp")
    )

    notifications_list = []
    for recipient in urgent_notifications:
        notification = recipient.notification
        notifications_list.append(
            {
                "id": notification.id,
                "author": notification.author,
                "message": notification.message,
                "timestamp": notification.timestamp,
                "category": notification.category,
                "read_status": recipient.read_status,
            }
        )

    return None, {"success": True, "res": notifications_list}


# get notifications by category
def get_notifications_by_category(
    orguser: OrgUser, category: str, page: int = 1, limit: int = 10
) -> Tuple[Optional[None], Dict[str, Any]]:
    """
    Get notifications for a specific category for the user.
    """
    return fetch_user_notifications_v1(orguser, page, limit, category=category)
