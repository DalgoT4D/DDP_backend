from typing import Tuple, Optional, Dict, Any, List
from datetime import datetime
from celery.result import AsyncResult
from django.core.paginator import Paginator
from ddpui.models.notifications import (
    Notification,
    NotificationRecipient,
    UserNotificationPreferences,
    NotificationCategory,
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
    sent_to: str, org_slug: str, user_email: str, manager_or_above: bool
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

    # Get or create notification preferences
    notification_preferences, _ = UserNotificationPreferences.objects.get_or_create(user=recipient)

    # Check if user is subscribed to this notification category
    if not notification_preferences.is_subscribed_to_category(notification.category):
        # Skip this recipient as they're not subscribed to this category
        return None

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

        # Check if user has email notifications enabled
        if notification_preferences.email_notifications_enabled:
            try:
                send_text_message(
                    recipient.user.email,
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
    category = getattr(notification_data, "category", "system")

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
    }

    return None, {
        "res": response,
        "errors": errors,
    }


# get notification history
def get_notification_history(
    page: int, limit: int, read_status: Optional[int] = None
) -> Tuple[Optional[None], Dict[str, Any]]:
    """returns history of sent notifications"""
    notifications = Notification.objects

    if read_status:
        notifications = notifications.filter(read_status=(read_status == 1))

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
    orguser: OrgUser, page: int, limit: int, read_status: int = None
) -> Tuple[Optional[None], Dict[str, Any]]:
    """returns all notifications for a specific user"""

    notifications = (
        NotificationRecipient.objects.filter(
            recipient=orguser,
            notification__sent_time__isnull=False,
            **({"read_status": read_status == 1} if read_status is not None else {}),
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


# New category-related functions
def get_user_notification_preferences(
    orguser: OrgUser,
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Get user notification preferences including category subscriptions
    """
    try:
        user_preference, created = UserPreferences.objects.get_or_create(orguser=orguser)
        return None, {"success": True, "res": user_preference.to_json()}
    except Exception as e:
        return f"Error retrieving user preferences: {str(e)}", None


def update_user_notification_preferences(
    orguser: OrgUser, preferences_data: dict
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Update user notification preferences including category subscriptions
    """
    try:
        user_preference, created = UserPreferences.objects.get_or_create(orguser=orguser)

        # Update preferences based on provided data
        for key, value in preferences_data.items():
            if hasattr(user_preference, key):
                setattr(user_preference, key, value)

        user_preference.save()
        return None, {"success": True, "message": "Preferences updated successfully"}
    except Exception as e:
        return f"Error updating user preferences: {str(e)}", None


def fetch_user_notifications_by_category(
    orguser: OrgUser, page: int, limit: int, category: str = None, urgent_only: bool = False
) -> Tuple[Optional[None], Dict[str, Any]]:
    """
    Returns notifications for a specific user filtered by category
    """
    # Base query for user's notifications
    user_notifications = (
        NotificationRecipient.objects.filter(recipient=orguser)
        .select_related("notification")
        .order_by("-notification__timestamp")
    )

    # Apply category filter
    if category:
        user_notifications = user_notifications.filter(notification__category=category)

    # Apply urgent filter
    if urgent_only:
        user_notifications = user_notifications.filter(notification__urgent=True)

    paginator = Paginator(user_notifications, limit)
    paginated_notifications = paginator.get_page(page)

    notification_list = [
        {
            "id": notification_recipient.notification.id,
            "author": notification_recipient.notification.author,
            "message": notification_recipient.notification.message,
            "email_subject": notification_recipient.notification.email_subject,
            "timestamp": notification_recipient.notification.timestamp,
            "urgent": notification_recipient.notification.urgent,
            "category": notification_recipient.notification.category,
            "scheduled_time": notification_recipient.notification.scheduled_time,
            "sent_time": notification_recipient.notification.sent_time,
            "read_status": notification_recipient.read_status,
        }
        for notification_recipient in paginated_notifications
    ]

    return None, {
        "success": True,
        "res": notification_list,
        "page": paginated_notifications.number,
        "total_pages": paginated_notifications.paginator.num_pages,
        "total_notifications": paginated_notifications.paginator.count,
    }


def get_urgent_notifications_for_user(
    orguser: OrgUser,
) -> Tuple[Optional[None], Dict[str, Any]]:
    """
    Get all unread urgent notifications for a user (for notification bar)
    """
    urgent_notifications = (
        NotificationRecipient.objects.filter(
            recipient=orguser, read_status=False, notification__urgent=True
        )
        .select_related("notification")
        .order_by("-notification__timestamp")
    )

    notification_list = [
        {
            "id": notification_recipient.notification.id,
            "author": notification_recipient.notification.author,
            "message": notification_recipient.notification.message,
            "timestamp": notification_recipient.notification.timestamp,
            "category": notification_recipient.notification.category,
        }
        for notification_recipient in urgent_notifications
    ]

    return None, {"success": True, "res": notification_list}


def create_categorized_notification(
    author: str,
    message: str,
    email_subject: str,
    category: str,
    recipients: List[int],
    urgent: bool = False,
    scheduled_time: Optional[datetime] = None,
) -> Tuple[Optional[Dict[str, str]], Optional[Dict[str, Any]]]:
    """
    Helper function to create notifications with specific categories
    """
    notification_data = NotificationDataSchema(
        author=author,
        message=message,
        email_subject=email_subject,
        urgent=urgent,
        scheduled_time=scheduled_time,
        recipients=recipients,
        category=category,
    )
    return create_notification(notification_data)


def get_user_notification_preferences(orguser: OrgUser) -> UserNotificationPreferences:
    """Get or create user notification preferences"""
    preferences, created = UserNotificationPreferences.objects.get_or_create(user=orguser)
    return preferences


def update_user_notification_preferences(
    orguser: OrgUser,
    email_notifications_enabled: bool = None,
    incident_notifications: bool = None,
    schema_change_notifications: bool = None,
    job_failure_notifications: bool = None,
    late_run_notifications: bool = None,
    dbt_test_failure_notifications: bool = None,
    system_notifications: bool = None,
) -> UserNotificationPreferences:
    """Update user notification preferences"""
    preferences = get_user_notification_preferences(orguser)

    if email_notifications_enabled is not None:
        preferences.email_notifications_enabled = email_notifications_enabled
    if incident_notifications is not None:
        preferences.incident_notifications = incident_notifications
    if schema_change_notifications is not None:
        preferences.schema_change_notifications = schema_change_notifications
    if job_failure_notifications is not None:
        preferences.job_failure_notifications = job_failure_notifications
    if late_run_notifications is not None:
        preferences.late_run_notifications = late_run_notifications
    if dbt_test_failure_notifications is not None:
        preferences.dbt_test_failure_notifications = dbt_test_failure_notifications
    if system_notifications is not None:
        preferences.system_notifications = system_notifications

    preferences.save()
    return preferences


def get_filtered_notifications(
    orguser: OrgUser,
    page: int = 1,
    limit: int = 10,
    category: str = None,
    urgent_only: bool = False,
    read_status: Optional[int] = None,
) -> Tuple[Optional[None], Dict[str, Any]]:
    """Get notifications filtered by category and other criteria"""
    notifications_query = NotificationRecipient.objects.filter(recipient=orguser)

    if category:
        notifications_query = notifications_query.filter(notification__category=category)

    if urgent_only:
        notifications_query = notifications_query.filter(notification__urgent=True)

    if read_status is not None:
        notifications_query = notifications_query.filter(read_status=(read_status == 1))

    notifications_query = notifications_query.order_by("-notification__timestamp")

    paginator = Paginator(notifications_query, limit)
    paginated_notifications = paginator.get_page(page)

    notification_list = [
        {
            "id": notification_recipient.notification.id,
            "message": notification_recipient.notification.message,
            "timestamp": notification_recipient.notification.timestamp,
            "urgent": notification_recipient.notification.urgent,
            "read_status": notification_recipient.read_status,
            "category": notification_recipient.notification.category,
            "author": notification_recipient.notification.author,
        }
        for notification_recipient in paginated_notifications
    ]

    return None, {
        "success": True,
        "res": notification_list,
        "page": page,
        "limit": limit,
        "total_pages": paginator.num_pages,
        "total_count": paginator.count,
    }


def get_urgent_notifications_for_user(orguser: OrgUser) -> List[Dict[str, Any]]:
    """Get urgent unread notifications for display in notification bar"""
    urgent_notifications = (
        NotificationRecipient.objects.filter(
            recipient=orguser, notification__urgent=True, read_status=False
        )
        .select_related("notification")
        .order_by("-notification__timestamp")
    )

    return [
        {
            "id": notif.notification.id,
            "message": notif.notification.message,
            "category": notif.notification.category,
            "timestamp": notif.notification.timestamp,
        }
        for notif in urgent_notifications
    ]


def create_job_failure_notification(
    flow_run_id: str, org: Org, error_message: str = "A data pipeline job has failed."
) -> None:
    """Create a job failure notification for all subscribed users in an org"""
    recipients = OrgUser.objects.filter(org=org).values_list("id", flat=True)

    create_categorized_notification(
        author="system@dalgo.ai",
        message=f"Job failure detected for flow run {flow_run_id}: {error_message}",
        email_subject="Data Pipeline Job Failure",
        category=NotificationCategory.JOB_FAILURE,
        recipients=list(recipients),
        urgent=False,
    )


def create_dbt_test_failure_notification(test_name: str, org: Org, error_details: str = "") -> None:
    """Create a dbt test failure notification"""
    recipients = OrgUser.objects.filter(org=org).values_list("id", flat=True)

    message = f"dbt test '{test_name}' has failed."
    if error_details:
        message += f" Details: {error_details}"

    create_categorized_notification(
        author="system@dalgo.ai",
        message=message,
        email_subject="dbt Test Failure",
        category=NotificationCategory.DBT_TEST_FAILURE,
        recipients=list(recipients),
        urgent=False,
    )


def create_late_run_notification(
    flow_name: str, org: Org, expected_time: str, delay_minutes: int
) -> None:
    """Create a late run notification"""
    recipients = OrgUser.objects.filter(org=org).values_list("id", flat=True)

    create_categorized_notification(
        author="system@dalgo.ai",
        message=f"Pipeline '{flow_name}' is running late. Expected at {expected_time}, delayed by {delay_minutes} minutes.",
        email_subject="Data Pipeline Running Late",
        category=NotificationCategory.LATE_RUN,
        recipients=list(recipients),
        urgent=False,
    )
