from typing import Tuple, Optional, Dict, Any, List
from datetime import datetime
from ddpui.models.notifications import (
    Notification,
    NotificationRecipient,
)
from ddpui.models.userpreferences import UserPreferences
from ddpui.models.org_user import OrgUser
from ddpui.utils import timezone
from ddpui.utils.discord import send_discord_notification
from ddpui.utils.sendgrid import send_email_notification
from ddpui.schemas.notifications_api_schemas import CreateNotificationSchema


# send notification
def create_notification(
    notification_data: CreateNotificationSchema,
) -> Tuple[Optional[Dict[str, str]], Optional[Dict[str, Any]]]:
    if not notification_data:
        return {
            "message": "Error sending discord notification: notifications_data is required"
        }, None

    errors = []

    author = notification_data.author
    message = notification_data.message
    urgent = notification_data.urgent
    scheduled_time = notification_data.scheduled_time
    recipients = notification_data.recipients

    if not author or not message or not recipients:
        return (
            "author, message, and recipients are required for each notification",
            None,
        )

    for recipient_id in recipients:
        try:
            recipient = OrgUser.objects.get(user_id=recipient_id)
            user_preference, created = UserPreferences.objects.get_or_create(
                orguser=recipient
            )
            notification = Notification.objects.create(
                author=author,
                message=message,
                urgent=urgent,
                scheduled_time=scheduled_time,
            )
            notification_recipient = NotificationRecipient.objects.create(
                notification=notification, recipient=recipient
            )
            if scheduled_time:
                # logic for scheduling notification goes here
                notification_recipient.save()
            else:
                notification.sent_time = timezone.as_utc(datetime.utcnow())
                notification.save()

                if user_preference.enable_email_notifications:
                    try:
                        send_email_notification(
                            user_preference.orguser.user.email, notification.message
                        )
                    except Exception as e:
                        errors.append(
                            {
                                "recipient": notification_recipient.recipient.user.email,
                                "error": f"Error sending email notification: {str(e)}",
                            }
                        )

                if (
                    user_preference.enable_discord_notifications
                    and user_preference.discord_webhook
                ):
                    try:
                        send_discord_notification(
                            user_preference.discord_webhook, notification.message
                        )
                    except Exception as e:
                        errors.append(
                            {
                                "recipient": notification_recipient.recipient.user.email,
                                "error": f"Error sending discord notification: {str(e)}",
                            }
                        )

        except OrgUser.DoesNotExist:
            errors.append(
                {
                    "recipient": notification_recipient.recipient.user.email,
                    "error": "Recipient does not exist",
                }
            )

    response = {
        "notification_id": notification.id,
        "message": notification.message,
        "urgent": notification.urgent,
        "sent_time": notification.sent_time,
        "scheduled_time": notification.scheduled_time,
        "author": notification.author,
    }

    return None, {"res": response, "errors": errors}


# get notification history
def get_notification_history() -> Tuple[Optional[Dict[str, str]], List[Dict[str, Any]]]:
    notifications = Notification.objects.all().order_by("-timestamp")
    notification_history = []

    for notification in notifications:
        recipients = NotificationRecipient.objects.filter(notification=notification)
        recipient_list = [
            {
                "username": recipient.recipient.user.username,
                "read_status": recipient.read_status,
            }
            for recipient in recipients
        ]

        notification_history.append(
            {
                "id": notification.id,
                "author": notification.author,
                "message": notification.message,
                "timestamp": notification.timestamp,
                "urgent": notification.urgent,
                "scheduled_time": notification.scheduled_time,
                "sent_time": notification.sent_time,
                "recipients": recipient_list,
            }
        )

    return None, notification_history


# get notification data
def get_user_notifications(
    orguser: OrgUser,
) -> Tuple[Optional[None], Dict[str, List[Dict[str, Any]]]]:

    notifications = (
        NotificationRecipient.objects.filter(
            recipient=orguser, notification__sent_time__isnull=False
        )
        .select_related("notification")
        .order_by("-notification__timestamp")
    )

    user_notifications = []

    for recipient in notifications:
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

    return None, {"res": user_notifications}


# mark notificaiton as read
def mark_notification_as_read_or_unread(
    user_id: int, notification_id: int, read_status: bool
) -> Tuple[Optional[Dict[str, str]], Optional[Dict[str, str]]]:
    try:
        notification_recipient = NotificationRecipient.objects.get(
            recipient__user_id=user_id, notification__id=notification_id
        )
        notification_recipient.read_status = read_status
        notification_recipient.save()
        return None, {"success": True, "message": "Notification updated successfully"}
    except NotificationRecipient.DoesNotExist:
        return "Notification not found for the given user", None


# delete notification
def delete_scheduled_notification(
    notification_id: int,
) -> Tuple[Optional[Dict[str, str]], Optional[Dict[str, str]]]:
    try:
        notification = Notification.objects.get(id=notification_id)

        if notification.sent_time is not None:
            return "Notification has already been sent and cannot be deleted.", None

        notification_recipients = NotificationRecipient.objects.filter(
            notification=notification
        )

        # for recipient in notification_recipients:
        #     task_id = recipient.task_id
        #     async_result = AsyncResult(task_id)
        #     async_result.revoke(terminate=True)

        notification.delete()
        notification_recipients.delete()

        return None, {
            "message": f"Notification with id: {notification_id} has been successfully deleted"
        }

    except Notification.DoesNotExist:
        return "Notification does not exist.", None
