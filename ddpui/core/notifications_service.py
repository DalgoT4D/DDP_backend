from datetime import datetime
from ddpui.models.notifications import Notification, NotificationRecipient, UserPreference
# from ddpui.celeryworkers.tasks import schedule_notification_task
from ddpui.models.org_user import OrgUser
from ddpui.utils import timezone
from ddpui.utils.discord import send_discord_notification
from ddpui.utils.sendgrid import send_email_notification
from celery.result import AsyncResult



# send notification
def create_notification(notification_data):
    if not notification_data:
        return {'error': 'notifications_data is required'}

    notification = {}
    errors = []

    author = notification_data.get('author')
    message = notification_data.get('message')
    urgent = notification_data.get('urgent', False)
    scheduled_time = notification_data.get('scheduled_time', None)
    recipients = notification_data.get('recipients', [])

    if not author or not message or not recipients:
        return {'author': author, 'error': 'author, message, and recipients are required for each notification'}
    
    for recipient_id in recipients:
        try:
            recipient = OrgUser.objects.get(id=recipient_id)
            user_preference = UserPreference.objects.get(orguser=recipient)
            notification = Notification.objects.create(
                author=author,
                message=message,
                urgent=urgent,
                scheduled_time=scheduled_time
            )
            notification_recipient = NotificationRecipient.objects.create(notification=notification, recipient=recipient)
            
            if scheduled_time:
                # logic for scheduling notification goes here
                # result = schedule_notification_task.apply_async((notification.id, recipient_id), eta=scheduled_time)
                # notification_recipient.task_id = result.task_id
                notification_recipient.save()
            else:
                notification.sent_time = timezone.as_utc(datetime.utcnow())
                notification.save()

                if user_preference.enable_email_notifications:
                    try:
                        send_email_notification(user_preference.orguser.user.email, notification.message)
                    except Exception as e:
                        raise Exception(f"Error sending discord notification: {str(e)}")
                    
                if user_preference.enable_discord_notifications and user_preference.discord_webhook:
                    try:
                        send_discord_notification(user_preference.discord_webhook, notification.message)
                    except Exception as e:
                        raise Exception(f"Error sending discord notification: {str(e)}")
        
        except OrgUser.DoesNotExist:
            errors.append({'recipient_id': recipient_id, 'error': 'Recipient does not exist'})
        except UserPreference.DoesNotExist:
            errors.append({'recipient_id': recipient_id, 'error': 'User preference for given id does not exist'})


    return {'res': notification, 'errors': errors}


# get notification history
def get_notification_history():
    notifications = Notification.objects.all().order_by('-timestamp')
    notification_history = []

    for notification in notifications:
        recipients = NotificationRecipient.objects.filter(notification=notification)
        recipient_list = [{'id': recipient.recipient_id, 'username': recipient.recipient.user.username, 'read_status': recipient.read_status} for recipient in recipients]
        
        notification_history.append({
            'id': notification.id,
            'author': notification.author,
            'message': notification.message,
            'timestamp': notification.timestamp,
            'urgent': notification.urgent,
            'scheduled_time': notification.scheduled_time,
            'sent_time': notification.sent_time,
            'recipients': recipient_list
        })

    return notification_history

# get notification data
def get_user_notifications(orguser):

    notifications = NotificationRecipient.objects.filter(
        recipient=orguser,
        notification__sent_time__isnull=False
    ).select_related('notification').order_by('-notification__timestamp')

    user_notifications = []

    for recipient in notifications:
        notification = recipient.notification
        user_notifications.append({
            'id': notification.id,
            'author': notification.author,
            'message': notification.message,
            'timestamp': notification.timestamp,
            'urgent': notification.urgent,
            'scheduled_time': notification.scheduled_time,
            'sent_time': notification.sent_time,
            'read_status': recipient.read_status
        })

    return {'orguser': orguser, 'notifications': user_notifications}


# mark notificaiton as read
def mark_notification_as_read_or_unread(orguser_id, notification_id, read_status):
    try:
        notification_recipient = NotificationRecipient.objects.get(
            recipient__id=orguser_id,
            notification__id=notification_id
        )
        notification_recipient.read_status = read_status
        notification_recipient.save()
        return {'success': True, 'message': 'Notification updated successfully'}
    except NotificationRecipient.DoesNotExist:
        return {'success': False, 'message': 'Notification not found for the given user'}
    

# delete notification
def delete_scheduled_notification(notification_id):
    try:
        notification = Notification.objects.get(id=notification_id)
        
        if notification.sent_time is not None:
            return {'error': 'Notification has already been sent and cannot be deleted.'}
        
        notification_recipients = NotificationRecipient.objects.filter(notification=notification)
        
        for recipient in notification_recipients:
            task_id = recipient.task_id
            async_result = AsyncResult(task_id)
            async_result.revoke(terminate=True)

        notification.delete()
        notification_recipients.delete()
    
    except Notification.DoesNotExist:
        return {'error': 'Notification does not exist.'}