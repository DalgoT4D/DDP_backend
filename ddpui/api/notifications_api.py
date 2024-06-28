from ddpui.models.notifications import Notification, NotificationRecipient, UserPreference
from celery.app.control import Control
from ddpui.celeryworkers.tasks import schedule_notification_task
from ddpui.models.org_user import OrgUser



# send notification
def create_notifications(notifications_data):
    if not notifications_data:
        return {'error': 'notifications_data is required'}

    notifications = []
    errors = []
    
    for notification_data in notifications_data:
        author = notification_data.get('author')
        message = notification_data.get('message')
        urgency_level = notification_data.get('urgency_level', False)
        scheduled_time = notification_data.get('scheduled_time', None)
        recipients = notification_data.get('recipients', [])

        if not author or not message or not recipients:
            errors.append({'author': author, 'error': 'author, message, and recipients are required for each notification'})
            continue
        
        for recipient_id in recipients:
            try:
                recipient = OrgUser.objects.get(user_id=recipient_id).user
                user_preference = UserPreference.objects.get(user__user=recipient)
                notification = Notification.objects.create(
                    author=author,
                    message=message,
                    urgency_level=urgency_level,
                    scheduled_time=scheduled_time
                )
                NotificationRecipient.objects.create(notification=notification, recipient=recipient)
                
                if scheduled_time:
                    schedule_notification_task.apply_async((notification, recipient, user_preference), eta=scheduled_time)
                else:
                    schedule_notification_task.delay(notification, recipient, user_preference)
            
            except OrgUser.DoesNotExist:
                errors.append({'recipient_id': recipient_id, 'error': 'Recipient does not exist'})
            except UserPreference.DoesNotExist:
                errors.append({'recipient_id': recipient_id, 'error': 'User preference for given id does not exist'})

        notifications.append({
            'id': notification.id,
            'author': author,
            'message': notification.message,
            'urgency_level': notification.urgency_level,
            'scheduled_time': notification.scheduled_time,
            'sent_time': notification.sent_time
        })

    return {'notifications': notifications, 'errors': errors}


# get notification history
def get_notification_history():
    notifications = Notification.objects.all().order_by('-timestamp')
    notification_history = []

    for notification in notifications:
        recipients = NotificationRecipient.objects.filter(notification=notification)
        recipient_list = [{'id': recipient.recipient.id, 'username': recipient.recipient.username, 'read_status': recipient.read_status} for recipient in recipients]
        
        notification_history.append({
            'id': notification.id,
            'author': notification.author,
            'message': notification.message,
            'timestamp': notification.timestamp,
            'urgency_level': notification.urgency_level,
            'scheduled_time': notification.scheduled_time,
            'sent_time': notification.sent_time,
            'recipients': recipient_list
        })

    return notification_history

# get notification data
def get_user_notifications(user):

    notifications = NotificationRecipient.objects.filter(
        recipient=user,
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
            'urgency_level': notification.urgency_level,
            'scheduled_time': notification.scheduled_time,
            'sent_time': notification.sent_time,
            'read_status': recipient.read_status
        })

    return {'user': user, 'notifications': user_notifications}


# mark notificaiton as read
def mark_notification_as_read_or_unread(user_id, notification_id, read_status):
    try:
        notification_recipient = NotificationRecipient.objects.get(
            recipient_id=user_id,
            notification_id=notification_id
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
        
        # Revoke the scheduled Celery tasks
        notification_recipients = NotificationRecipient.objects.filter(notification=notification)
        
        control = Control(app=schedule_notification_task.app)
        
        for recipient in notification_recipients:
            task_id = schedule_notification_task.AsyncResult((notification.id, recipient.id)).task_id
            if task_id:
                control.revoke(task_id, terminate=True)
        
        # Delete the notification and its recipients
        notification.delete()
        notification_recipients.delete()
        
        return {'success': 'Scheduled notification has been successfully deleted.'}
    
    except Notification.DoesNotExist:
        return {'error': 'Notification does not exist.'}