# views.py
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.models import User
from ddpui.models import Notification, UserPreference
import json
import requests
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


SENDGRID_APIKEY = os.getenv("SENDGRID_APIKEY")
SENDGRID_SENDER = os.getenv("SENDGRID_SENDER")


# Send notification via email
def send_email_notification(to_email, message):
    sendgrid_client = SendGridAPIClient(SENDGRID_APIKEY)
    email_message = Mail(from_email=SENDGRID_SENDER, to_emails=to_email, subject="Message from Dalgo Team", html_content=message)

    try:
        sendgrid_client.send(email_message)
    except Exception as error:
        raise Exception(f"Failed to send notification through mail. Response: {error}")


# send notification via discord
def send_discord_notification(webhook_url, message):
    data = {
        "content": message
    }

    response = requests.post(webhook_url, json=data)

    if response.status_code != 204:
        raise Exception(f"Failed to send notification. Status code: {response.status_code}, Response: {response.text}")


# send notification
def create_notifications(user_ids, message, author, urgent=False):
    if not user_ids or not message:
        return {'error': 'user_ids and message are required'}
        
    errors=[]
    notifications = []
    try:
        for user_id in user_ids:
            user = User.objects.get(id=user_id)
            user_preference = UserPreference.objects.get(user=user)

            if not user_preference.enable_notification:
                errors.append({f'User with user_id:{user_id} has not opted to receive notifications.'})
                continue

            # Send the notification based on the preferred channel
            try:
                if user_preference.enable_email:
                    send_email_notification(user_preference.email_id, message)

                if user_preference.enable_discord:
                    send_discord_notification(user_preference.discord_webhook, message)

            except Exception as e:
                errors.append({'user_id': user_id, 'error': str(e)})
                continue
            
            notification = Notification.objects.create(user=user, message=message, urgent=urgent, author=author)
            notifications.append({
                'id': notification.id,
                'user_id': user.id,
                'message': notification.message,
                'urgent': notification.urgent
            })


    except User.DoesNotExist:
            errors.append({f'User with id {user_id} does not exist'})
    except UserPreference.DoesNotExist:
        errors.append({f'User preference for user with id {user_id} does not exist'})

    return {'notifications': notifications,'errors': errors}


# get notification data
def get_notifications(user_id):
    user = User.objects.get(id=user_id)
    notifications = Notification.objects.filter(user=user)
    notifications_data = [{'id': n.id, 'message': n.message, 'created_at': n.created_at, 'read_status': n.read_status, 'urgent': n.urgent} for n in notifications]
    
    return notifications_data


# mark notificaiton as read
def mark_as_read(notification_id):
    try:
        notification = Notification.objects.get(id=notification_id)
        notification.read_status = True
        notification.save()
        return {'id': notification.id, 'status': notification.read_status}
    except Notification.DoesNotExist:
        return {'error': 'Notification not found'}
    

# delete notification
def delete_notification(notification_id):
    try:
        notification = Notification.objects.get(id=notification_id)
        notification.delete()
        return {'message': 'Notification deleted'}
    except Notification.DoesNotExist:
        return {'error': 'Notification not found'}
    


def set_user_preferences(user_id, enable_email=False, enable_discord=False, enable_notification=False, email_id=None, discord_webhook=None):
    try:
        user = User.objects.get(id=user_id)
        UserPreference.objects.create(
            user=user,
            email_id=email_id,
            discord_webhook=discord_webhook,
            enable_email=enable_email,
            enable_discord=enable_discord,
            enable_notification=enable_notification
        )
        return {'status': 'success', 'message': 'Preferences set successfully'}
    except User.DoesNotExist:
        return {'status': 'error', 'message': f'User with id {user_id} does not exist'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}
    

def update_user_preferences(user_id, enable_email=None, enable_discord=None, enable_notification=None, email_id=None, discord_webhook=None):
    try:
        user_preference = UserPreference.objects.get(user__id=user_id)
        if email_id is not None:
            user_preference.email_id = email_id
        if discord_webhook is not None:
            user_preference.discord_webhook = discord_webhook
        if enable_email is not None:
            user_preference.enable_email = enable_email
        if enable_notification is not None:
            user_preference.enable_notification = enable_notification
        if enable_discord is not None:
            user_preference.enable_discord = enable_discord

        user_preference.save()

        return {'status': 'success', 'message': 'Preferences updated successfully'}
    except UserPreference.DoesNotExist:
        return {'status': 'error', 'message': f'User preference for user with id {user_id} does not exist'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}
    

def get_user_preferences(user_id):
    try:
        user_preference = UserPreference.objects.get(user__id=user_id)
        preferences = {
            'email_id': user_preference.email_id,
            'user_id': user_id,
            'discord_webhook': user_preference.discord_webhook,
            'enable_email': user_preference.enable_email,
            'enable_discord': user_preference.enable_discord,
            'enable_notification': user_preference.enable_notification,
        }
        return {'status': 'success', 'preferences': preferences}
    except UserPreference.DoesNotExist:
        return {'status': 'error', 'message': f'User preference for user with id {user_id} does not exist'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}