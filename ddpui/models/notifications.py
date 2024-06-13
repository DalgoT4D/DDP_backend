from django.db import models
from django.contrib.auth.models import User

class Notification(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='notifications')
    message = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=20, default='unread')
    urgent = models.BooleanField(default=False)
    channel = models.CharField(max_length=20, default='dashboard')

class UserPreference(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='preference')
    enable_email = models.BooleanField(default=True)
    enable_discord = models.BooleanField(default=True)
    email_id = models.EmailField(blank=True, null=True)
    discord_webhook = models.URLField(blank=True, null=True)
    enable_notification = models.BooleanField(default=True)