# Generated by Django 4.2 on 2024-07-10 03:41

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0083_merge_20240703_1732"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="notificationrecipient",
            name="notification",
        ),
        migrations.RemoveField(
            model_name="notificationrecipient",
            name="recipient",
        ),
        migrations.DeleteModel(
            name="Notification",
        ),
        migrations.DeleteModel(
            name="NotificationRecipient",
        ),
    ]