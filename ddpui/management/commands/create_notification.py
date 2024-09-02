import sys
from django.core.management.base import BaseCommand
from django.utils.dateparse import parse_datetime
from ddpui.core import notifications_service
from ddpui.schemas.notifications_api_schemas import (
    CreateNotificationPayloadSchema,
    SentToEnum,
)


class Command(BaseCommand):
    """Creates a notification"""

    help = "Send notification to a user or a group of users"

    def add_arguments(self, parser):
        """adds command line arguments"""
        parser.add_argument("author", type=str, help="Author of the notification")
        parser.add_argument("message", type=str, help="Message of the notification")
        parser.add_argument(
            "sent_to",
            type=str,
            choices=[sent_to.value for sent_to in SentToEnum],
            help="Target audience of the notification (e.g., 'all_users', 'single_user')",
        )
        parser.add_argument(
            "--user_email",
            type=str,
            help="Email address of the single user if sent_to is 'single_user'",
        )
        parser.add_argument(
            "--org_slug", type=str, help="Org slug if sent_to is 'all_org_users'"
        )
        parser.add_argument(
            "--manager_or_above",
            action="store_true",
            help="Only applicable if sent_to is 'all_org_users'; flag to include managers or above",
        )
        parser.add_argument(
            "--urgent",
            action="store_true",
            help="Whether the notification is urgent or not",
        )
        parser.add_argument(
            "--scheduled_time",
            type=str,
            help="The scheduled time of the notification in ISO 8601 format (e.g., 2024-08-30T19:51:51Z)",
        )

    def handle(self, *args, **options):
        # Parse scheduled_time
        scheduled_time_str = options.get("scheduled_time")
        scheduled_time = None
        if scheduled_time_str:
            try:
                scheduled_time = parse_datetime(scheduled_time_str)
                if not scheduled_time:
                    raise ValueError("Invalid date format.")
            except ValueError as e:
                print(f"Error parsing scheduled_time: {e}")
                sys.exit(1)

        # Prepare payload
        payload = CreateNotificationPayloadSchema(
            author=options["author"],
            message=options["message"],
            sent_to=SentToEnum(options["sent_to"]),
            urgent=options.get("urgent", False),
            scheduled_time=scheduled_time,
            user_email=options.get("user_email"),
            org_slug=options.get("org_slug"),
            manager_or_above=options.get("manager_or_above", False),
        )

        # Get recipients based on sent_to field
        error, recipients = notifications_service.get_recipients(
            payload.sent_to,
            payload.org_slug,
            payload.user_email,
            payload.manager_or_above,
        )

        if error:
            print(f"Error in getting recipients: {error}")
            sys.exit(1)

        # Create notification data
        notification_data = {
            "author": payload.author,
            "message": payload.message,
            "urgent": payload.urgent,
            "scheduled_time": payload.scheduled_time,
            "recipients": recipients,
        }

        # Call the create notification service
        error, result = notifications_service.create_notification(notification_data)

        if error:
            print(f"Error in creating notification: {error}")
            sys.exit(1)

        print(f"Notification created successfully: {result}")
