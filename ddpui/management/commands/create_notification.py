import sys
from django.core.management.base import BaseCommand
from django.utils.dateparse import parse_datetime
from ddpui.core import notifications_service
from ddpui.schemas.notifications_api_schemas import (
    SentToEnum,
    NotificationDataSchema,
)
from ddpui.models.org import Org


class Command(BaseCommand):
    """Creates a notification"""

    help = "Send notification to a user or a group of users"

    def add_arguments(self, parser):
        """adds command line arguments"""
        parser.add_argument("--author", type=str, help="Author", required=True)
        parser.add_argument(
            "--subject", type=str, help="Email subject", default="Notification from Dalgo"
        )
        parser.add_argument("--message", type=str, help="Notification message on command-line")
        parser.add_argument(
            "--message-file", type=str, help="File containing the notification message"
        )
        parser.add_argument(
            "--audience",
            type=str,
            choices=[sent_to.value for sent_to in SentToEnum],
            help="Target audience (all_users | all_org_users | single_user)",
        )
        parser.add_argument(
            "--email",
            type=str,
            help="Email address if audience = single_user",
        )
        parser.add_argument("--org", type=str, help="Org slug if audience = all_org_users")
        parser.add_argument(
            "--manager_or_above",
            action="store_true",
            help="Only applicable if audience = all_org_users; flag to include managers or above",
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
        parser.add_argument("--dry-run", action="store_true", help="Dry run mode")

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

        if not Org.objects.filter(slug=options["org"]).exists():
            print(f"Organization with slug {options['org']} does not exist.")
            sys.exit(1)

        message = None
        if options["message"]:
            message = options["message"]
        elif options["message_file"]:
            try:
                with open(options["message_file"], "r", encoding="utf-8") as f:
                    message = f.read()
            except FileNotFoundError as e:
                print(f"Error reading message file: {e}")
                sys.exit(1)

        if message is None:
            print("Please provide a message or a message file.")
            sys.exit(1)

        # Get recipients based on audience field
        error, recipients = notifications_service.get_recipients(
            SentToEnum(options["audience"]),
            options.get("org"),
            options.get("email"),
            options.get("manager_or_above", False),
        )

        if error:
            print(f"Error in getting recipients: {error}")
            sys.exit(1)

        # Create notification data
        notification_data = NotificationDataSchema(
            author=options["author"],
            email_subject=options["subject"],
            recipients=recipients,
            message=message,
            urgent=options.get("urgent", False),
            scheduled_time=scheduled_time,
        )

        if options["dry_run"]:
            print(notification_data)
        else:
            # Call the create notification service
            error, result = notifications_service.create_notification(notification_data)

            if error:
                print(f"Error in creating notification: {error}")
                sys.exit(1)

            print(f"Notification created successfully: {result}")
