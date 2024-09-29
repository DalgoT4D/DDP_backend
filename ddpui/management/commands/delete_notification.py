import sys
from django.core.management.base import BaseCommand
from ddpui.core import notifications_service


class Command(BaseCommand):
    """Deletes a notification by its ID"""

    help = "Deletes a notification by its ID"

    def add_arguments(self, parser):
        """Adds command line arguments"""
        parser.add_argument("notification_id", type=int, help="ID of the notification to delete")

    def handle(self, *args, **options):
        notification_id = options["notification_id"]

        # Call the notification service to delete the notification
        error, result = notifications_service.delete_scheduled_notification(notification_id)

        if error is not None:
            self.stderr.write(f"Error: {error}")
            sys.exit(1)

        self.stdout.write(f"Notification with ID {notification_id} deleted successfully.")
