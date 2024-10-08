import sys
from django.core.management.base import BaseCommand

from ddpui.models.org_user import OrgUser
from ddpui.models.userpreferences import UserPreferences


class Command(BaseCommand):
    """Patch style toggle of user preferences"""

    help = "Patch style toggle of user preferences"

    def add_arguments(self, parser):
        """Adds command line arguments"""
        parser.add_argument(
            "orguser_id",
            type=str,
            help="ID of the orguser who preferences you want to toggle; pass 'all' to toggle for all orgusers",
        )
        parser.add_argument(
            "--enable_discord_notifications",
            action="store_const",
            const=True,
            help="Pass to enable discord notification",
            default=None,
        )
        parser.add_argument(
            "--disable_discord_notifications",
            action="store_const",
            const=False,
            help="Pass to disable discord notification",
            dest="enable_discord_notifications",
            default=None,
        )
        parser.add_argument(
            "--enable_email_notifications",
            action="store_const",
            const=True,
            help="Pass to enable email notification",
            default=None,
        )
        parser.add_argument(
            "--disable_email_notifications",
            action="store_const",
            const=False,
            help="Pass to disable email notification",
            dest="enable_email_notifications",
            default=None,
        )
        parser.add_argument(
            "--discord_webhook",
            type=str,
            help="Pass the discord webhook url",
            default=None,
        )

    def handle(self, *args, **options):
        orguser_id = options.get("orguser_id")
        enable_discord_notifications = options.get("enable_discord_notifications")
        enable_email_notifications = options.get("enable_email_notifications")
        discord_webhook = options.get("discord_webhook")

        orgusers = (
            OrgUser.objects.all() if orguser_id == "all" else OrgUser.objects.filter(id=orguser_id)
        )

        for orguser in orgusers:
            user_pref, created = UserPreferences.objects.get_or_create(orguser=orguser)
            if enable_discord_notifications is not None:
                user_pref.enable_discord_notifications = enable_discord_notifications
            if enable_email_notifications is not None:
                user_pref.enable_email_notifications = enable_email_notifications
            if discord_webhook is not None:
                user_pref.discord_webhook = discord_webhook

            user_pref.save()
