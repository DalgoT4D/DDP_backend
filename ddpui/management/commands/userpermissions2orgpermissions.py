from datetime import datetime
from django.core.management.base import BaseCommand

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.userpreferences import UserPreferences


class Command(BaseCommand):
    """
    This script creates OrgPermissions from UserPermissions
    """

    help = "Create OrgPermissions from UserPermissions"

    def add_arguments(self, parser):
        parser.add_argument("--org", type=str, help="Org slug, use 'all' to update all orgs")

    def handle(self, *args, **options):
        q = Org.objects
        if options["org"] != "all":
            q = q.filter(slug=options["org"])
        if q.count() == 0:
            print("No orgs found")
            return
        for org in q.all():
            print("Processing org " + org.slug)

            orgpreferences = OrgPreferences.objects.filter(org=org).first()
            if orgpreferences is None:
                print("creating org preferences for " + org.slug)
                orgpreferences = OrgPreferences.objects.create(org=org)

            for orguser in OrgUser.objects.filter(org=org):
                userpreferences = UserPreferences.objects.filter(orguser=orguser).first()

                if userpreferences is not None:
                    print("Found user preferences for " + orguser.user.email)

                    if orgpreferences.llm_optin is False and userpreferences.llm_optin is True:
                        print("Approving LLM opt-in by " + orguser.user.email)
                        orgpreferences.llm_optin = True
                        orgpreferences.llm_optin_approved_by = orguser
                        orgpreferences.llm_optin_date = datetime.now()

                    if (
                        orgpreferences.enable_discord_notifications is False
                        and userpreferences.enable_discord_notifications is True
                        and userpreferences.discord_webhook is not None
                    ):
                        # use the discord webbhook from the first user we find
                        print(
                            "Discord notifications enabled by "
                            + orguser.user.email
                            + ", settings webook to "
                            + userpreferences.discord_webhook
                        )
                        orgpreferences.enable_discord_notifications = True
                        orgpreferences.discord_webhook = userpreferences.discord_webhook

                    orgpreferences.save()
