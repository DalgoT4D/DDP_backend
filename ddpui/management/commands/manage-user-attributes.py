from dotenv import load_dotenv
from django.core.management.base import BaseCommand

from django.contrib.auth.models import User
from ddpui.utils.custom_logger import CustomLogger
from ddpui.models.org_user import UserAttributes

logger = CustomLogger("ddpui")

load_dotenv()


class Command(BaseCommand):
    """
    This script manages UserAttributes for a user
    """

    help = "Writes the UserAttributes table"
    attributes = [
        "email_verified",
        "can_create_orgs",
        "is_consultant",
        "is_platform_admin",
    ]

    def add_arguments(self, parser):  # skipcq: PYL-R0201
        parser.add_argument("--email", required=True, help="email of the user")
        parser.add_argument(
            "--enable", nargs="+", help="attributes to set", choices=self.attributes
        )
        parser.add_argument(
            "--disable", nargs="+", help="attributes to unset", choices=self.attributes
        )

    def handle(self, *args, **options):
        """for the given user, set/unset specified attributes"""
        user = User.objects.filter(email=options["email"]).first()
        userattributes = UserAttributes.objects.filter(user=user).first()
        if userattributes is None:
            userattributes = UserAttributes(user=user)
        for attribute in options["enable"] or []:
            setattr(userattributes, attribute, True)
        for attribute in options["disable"] or []:
            setattr(userattributes, attribute, False)
        userattributes.save()
        print(userattributes)
