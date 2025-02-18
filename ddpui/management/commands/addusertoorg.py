from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role


class Command(BaseCommand):
    """Adds/ removes a user to/ from an org"""

    help = "Adds/ removes a user to/ from an org"

    def add_arguments(self, parser):
        """Adds command line arguments"""
        parser.add_argument("user", help="Email username i.e. without @projecttech4dev.org")
        parser.add_argument("org", help="Org name or slug")
        parser.add_argument("--delete", action="store_true")

    def handle(self, *args, **options):
        username = options["user"]
        user = User.objects.filter(email=username + "@projecttech4dev.org").first()
        if user is None:
            print("User not found")
            return

        org = Org.objects.filter(name=options["org"]).first()
        if org is None:
            org = Org.objects.filter(slug=options["org"]).first()

        if org is None:
            print("Org not found")
            return

        orguser = OrgUser.objects.filter(user=user, org=org).first()

        if options["delete"]:
            if orguser:
                orguser.delete()
                print("User removed from org")
            else:
                print("User not in org")
            return

        if orguser:
            print("User already in org")
            return

        role = Role.objects.filter(slug="account-manager").first()
        if role is None:
            print('Role "account-manager" not found')
            return

        OrgUser.objects.create(user=user, org=org, new_role=role)

        print("User added to org")
