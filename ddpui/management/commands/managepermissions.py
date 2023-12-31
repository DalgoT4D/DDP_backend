"""permissions management script"""

from django.core.management.base import BaseCommand, CommandParser

from ddpui.models.org_user import OrgUser
from ddpui.models.permissions import Permission, Verb, Target, UserPermissions


class Command(BaseCommand):
    """Docstring"""

    help = "Manages user permissions"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--email", required=True)
        parser.add_argument("--verb", required=True, choices=Verb.choices())
        parser.add_argument("--target", required=True, choices=Target.choices())
        parser.add_argument("--is-able", required=True, choices=[True, False])

    def handle(self, *args, **options):
        """Docstring"""
        # for manual airbyte syncs on ingest page
        email = options["email"]
        verb = Verb(options["verb"])
        target = Target(options["target"])
        orguser = OrgUser.objects.filter(user__email=email).first()
        if not orguser:
            print(f"no user with email {email}")
            return
        permission = Permission.objects.filter(verb=verb, target=target).first()
        if not permission:
            print(f"no permission with verb {verb} and target {target}")
            return
        userpermission = UserPermissions.objects.filter(
            orguser=orguser, permission=permission
        ).first()
        if not userpermission:
            userpermission = UserPermissions(orguser=orguser, permission=permission)
        userpermission.is_able = options["is_able"] == "True"
        userpermission.save()
        print(f"set {userpermission}")
