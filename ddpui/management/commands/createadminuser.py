from django.core.management.base import BaseCommand

from django.contrib.auth.models import User
from ddpui.models.admin_user import AdminUser


class Command(BaseCommand):
    """Docstring"""

    help = "Creates a platform administrator"

    def add_arguments(self, parser):
        """Docstring"""
        parser.add_argument("--email", required=True)
        parser.add_argument("--password", required=True)

    def handle(self, *args, **options):
        """Docstring"""
        adminuser = AdminUser.objects.filter(user__username=options["email"]).first()
        if adminuser:
            print(f"user account exists having userid {adminuser.user.id}")
        else:
            user = User.objects.create_user(
                username=options["email"],
                email=options["email"],
                password=options["password"],
            )
            adminuser = AdminUser.objects.create(user=user)
            print(f"created admin user with email {adminuser.user.email}")
