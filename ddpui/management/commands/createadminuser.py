from django.core.management.base import BaseCommand

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
        user = AdminUser.objects.filter(email=options["email"]).first()
        if user:
            print(f"user account exists having id {user.id}")
        else:
            user = AdminUser.objects.create(email=options["email"], active=True)
            print(
                f"created admin user with email {user.email}, password currently ignored"
            )
