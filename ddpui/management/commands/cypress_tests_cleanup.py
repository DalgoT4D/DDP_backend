from django.core.management.base import BaseCommand
from django.contrib.auth.models import User

from ddpui.models.org import Org


class Command(BaseCommand):
    """
    This script cleansup the test users and the test organizations created
    by cypress. The org slugs start with 'cypress_' while username/email starts
    with 'cypress_'.
    """

    help = "Deletes the user and org created by cypress while tessting"

    def handle(self, *args, **options):
        """Delete cypress user and org"""
        for org in Org.objects.filter(slug__startswith="cypress_").all():
            org.delete()

        for user in User.objects.filter(username__startswith="cypress_").all():
            user.delete()
