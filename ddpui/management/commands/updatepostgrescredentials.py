from django.core.management.base import BaseCommand
from django.contrib.auth.models import User

from ddpui.models.org import Org


class Command(BaseCommand):
    """
    Iterates through postgres warehouses,
    Retrieves credentials from the secrets manager,
    If there is no "tunnel_method" key,
    Add
      tunnel_method: { "tunnel_method": "NO_TUNNEL" }
    Save the credentials back to the secrets manager
    """

    help = "Adds tunnel_method: NO_TUNNEL to the postgres credentials in the secrets manager."

    def handle(self, *args, **options):
        """For every org"""
        pass
