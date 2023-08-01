from dotenv import load_dotenv
from django.core.management.base import BaseCommand

from ddpui.models.org_user import Org
from ddpui.utils.deleteorg import delete_one_org


load_dotenv()


class Command(BaseCommand):
    """
    This script deletes an org and all associated entities
    Not only in the Django database, but also in Airbyte and in Prefect
    """

    help = "Deletes an organization"

    def add_arguments(self, parser):  # skipcq: PYL-R0201
        """The main parameter is the org name"""
        parser.add_argument("--org-name", required=True)
        parser.add_argument("--yes-really", action="store_true")

    def handle(self, *args, **options):
        """Docstring"""
        if options["org_name"] == "ALL":
            for org in Org.objects.all():
                self.delete_one_org(org, options["yes_really"])
        else:
            org = Org.objects.filter(name=options["org_name"]).first()
            if org is None:
                org = Org.objects.filter(slug=options["org_name"]).first()
            if org is None:
                print("no such org")
                return

            delete_one_org(org, options["yes_really"])
