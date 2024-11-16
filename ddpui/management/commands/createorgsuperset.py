from django.core.management.base import BaseCommand

from ddpui.models.org import Org
from ddpui.models.org_supersets import OrgSupersets


class Command(BaseCommand):
    """
    This script creates OrgSupersets for Orgs
    """

    help = "Create an OrgSuperset for an Org"

    def add_arguments(self, parser):
        parser.add_argument("--org", type=str, help="Org slug", required=True)
        parser.add_argument("--container-name", type=str, help="Container name", required=True)
        parser.add_argument("--superset-version", type=str, help="Superset version", required=True)
        parser.add_argument("--overwrite", action="store_true", help="Overwrite existing plan")

    def handle(self, *args, **options):
        org = Org.objects.get(slug=options["org"])

        org_superset = OrgSupersets.objects.filter(org=org).first()
        if org_superset and not options["overwrite"]:
            self.stdout.write(self.style.ERROR(f"Org {options['org']} already has a superset"))
            return

        if not org_superset:
            org_superset = OrgSupersets(org=org)

        org_superset.container_name = options["container_name"]
        org_superset.superset_version = options["superset_version"]

        org_superset.save()
        print("OrgSuperset created successfully for " + org.slug)
