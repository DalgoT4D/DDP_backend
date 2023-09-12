from dotenv import load_dotenv
from django.core.management.base import BaseCommand

from ddpui.models.org import OrgDataFlow
from ddpui.utils.deploymentblocks import write_dataflowblocks


load_dotenv()


class Command(BaseCommand):
    """
    This script populates the DataflowBlock table
    """

    help = "Writes the DataflowBlock table"

    def add_arguments(self, parser):  # skipcq: PYL-R0201
        parser.add_argument("--org-slug")

    def handle(self, *args, **options):
        """filters on --org-slug if provides"""
        q_orgdataflow = OrgDataFlow.objects.all()
        if options["org_slug"]:
            q_orgdataflow = q_orgdataflow.filter(org__slug=options["org_slug"])

        for odf in q_orgdataflow:
            write_dataflowblocks(odf)
