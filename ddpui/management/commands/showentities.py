"""shows blocks and tasks for an org"""
from django.core.management.base import BaseCommand
from django.utils.text import slugify

from ddpui.models.orgjobs import DataflowBlock
from ddpui.models.org import Org, OrgPrefectBlock, OrgDataFlow


class Command(BaseCommand):
    """
    This script displays dataflows and prefect blocks in the django db
    """

    help = "Displays dataflows and prefect blocks in the django db"

    def add_arguments(self, parser):  # skipcq: PYL-R0201
        parser.add_argument("--org")

    def show_orgprefectblocks(self, org: Org):
        """shows all OrgPrefectBlocks for an org"""
        print("Blocks for " + org.slug + ":")
        for opb in OrgPrefectBlock.objects.filter(org=org).order_by("block_type"):
            if opb.block_type == "Airbyte Server":
                print(f"  {opb.block_type}")
            elif opb.block_type == "Airbyte Connection":
                print(f"  {opb.block_name}")
            elif opb.block_type.find("Shell") == 0:
                print(f"  {opb.command}")
            else:
                print(f"  dbt {opb.command}")

    def show_orgdataflows(self, org: Org):
        """shows all OrgDataFlows for an org"""
        print("Manual Dataflows for " + org.slug + ":")
        for dataflow in OrgDataFlow.objects.filter(org=org).filter(
            dataflow_type="manual"
        ):
            for dfb in DataflowBlock.objects.filter(dataflow=dataflow):
                opb = dfb.opb
                print(
                    f"  {dataflow.deployment_name:50} [{opb.block_type:20}] {opb.command}"
                )

        print("Orchestrated Dataflows for " + org.slug + ":")
        for dataflow in OrgDataFlow.objects.filter(org=org).filter(
            dataflow_type="orchestrate"
        ):
            for dfb in DataflowBlock.objects.filter(dataflow=dataflow):
                opb = dfb.opb
                print(
                    f"  {dataflow.deployment_name:50} [{opb.block_type:20}] {opb.command}"
                )
            print("")

    def show_org_entities(self, org: Org):
        """shows all entities for an org"""

        if org.slug is None:
            org.slug = slugify(org.name)[:20]
            org.save()
        print(f"{org.slug}")
        self.show_orgprefectblocks(org)
        print("")
        self.show_orgdataflows(org)
        print("=" * 80)

    def handle(self, *args, **options):
        """filters on --org if provided"""
        if options["org"]:
            org = Org.objects.filter(slug=options["org"]).first()
            self.show_org_entities(org)
        else:
            for org in Org.objects.all():
                self.show_org_entities(org)
