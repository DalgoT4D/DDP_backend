"""shows blocks and tasks for an org"""
from django.core.management.base import BaseCommand
from django.utils.text import slugify

from ddpui.models.orgjobs import DataflowBlock
from ddpui.models.org import Org, OrgPrefectBlock, OrgDataFlow
from ddpui.models.tasks import OrgTask, OrgDataFlowv1


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

    def show_manual_dataflows(self, org: Org):
        """shows all manual OrgDataFlows for an org"""
        print("Manual Dataflows for " + org.slug + ":")
        for dataflow in OrgDataFlow.objects.filter(org=org).filter(
            dataflow_type="manual"
        ):
            assert DataflowBlock.objects.filter(dataflow=dataflow).count() == 1
            dfb = DataflowBlock.objects.filter(dataflow=dataflow).first()
            print(
                f"  {dataflow.deployment_name:50} [{dfb.opb.block_type:20}] {dfb.opb.command}"
            )

    def show_orchestrated_dataflows(self, org: Org):
        """shows all orchestrated OrgDataFlows for an org"""
        print("Orchestrated Dataflows for " + org.slug + ":")
        for dataflow in OrgDataFlow.objects.filter(org=org).filter(
            dataflow_type="orchestrate"
        ):
            q_dfdfb = DataflowBlock.objects.filter(dataflow=dataflow)
            if (
                q_dfdfb.filter(opb__block_type="Airbyte Connection").count() == 1
                and q_dfdfb.filter(opb__block_type="Shell Operation").count() == 1
                and q_dfdfb.filter(opb__block_type="dbt Core Operation").count() == 5
            ):
                print(f"  {dataflow.deployment_name:50} [with all 7 blocks]")
            else:
                for dfb in q_dfdfb:
                    opb = dfb.opb
                    print(
                        f"  {dataflow.deployment_name:50} [{opb.block_type:20}] {opb.command}"
                    )
            print("")

    def show_org_tasks(self, org: Org):
        """shows all tasks for an org"""
        print("OrgTasks for " + org.slug + ":")
        for orgtask in OrgTask.objects.filter(org=org):
            print(
                f"  {orgtask.task.type} {orgtask.task.label} {orgtask.connection_id if orgtask.connection_id else ''}"
            )

    def show_v1_manual_dataflows(self, org: Org):
        """show the v1 dataflows"""
        print("v1 Manual Dataflows for " + org.slug + ":")
        for dataflow in OrgDataFlowv1.objects.filter(org=org).filter(
            dataflow_type="manual"
        ):
            print(f"  {dataflow.deployment_name:50} ")

    def show_v1_orchestrated_dataflows(self, org: Org):
        """show the v1 dataflows"""
        print("v1 orchestrated Dataflows for " + org.slug + ":")
        for dataflow in OrgDataFlowv1.objects.filter(org=org).filter(
            dataflow_type="orchestrate"
        ):
            print(f"  {dataflow.deployment_name:50} ")

    def show_org_entities(self, org: Org):
        """shows all entities for an org"""

        if org.slug is None:
            org.slug = slugify(org.name)[:20]
            org.save()
        print(f"{org.slug}")
        self.show_orgprefectblocks(org)
        print("")
        self.show_org_tasks(org)
        print("")
        self.show_manual_dataflows(org)
        print("")
        self.show_v1_manual_dataflows(org)
        print("")
        self.show_orchestrated_dataflows(org)
        print("")
        self.show_v1_orchestrated_dataflows(org)
        print("=" * 80)

    def handle(self, *args, **options):
        """filters on --org if provided"""
        if options["org"]:
            org = Org.objects.filter(slug=options["org"]).first()
            self.show_org_entities(org)
        else:
            for org in Org.objects.all():
                self.show_org_entities(org)
