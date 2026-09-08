"""create the dataflow for edr send report"""

from django.core.management.base import BaseCommand

from ddpui.models.org import Org, OrgDataFlowv1
from ddpui.models.tasks import OrgTask, DataflowOrgTask
from ddpui.core.orgtaskfunctions import get_edr_send_report_task


class Command(BaseCommand):
    """Create the dataflow for edr send report"""

    help = "Create the dataflow for edr send report"

    def add_arguments(self, parser):
        parser.add_argument("org", type=str, help='Org slug, provide "all" to fix links')
        parser.add_argument("--cron", type=str, default="0 0 * * *")
        parser.add_argument(
            "--fix-links",
            action="store_true",
            help="Create missing links between OrgTask and OrgDataFlowv1",
        )
        parser.add_argument(
            "--refresh-all",
            action="store_true",
            help="Update deployment params for all orgs that already have an EDR dataflow (does not create new ones)",
        )

    def handle(self, *args, **options):
        from ddpui.ddpdbt.elementary_service import ensure_edr_sendreport_dataflow
        from ddpui.utils.constants import TASK_GENERATE_EDR

        if options["refresh_all"]:
            # Only update orgs that already have an EDR dataflow — do not create new ones
            edr_orgtask_ids = OrgTask.objects.filter(task__slug=TASK_GENERATE_EDR).values_list(
                "id", flat=True
            )
            existing_dfots = DataflowOrgTask.objects.filter(
                orgtask_id__in=edr_orgtask_ids
            ).select_related("orgtask__org", "dataflow")

            for dfot in existing_dfots:
                org = dfot.orgtask.org
                if not org.dbt:
                    print(f"{org.slug}: no dbt config, skipping")
                    continue
                ensure_edr_sendreport_dataflow(org, dfot.dataflow.cron or "0 0 * * *")
                print(f"{org.slug}: updated")
            return

        if options["fix_links"]:
            for org in Org.objects.exclude(dbt__isnull=True):
                org_task = get_edr_send_report_task(org)
                if org_task is None:
                    print(f"no edr OrgTask found for {org.slug}, skipping")
                    continue

                if DataflowOrgTask.objects.filter(orgtask=org_task).exists():
                    print(f"DataflowOrgTask already exists for {org.slug}, skipping")
                    continue

                deployment_name_prefix = f"pipeline-{org_task.org.slug}-{org_task.task.slug}-"
                dataflow = OrgDataFlowv1.objects.filter(
                    org=org,
                    deployment_name__startswith=deployment_name_prefix,
                ).first()
                if dataflow is None:
                    print(f"no OrgDataFlowv1 found for {org.slug}, skipping")
                    continue

                DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=org_task)
                print(f"created DataflowOrgTask for {org.slug}")

            return

        org = Org.objects.filter(slug=options["org"]).first()
        if org is None:
            print(f"Org with slug {options['org']} does not exist")
            return

        if not org.dbt:
            print(f"OrgDbt for {org.slug} not found")
            return

        dataflow = ensure_edr_sendreport_dataflow(org, options["cron"])
