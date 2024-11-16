from datetime import datetime
from django.core.management.base import BaseCommand

from ddpui.models.org import Org
from ddpui.models.org_plans import OrgPlans


class Command(BaseCommand):
    """
    This script creates OrgPlans for Orgs
    """

    help = "Create an OrgPlan for an Org"

    def add_arguments(self, parser):
        parser.add_argument("--org", type=str, help="Org slug", required=True)
        parser.add_argument("--with-superset", action="store_true", help="Include superset")
        parser.add_argument(
            "--duration",
            choices=["Monthly", "Annual"],
            help="Subscription duration",
            required=True,
        )
        parser.add_argument("--start-date", type=str, help="Start date", required=False)
        parser.add_argument("--end-date", type=str, help="Start date", required=False)
        parser.add_argument("--overwrite", action="store_true", help="Overwrite existing plan")

    def handle(self, *args, **options):
        """create the OrgPlan for the Org"""
        org = Org.objects.filter(slug=options["org"]).first()
        if org is None:
            self.stdout.write(self.style.ERROR(f"Org {options['org']} not found"))
            return

        org_plan = OrgPlans.objects.filter(org=org).first()
        if org_plan and not options["overwrite"]:
            self.stdout.write(self.style.ERROR(f"Org {options['org']} already has a plan"))
            return

        if not org_plan:
            org_plan = OrgPlans(org=org)

        org_plan.superset_included = options["with_superset"]
        org_plan.subscription_duration = options["duration"]

        org_plan.start_date = (
            datetime.strptime(options["start_date"], "%Y-%m-%d") if options["start_date"] else None
        )
        org_plan.end_date = (
            datetime.strptime(options["end_date"], "%Y-%m-%d") if options["end_date"] else None
        )

        if options["with_superset"]:
            org_plan.base_plan = "DALGO + Superset"
            org_plan.can_upgrade_plan = False
            org_plan.features = {
                "pipeline": ["Ingest", "Transform", "Orchestrate"],
                "aiFeatures": ["AI data analysis"],
                "dataQuality": ["Data quality dashboards"],
            }
        else:
            org_plan.base_plan = "DALGO"
            org_plan.can_upgrade_plan = True
            org_plan.features = {
                "pipeline": ["Ingest", "Transform", "Orchestrate"],
                "aiFeatures": ["AI data analysis"],
                "dataQuality": ["Data quality dashboards"],
                "superset": ["Superset dashboards", "Superset Usage dashboards"],
            }

        org_plan.save()

        print(org_plan.to_json())
