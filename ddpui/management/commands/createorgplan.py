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

        if OrgPlans.objects.filter(org=org).exists() and not options["overwrite"]:
            self.stdout.write(self.style.ERROR(f"Org {options['org']} already has a plan"))
            return

        start_date = (
            datetime.strptime(options["start_date"], "%Y-%m-%d") if options["start_date"] else None
        )
        end_date = (
            datetime.strptime(options["end_date"], "%Y-%m-%d") if options["end_date"] else None
        )

        if options["with_superset"]:
            base_plan = "DALGO + Superset"
            can_upgrade_plan = False
            features = {
                "pipeline": ["Ingest", "Transform", "Orchestrate"],
                "aiFeatures": ["AI data analysis"],
                "dataQuality": ["Data quality dashboards"],
            }
        else:
            base_plan = "DALGO"
            can_upgrade_plan = True
            features = {
                "pipeline": ["Ingest", "Transform", "Orchestrate"],
                "aiFeatures": ["AI data analysis"],
                "dataQuality": ["Data quality dashboards"],
                "superset": ["Superset dashboards", "Superset Usage dashboards"],
            }

        org_plan = OrgPlans.objects.update_or_create(
            org=org,
            base_plan=base_plan,
            superset_included=options["with_superset"],
            subscription_duration=options["duration"],
            start_date=start_date,
            end_date=end_date,
            can_upgrade_plan=can_upgrade_plan,
            features=features,
        )
        print(org_plan.to_json())
