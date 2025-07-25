from datetime import datetime
import pytz
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
        parser.add_argument("--with-superset", choices=["yes", "no"], help="Include superset")
        parser.add_argument(
            "--plan",
            choices=["Free Trial", "DALGO", "Internal"],
        )
        parser.add_argument(
            "--duration",
            choices=["Monthly", "Annual"],
            help="Subscription duration",
        )
        parser.add_argument("--start-date", type=str, help="Start date", required=False)
        parser.add_argument("--end-date", type=str, help="End date", required=False)
        parser.add_argument("--overwrite", action="store_true", help="Overwrite existing plan")
        parser.add_argument("--show", action="store_true", help="Show existing plan")

    def handle(self, *args, **options):
        """Create the OrgPlan for the Org"""
        org = Org.objects.filter(slug=options["org"]).first()
        if org is None:
            self.stdout.write(self.style.ERROR(f"Org {options['org']} not found"))
            return

        org_plan = OrgPlans.objects.filter(org=org).first()
        if options["show"]:
            if org_plan:
                self.stdout.write(str(org_plan))
            else:
                self.stdout.write("no such org")
            return

        if not org_plan:
            if not options["duration"]:
                self.stdout.write(self.style.ERROR("--duration is required"))
                return
            if not options["plan"]:
                self.stdout.write(self.style.ERROR("--plan is required"))
                return
            org_plan = OrgPlans(org=org)

        elif not options["overwrite"]:
            self.stdout.write(self.style.ERROR(f"Org {options['org']} already has a plan"))
            self.stdout.write(str(org_plan))
            return

        if options["duration"]:
            org_plan.subscription_duration = options["duration"]

        if options["plan"]:
            org_plan.base_plan = options["plan"]

        if options["start_date"]:
            org_plan.start_date = datetime.strptime(options["start_date"], "%Y-%m-%d").astimezone(
                pytz.UTC
            )

        if options["end_date"]:
            org_plan.end_date = datetime.strptime(options["end_date"], "%Y-%m-%d").astimezone(
                pytz.UTC
            )

        if options["plan"]:
            org_plan.features = {
                "pipeline": ["Ingest", "Transform", "Orchestrate"],
                "aiFeatures": ["AI data analysis"],
                "dataQuality": ["Data quality dashboards"],
            }

            if options["with_superset"] == "yes":
                org_plan.features["superset"] = ["Superset dashboards", "Superset Usage dashboards"]

            elif options["with_superset"] == "no" and "superset" in org_plan.features:
                del org_plan.features["superset"]

            if options["plan"] == "Free Trial":
                org_plan.can_upgrade_plan = True

            elif options["plan"] == "Internal":
                org_plan.can_upgrade_plan = False

            elif options["with_superset"] == "no":
                org_plan.can_upgrade_plan = True

            elif options["with_superset"] == "yes":
                org_plan.can_upgrade_plan = False

        if options["with_superset"] == "yes":
            org_plan.superset_included = options["with_superset"]

        org_plan.save()

        self.stdout.write(str(org_plan))
