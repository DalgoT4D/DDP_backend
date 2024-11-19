from datetime import datetime
from django.core.management.base import BaseCommand

from ddpui.models.org import Org
from ddpui.models.org_plans import OrgPlans
from ddpui.utils.constants import DALGO_WITH_SUPERSET, DALGO, FREE_TRIAL


class Command(BaseCommand):
    """
    This script creates OrgPlans for Orgs
    """

    help = "Create an OrgPlan for an Org"

    def add_arguments(self, parser):
        parser.add_argument("--org", type=str, help="Org slug", required=True)
        parser.add_argument("--with-superset", action="store_true", help="Include superset")
        parser.add_argument(
            "--is-free-trial",
            action="store_true",
            help="Set the plan as Free Trial",
        )
        parser.add_argument(
            "--duration",
            choices=["Monthly", "Annual"],
            help="Subscription duration",
            required=True,
        )
        parser.add_argument("--start-date", type=str, help="Start date", required=False)
        parser.add_argument("--end-date", type=str, help="End date", required=False)
        parser.add_argument("--overwrite", action="store_true", help="Overwrite existing plan")

    def handle(self, *args, **options):
        """Create the OrgPlan for the Org"""
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

        org_plan.subscription_duration = options["duration"]

        org_plan.start_date = (
            datetime.strptime(options["start_date"], "%Y-%m-%d") if options["start_date"] else None
        )
        org_plan.end_date = (
            datetime.strptime(options["end_date"], "%Y-%m-%d") if options["end_date"] else None
        )

        if options["is_free_trial"]:
            org_plan.base_plan = "Free trial"
            org_plan.superset_included = True
            org_plan.can_upgrade_plan = True
            org_plan.features = FREE_TRIAL
        else:
            org_plan.base_plan = "DALGO"
            if options["with_superset"]:
                org_plan.superset_included = True
                org_plan.can_upgrade_plan = False
                org_plan.features = DALGO
            else:
                org_plan.superset_included = False
                org_plan.can_upgrade_plan = True
                org_plan.features = DALGO_WITH_SUPERSET

        org_plan.save()

        print(org_plan.to_json())
