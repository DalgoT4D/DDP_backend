"""Delete EDR dataflows (and their OrgTasks) for one or more orgs."""

from dotenv import load_dotenv
from django.core.management.base import BaseCommand

from ddpui.models.org_user import Org
from ddpui.services.org_cleanup_service import OrgCleanupService


load_dotenv()


class Command(BaseCommand):
    help = "Delete EDR dataflows and OrgTasks for the given org(s)"

    def add_arguments(self, parser):
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "--orgs",
            nargs="+",
            metavar="SLUG",
            help="One or more org slugs",
        )
        group.add_argument(
            "--all",
            action="store_true",
            dest="all_orgs",
            help="Run against every org that has an EDR OrgTask",
        )
        parser.add_argument(
            "--yes-really",
            action="store_true",
            help="Actually delete. Without this flag the command is a dry-run.",
        )

    def handle(self, *args, **options):
        from ddpui.models.tasks import OrgTask
        from ddpui.utils.constants import TASK_GENERATE_EDR

        dry_run = not options["yes_really"]
        if dry_run:
            self.stdout.write("DRY RUN — pass --yes-really to actually delete\n")

        if options["all_orgs"]:
            org_ids = (
                OrgTask.objects.filter(task__slug=TASK_GENERATE_EDR)
                .values_list("org_id", flat=True)
                .distinct()
            )
            orgs = list(Org.objects.filter(id__in=org_ids))
        else:
            orgs = []
            for slug in options["orgs"]:
                org = Org.objects.filter(slug=slug).first()
                if org is None:
                    self.stderr.write(f"org not found: {slug}\n")
                else:
                    orgs.append(org)

        if not orgs:
            self.stdout.write("no orgs to process\n")
            return

        for org in orgs:
            self.stdout.write(f"[{org.slug}] processing...\n")
            OrgCleanupService(org, dry_run=dry_run).delete_elementary_setup()
            self.stdout.write(f"[{org.slug}] done\n")
