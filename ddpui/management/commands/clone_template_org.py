from django.core.management.base import BaseCommand, CommandError

from ddpui.models.org import Org
from ddpui.core.trial.clone_service import clone_template_org
from ddpui.core.trial.exceptions import TrialAccountExistsError
from ddpui.schemas.trial_schema import TrialCloneRequest


class Command(BaseCommand):
    """Clone a template org into a fresh trial org and print per-step timings."""

    help = "Deep-clone a template org into a new trial org (Steps 1-3) and report timing"

    def add_arguments(self, parser):
        parser.add_argument("--template", required=True, help="template org slug")
        parser.add_argument("--email", required=True, help="trial user email")

    def handle(self, *args, **options):
        template = Org.objects.filter(slug=options["template"]).first()
        if template is None:
            raise CommandError(f"no org with slug {options['template']}")

        try:
            run = clone_template_org(
                TrialCloneRequest(template_org_id=template.id, trial_email=options["email"])
            )
        except TrialAccountExistsError:
            self.stdout.write(
                self.style.WARNING(
                    f"account already exists for {options['email']} — route to login"
                )
            )
            return

        self.stdout.write(self.style.SUCCESS("clone completed"))
        self.stdout.write(f"trial org: {run.trial_org and run.trial_org.slug}")
        total = round(sum(run.timings.values()), 3)
        for step, secs in run.timings.items():
            self.stdout.write(f"  {step}: {secs}s")
        self.stdout.write(self.style.SUCCESS(f"total: {total}s"))
