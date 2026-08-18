"""Move existing rows onto the current work-function list (`trial_schema.WorkDomain`).

    program_manager -> program_implementation
    data_tech       -> data_technology
    consultant      -> external_consultant
    none            -> NULL
    field_worker    -> NULL

`monitoring_evaluation` / `leadership` kept their slugs. Unrecognised values are reported, not
guessed at. Both tables holding the value are covered: `OrgUser.work_domain` and
`TrialSignup.role` — both nullable columns, so the two NULL rows above are legal. Idempotent.

    python manage.py migrate_work_domains --dry-run   # counts only, no writes
    python manage.py migrate_work_domains             # apply
"""

from typing import get_args

from django.core.management.base import BaseCommand
from django.db import transaction

from ddpui.models.org_user import OrgUser
from ddpui.models.trial_signup import TrialSignup
from ddpui.schemas.trial_schema import WorkDomain

# retired slug -> current slug, for options that survived under a new name
RENAMES = {
    "program_manager": "program_implementation",
    "data_tech": "data_technology",
    "consultant": "external_consultant",
}

# retired with no successor -> NULL (the columns are nullable, so nothing is invented)
DROPPED = ("none", "field_worker")

# (model, field) pairs holding a work-function slug
TARGETS = ((OrgUser, "work_domain"), (TrialSignup, "role"))


class Command(BaseCommand):
    """rewrite stored work_domain / role slugs onto the current option list"""

    help = "Migrate existing work-function values to the current option list"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="report what would change without writing",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        if dry_run:
            self.stdout.write("DRY RUN — no writes")

        for model, field in TARGETS:
            self._migrate(model, field, dry_run)

    def _migrate(self, model, field, dry_run: bool):
        """apply the renames + retirements for one (model, field) pair"""
        label = f"{model.__name__}.{field}"
        self.stdout.write(f"\n{label}")

        with transaction.atomic():
            for old_slug, new_slug in RENAMES.items():
                rows = model.objects.filter(**{field: old_slug})
                count = rows.count()
                if count == 0:
                    continue
                self.stdout.write(f"  {old_slug} -> {new_slug}: {count}")
                if not dry_run:
                    rows.update(**{field: new_slug})

            for retired_slug in DROPPED:
                rows = model.objects.filter(**{field: retired_slug})
                count = rows.count()
                if count == 0:
                    continue
                self.stdout.write(f"  {retired_slug} -> NULL: {count}")
                if not dry_run:
                    rows.update(**{field: None})

            if dry_run:
                # nothing was written, but the reads above ran inside the transaction; roll it
                # back so a --dry-run never leaves a connection holding one open
                transaction.set_rollback(True)

        self._report_unknown(model, field)

    def _report_unknown(self, model, field):
        """list any remaining values that are neither current nor handled above"""
        known = set(get_args(WorkDomain)) | set(RENAMES) | set(DROPPED)
        unknown = (
            model.objects.exclude(**{f"{field}__in": known})
            .exclude(**{f"{field}__isnull": True})
            .exclude(**{field: ""})
            .values_list(field, flat=True)
            .distinct()
        )
        for value in unknown:
            count = model.objects.filter(**{field: value}).count()
            self.stdout.write(self.style.WARNING(f"  unrecognised '{value}': {count} (left as-is)"))
