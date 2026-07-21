"""Fully delete a trial account created by the Free Trial clone, keyed by email.

Reaps everything a trial creates:
- the trial Org and all its external resources (Airbyte workspace, managed dbt GitHub repo,
  Prefect deployments/blocks, warehouse secret, OrgUser) via OrgCleanupService
- the dedicated trials-RDS database + owner role (ft_<email>_db / ft_<email>_user)
- the leftover Django User (OrgCleanupService removes the OrgUser but not the User)

Usage: python manage.py cleanup_trial_clone --email <trial-email>
"""

from django.core.management.base import BaseCommand
from django.contrib.auth.models import User

from ddpui.models.org_user import OrgUser
from ddpui.services.org_cleanup_service import OrgCleanupService
from ddpui.core.trial.warehouse_provision import drop_trial_database
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


class Command(BaseCommand):
    """Fully delete a trial account (org + external resources + RDS db/role + Django user)."""

    help = "Fully delete a trial account by email (org, Airbyte/repo/Prefect, RDS db+role, user)"

    def add_arguments(self, parser):
        parser.add_argument("--email", required=True, help="the trial user's email")

    def handle(self, *args, **options):
        email = options["email"]
        user = User.objects.filter(username=email).first()
        orguser = OrgUser.objects.filter(user=user).first() if user else None

        if orguser is not None:
            self.stdout.write(f"deleting org {orguser.org.slug} and its external resources ...")
            OrgCleanupService(orguser.org, dry_run=False).delete_org()
        else:
            self.stdout.write("no trial org found for this email (org may already be gone)")

        # always attempt the RDS drop — the db/role names are deterministic from the email, so
        # this cleans up even if the org row was already deleted but the db leaked.
        drop_trial_database(email)

        if user is not None:
            user.delete()

        self.stdout.write(self.style.SUCCESS(f"fully deleted trial for {email}"))
