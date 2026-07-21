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

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.core.trial.clone_service import delete_trial_org
from ddpui.core.trial.activation import CLONE_LOCK_PREFIX
from ddpui.core.trial.warehouse_provision import drop_trial_database, email_hash8
from ddpui.utils.redis_client import RedisClient
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

        # Collect every trial org for this email: (1) via the OrgUser link, AND (2) orphaned orgs
        # with 0 OrgUsers, matched by the deterministic email-derived slug prefix `trial-<hash8>`.
        # An orphan happens when a previous delete removed the OrgUser but the final org.delete()
        # failed (e.g. the old PROTECT-FK Metric/KPI bug) — the OrgUser-only lookup would miss it,
        # yet its name still blocks the next clone. delete_trial_org() reaps viz + external resources.
        target_orgs = {}
        if user is not None:
            for ou in OrgUser.objects.filter(user=user).select_related("org"):
                if ou.org is not None:
                    target_orgs[ou.org.id] = ou.org
        for org in Org.objects.filter(slug__startswith=f"trial-{email_hash8(email)}"):
            target_orgs[org.id] = org

        if target_orgs:
            for org in target_orgs.values():
                self.stdout.write(f"deleting org {org.slug} and its external resources ...")
                delete_trial_org(org)
        else:
            self.stdout.write("no trial org found for this email (org may already be gone)")

        # always attempt the RDS drop — the db/role names are deterministic from the email, so
        # this cleans up even if the org row was already deleted but the db leaked.
        drop_trial_database(email)

        if user is not None:
            user.delete()

        # clear the per-email running-clone lock (acquired at activate/retry, normally released by
        # the task in its finally). If cleanup runs while that lock is still live — e.g. a worker
        # died mid-clone before releasing it — a fresh signup→activate (or retry) for the same
        # email 409s ("a trial is already being set up") until the TTL expires. Deleting it here
        # keeps the email immediately reusable.
        redis = RedisClient.get_instance()
        lock_deleted = redis.delete(f"{CLONE_LOCK_PREFIX}{email}")
        if lock_deleted:
            self.stdout.write("cleared stale running-clone lock")

        self.stdout.write(self.style.SUCCESS(f"fully deleted trial for {email}"))
