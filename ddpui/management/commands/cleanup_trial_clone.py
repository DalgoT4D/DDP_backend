"""Fully delete trial accounts created by the Free Trial clone.

Reaps everything a trial creates:
- the trial Org and all its external resources (Airbyte workspace, managed dbt GitHub repo,
  Prefect deployments/blocks, warehouse secret, OrgUser) via OrgCleanupService
- the dedicated trials-RDS database + owner role (ft_<email>_db / ft_<email>_user)
- the leftover Django User (OrgCleanupService removes the OrgUser but not the User)

Two ways to pick what gets deleted, sharing one teardown path:

    python manage.py cleanup_trial_clone --email <trial-email>   # one account, by hand
    python manage.py cleanup_trial_clone --expired               # every trial past its end_date

`--expired` is what the nightly `reap_expired_trial_orgs` celery task runs. Keeping it in this
command rather than in a separate service means the scheduled reap and the manual cleanup can
never drift apart, and the nightly job can be rehearsed by hand at any time.
"""

import time

from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
from django.utils import timezone

from ddpui.models.org import Org
from ddpui.models.org_plans import OrgPlans, OrgPlanType
from ddpui.models.org_user import OrgUser
from ddpui.core.trial.clone_service import delete_trial_org
from ddpui.core.trial.activation import CLONE_LOCK_PREFIX
from ddpui.core.trial.constants import TRIAL_ORG_SLUG_PREFIX, TRIAL_REAP_STAGGER_SECONDS
from ddpui.core.trial.warehouse_provision import drop_trial_database, email_hash8
from ddpui.utils.redis_client import RedisClient
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


class Command(BaseCommand):
    """Fully delete trial accounts (org + external resources + RDS db/role + Django user)."""

    help = "Delete trial accounts by email (--email) or every expired trial (--expired)"

    def add_arguments(self, parser):
        selector = parser.add_mutually_exclusive_group(required=True)
        selector.add_argument("--email", help="the trial user's email")
        selector.add_argument(
            "--expired",
            action="store_true",
            help="delete every free-trial org whose OrgPlans.end_date has passed",
        )

    def handle(self, *args, **options):
        if options["expired"]:
            reaped = self.reap_expired()
            return f"reaped {reaped} expired trial(s)"

        self.purge_email(options["email"])
        return None

    def expired_trial_plans(self, now):
        """Free-trial plans past their end_date, restricted to CLONE-CREATED orgs.

        The `slug__startswith` filter is the one standing between this command and a very bad
        night. `base_plan == FREE_TRIAL` on its own is NOT a trial-clone marker: `create_org_plan`
        (ddpui/core/orgfunctions.py) lets an admin put any org — including a real, paying customer
        — on the Free Trial plan, and once that plan lapsed a base_plan-only filter would delete
        their org and drop their warehouse. Only clone-created orgs get the `trial-<hash8>-` slug.

        A user who upgrades drops out for free: the upgrade moves `base_plan` off FREE_TRIAL, so
        the row stops matching.
        """
        return (
            OrgPlans.objects.filter(
                base_plan=OrgPlanType.FREE_TRIAL.value,
                end_date__isnull=False,
                end_date__lte=now,
                org__slug__startswith=TRIAL_ORG_SLUG_PREFIX,
            )
            .select_related("org")
            .order_by("id")
        )

    def reap_expired(self) -> int:
        """Delete every expired trial, one at a time with a gap. Returns how many were reaped."""
        org_plans = list(self.expired_trial_plans(timezone.now()))
        self.stdout.write(f"{len(org_plans)} expired trial(s) to reap")

        reaped = 0
        for index, org_plan in enumerate(org_plans):
            # sleep at the TOP of the iteration, not the bottom, so a failed org still gets its
            # gap before the next one starts — a failing org is frequently an overloaded or
            # unreachable one, which is exactly when firing the next teardown is worst.
            if index > 0:
                time.sleep(TRIAL_REAP_STAGGER_SECONDS)

            org = org_plan.org
            try:
                # one bad org must not strand the rest. exc_info=True keeps the traceback, so a
                # logic bug here doesn't read the same as an Airbyte/RDS outage.
                self.purge_expired_org(org)
                reaped += 1
            except Exception as err:  # skipcq PYL-W0703
                logger.error("trial reap failed for org %s: %s", org.slug, err, exc_info=True)

        self.stdout.write(
            self.style.SUCCESS(f"reaped {reaped} of {len(org_plans)} expired trial(s)")
        )
        return reaped

    def purge_expired_org(self, org: Org) -> None:
        """Reap one expired trial org by handing its owner's email to the --email path.

        `.order_by("id")` pins this to the earliest-created OrgUser — the trial's original owner —
        the same rule `lifecycle_emails.process_trial` uses; `.first()` on an unordered queryset
        has no defined result in Postgres.

        `user.username` (not `user.email`) is the key: the clone creates the user with
        `username=trial_email`, and `email_hash8` / `drop_trial_database` derive the RDS db and
        role names from that same string.
        """
        orguser = OrgUser.objects.filter(org=org).select_related("user").order_by("id").first()
        if orguser is None:
            # No OrgUser means no email, and both the RDS drop and the Django-user delete are
            # keyed by email — there is no way to compute `ft_<hash>_db` for an org whose owner
            # is already gone. Delete what we can reach and say plainly what leaked.
            logger.warning(
                "expired trial org %s has no orguser; deleting the org only — "
                "its trials-RDS database and role are left behind",
                org.slug,
            )
            self.stdout.write(f"deleting orphan org {org.slug} (no orguser; RDS db left behind)")
            delete_trial_org(org)
            return

        self.purge_email(orguser.user.username)

    def purge_email(self, email: str) -> None:
        """Fully delete the trial account for one email: org(s), RDS db+role, user, clone lock."""
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
        for org in Org.objects.filter(
            slug__startswith=f"{TRIAL_ORG_SLUG_PREFIX}{email_hash8(email)}"
        ):
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
