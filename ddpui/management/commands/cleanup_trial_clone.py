"""Fully delete trial accounts created by the Free Trial clone.

Deletes everything a trial creates:
- the trial Org and all its external resources (Airbyte workspace, managed dbt GitHub repo,
  Prefect deployments/blocks, warehouse secret, OrgUser) via OrgCleanupService
- the dedicated trials-RDS database + owner role (ft_<email>_db / ft_<email>_user)
- the leftover Django User (OrgCleanupService removes the OrgUser but not the User)

One thing is deliberately NOT deleted: the `TrialSignup` record (ddpui/models/trial_signup.py).
It has no FK to any of the above, so nothing cascades it away, and this command stamps its
`deleted_at` on the way past. After a deletion that record is the only remaining trace that the
trial ever existed — who signed up, for what org, in what role, and when it was torn down.

Two ways to pick what gets deleted, sharing one teardown path:

    python manage.py cleanup_trial_clone --email <trial-email>   # one account, by hand
    python manage.py cleanup_trial_clone --expired               # every trial past its end_date

`--expired` is what the hourly `delete_expired_trial_orgs` celery task runs. Keeping it in this
command rather than in a separate service means the scheduled delete and the manual cleanup can
never drift apart, and the scheduled job can be rehearsed by hand at any time.

Why hourly: `end_date` is a real timestamp (clone time + TRIAL_DURATION_DAYS), so a trial started
16:45 expires at 16:45 on day 14. Selection is `end_date <= timezone.now()` evaluated per run, so
an hourly sweep deletes it on the first hour boundary AT OR AFTER expiry — 17:00, never 16:00 and
never early. A daily midnight sweep instead gave it another seven hours of life.
"""

import re
import time

from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
from django.utils import timezone

from ddpui.models.org import Org
from ddpui.models.org_plans import OrgPlans, OrgPlanType
from ddpui.models.org_user import OrgUser
from ddpui.core.trial.clone_service import delete_trial_org
from ddpui.core.trial.activation import CLONE_LOCK_PREFIX
from ddpui.core.trial.constants import (
    TRIAL_ORG_SLUG_PREFIX,
    TRIAL_ORG_SLUG_REGEX,
    TRIAL_DELETE_LOCK_KEY,
    TRIAL_DELETE_LOCK_TTL_SECONDS,
    TRIAL_DELETE_STAGGER_SECONDS,
)
from ddpui.core.trial.signup_record import record_deletion
from ddpui.core.trial.warehouse_provision import drop_trial_database, email_hash8
from ddpui.utils.redis_client import RedisClient
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


def is_trial_slug(slug: str) -> bool:
    """True if this slug was minted by the trial clone (`trial-<8 hex>-…`)."""
    return bool(slug and re.match(TRIAL_ORG_SLUG_REGEX, slug))


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
            deleted = self.delete_expired_trials()
            return f"deleted {deleted} expired trial(s)"

        self.purge_email(options["email"])
        return None

    def expired_trial_plans(self, now):
        """Free-trial plans past their end_date, restricted to CLONE-CREATED orgs.

        The slug filter is the one standing between this command and a very bad night.
        `base_plan == FREE_TRIAL` on its own is NOT a trial-clone marker: `create_org_plan`
        (ddpui/core/orgfunctions.py) lets an admin put any org — including a real, paying customer
        — on the Free Trial plan, and once that plan lapsed a base_plan-only filter would delete
        their org and drop their warehouse. Only clone-created orgs get a `trial-<hash8>-` slug.

        Matching the 8 hex chars rather than the bare "trial-" prefix keeps out a real org that
        merely happens to be named "Trial Foundation".

        A user who upgrades drops out for free: the upgrade moves `base_plan` off FREE_TRIAL, so
        the row stops matching.
        """
        return (
            OrgPlans.objects.filter(
                base_plan=OrgPlanType.FREE_TRIAL.value,
                end_date__isnull=False,
                end_date__lte=now,
                org__slug__regex=TRIAL_ORG_SLUG_REGEX,
            )
            .select_related("org")
            .order_by("id")
        )

    def delete_expired_trials(self) -> int:
        """Delete every expired trial, under a global lock. Returns how many were deleted.

        The lock makes concurrent deletes impossible. It matters because the schedule is hourly
        while a run is NOT bounded by an hour — TRIAL_DELETE_STAGGER_SECONDS alone reaches
        the hour mark at ~120 orgs. Overlapping runs would both select the same rows (nothing is marked
        in-progress; a row only stops matching once its org row is gone) and tear the same org
        down twice.

        Returning 0 when the lock is held is correct, not a skipped delete: the run that owns the
        lock is working through the very same queryset, and anything it misses is picked up by
        the next tick an hour later.
        """
        redis = RedisClient.get_instance()
        if not redis.set(TRIAL_DELETE_LOCK_KEY, "1", nx=True, ex=TRIAL_DELETE_LOCK_TTL_SECONDS):
            logger.info("an expired-trial deletion is already running; skipping this one")
            self.stdout.write("an expired-trial deletion is already running; skipping")
            return 0

        try:
            return self.delete_expired_trials_unlocked()
        finally:
            # release on the way out (success OR crash) so the next hourly tick isn't blocked by
            # the TTL. `delete` is idempotent — safe if the TTL already fired.
            redis.delete(TRIAL_DELETE_LOCK_KEY)

    def delete_expired_trials_unlocked(self) -> int:
        """Delete every expired trial, one at a time with a gap. Returns how many were deleted."""
        org_plans = list(self.expired_trial_plans(timezone.now()))
        self.stdout.write(f"{len(org_plans)} expired trial(s) to delete")

        deleted = 0
        for index, org_plan in enumerate(org_plans):
            # sleep at the TOP of the iteration, not the bottom, so a failed org still gets its
            # gap before the next one starts — a failing org is frequently an overloaded or
            # unreachable one, which is exactly when firing the next teardown is worst.
            if index > 0:
                time.sleep(TRIAL_DELETE_STAGGER_SECONDS)

            org = org_plan.org
            try:
                # one bad org must not strand the rest. exc_info=True keeps the traceback, so a
                # logic bug here doesn't read the same as an Airbyte/RDS outage.
                self.purge_expired_org(org)
                deleted += 1
            except Exception as err:  # skipcq PYL-W0703
                logger.error("trial delete failed for org %s: %s", org.slug, err, exc_info=True)

        self.stdout.write(
            self.style.SUCCESS(f"deleted {deleted} of {len(org_plans)} expired trial(s)")
        )
        return deleted

    def purge_expired_org(self, org: Org) -> None:
        """Delete one expired trial org by handing its owner's email to the --email path.

        `.order_by("id")` pins this to the earliest-created OrgUser — the trial's original owner —
        the same rule `lifecycle_emails.process_trial` uses; `.first()` on an unordered queryset
        has no defined result in Postgres.

        `user.username` (not `user.email`) is the key: the clone creates the user with
        `username=trial_email`, and `email_hash8` / `drop_trial_database` derive the RDS db and
        role names from that same string.
        """
        orguser = OrgUser.objects.filter(org=org).select_related("user").order_by("id").first()
        if orguser is None:
            # No OrgUser means no email, and the RDS drop, the Django-user delete and the
            # TrialSignup stamp are all keyed by email — there is no way to compute
            # `ft_<hash>_db` for an org whose owner is already gone. Delete what we can reach and
            # say plainly what leaked. The TrialSignup record stays open (deleted_at NULL); it is
            # closable by hand with `--email` once the owner's address is known.
            logger.warning(
                "expired trial org %s has no orguser; deleting the org only — its trials-RDS "
                "database and role are left behind and its TrialSignup record stays open",
                org.slug,
            )
            self.stdout.write(f"deleting orphan org {org.slug} (no orguser; RDS db left behind)")
            delete_trial_org(org)
            return

        self.purge_email(orguser.user.username)

    def collect_trial_orgs(self, email: str, user) -> tuple:
        """Trial orgs to delete for this email, and the non-trial orgs deliberately left alone.

        Two sources: (1) the OrgUser link, and (2) orphaned orgs with 0 OrgUsers, matched by the
        deterministic email-derived slug `trial-<hash8>`. An orphan happens when a previous delete
        removed the OrgUser but the final org.delete() failed (e.g. the old PROTECT-FK Metric/KPI
        bug) — the OrgUser-only lookup would miss it, yet its name still blocks the next clone.

        The OrgUser branch is filtered to TRIAL SLUGS ONLY, and that filter is load-bearing.
        Without it this returns every org the user belongs to — so a trial email that was later
        invited into a real org would take that org's Airbyte workspace and warehouse down with
        the trial, and the hourly `--expired` sweep would do it unattended.
        `account_exists_for_email` stops a trial being created for an email that already has an
        OrgUser, but nothing stops the invitation going the other way afterwards. This command
        deletes trials; a non-trial org is never its business.
        """
        target_orgs = {}
        skipped = []
        if user is not None:
            for ou in OrgUser.objects.filter(user=user).select_related("org"):
                if ou.org is None:
                    continue
                if is_trial_slug(ou.org.slug):
                    target_orgs[ou.org.id] = ou.org
                else:
                    skipped.append(ou.org.slug)
        for org in Org.objects.filter(
            slug__startswith=f"{TRIAL_ORG_SLUG_PREFIX}{email_hash8(email)}"
        ):
            target_orgs[org.id] = org
        return target_orgs, skipped

    def purge_email(self, email: str) -> None:
        """Fully delete the trial account for one email: org(s), RDS db+role, user, clone lock."""
        user = User.objects.filter(username=email).first()
        target_orgs, skipped = self.collect_trial_orgs(email, user)

        if skipped:
            logger.warning(
                "%s also belongs to non-trial org(s) %s — leaving them alone", email, skipped
            )
            self.stdout.write(f"skipping non-trial org(s) for this email: {', '.join(skipped)}")

        if target_orgs:
            for org in target_orgs.values():
                self.stdout.write(f"deleting org {org.slug} and its external resources ...")
                delete_trial_org(org)
        else:
            self.stdout.write("no trial org found for this email (org may already be gone)")

        # always attempt the RDS drop — the db/role names are deterministic from the email, so
        # this cleans up even if the org row was already deleted but the db leaked.
        drop_trial_database(email)

        # Close the durable TrialSignup record — its deleted_at is the whole reason anything is
        # left to know this trial existed once the rows below are gone. Best-effort: the account
        # IS deleted by this point, and raising here would log the org as a failed delete (and, in
        # the --expired loop, keep retrying an org that no longer exists).
        try:
            record_deletion(email)
        except Exception as err:  # skipcq PYL-W0703
            logger.error("failed to stamp deleted_at for trial %s: %s", email, err, exc_info=True)

        # Deleting the User cascades its remaining OrgUser rows away, so it must only happen once
        # the user has no membership left. A trial email that was invited into a real org keeps
        # its login; only the trial membership above was removed.
        if user is not None:
            if skipped:
                logger.warning(
                    "keeping django user %s — still a member of non-trial org(s) %s",
                    email,
                    skipped,
                )
                self.stdout.write(f"keeping the django user ({len(skipped)} non-trial org(s))")
            else:
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
