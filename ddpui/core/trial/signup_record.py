"""Read/write the standalone TrialSignup record — the only trace of a trial that survives day 14.

Four write points, one per lifecycle stage (see ddpui/models/trial_signup.py):

    POST /trial/signup              -> record_signup()
    POST /trial/activate            -> record_tnc_accepted()
    clone_template_org() succeeds   -> record_trial_start()
    cleanup_trial_clone deletion    -> record_deletion()

Every function here is best-effort from the caller's point of view: none of them is allowed to be
the reason a signup 500s or an hourly deletion sweep aborts, so callers wrap them accordingly.
Keeping that policy at the call sites (rather than swallowing exceptions in here) means a genuine
bug in this module still shows up in the logs with a traceback.

`email` is matched exactly, not case-insensitively — deliberately the same rule the rest of the
trial flow uses (`User.objects.filter(username=email)` in trial_api / cleanup_trial_clone), so a
record written at signup is found again by the deletion.
"""

from datetime import datetime

from django.utils import timezone

from ddpui.models.trial_signup import TrialSignup
from ddpui.schemas.trial_schema import ActivationTokenData, TrialSignupSchema
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.signup_record")


def open_record_for_email(email: str) -> TrialSignup | None:
    """The still-open record for this email (deleted_at IS NULL), or None.

    `.order_by("-id").first()` rather than `.get()`: two concurrent POST /trial/signup calls for
    the same email can both miss and both create (there is no unique constraint — a partial index
    on (email) WHERE deleted_at IS NULL would be the fix if that ever mattered). Picking the
    newest keeps every caller deterministic instead of raising MultipleObjectsReturned.
    """
    return TrialSignup.objects.filter(email=email, deleted_at__isnull=True).order_by("-id").first()


def record_signup(payload: TrialSignupSchema) -> TrialSignup:
    """Open (or refresh) the record for a signup-form submission.

    Called BEFORE email verification, so rows exist for people who never click the link and for
    clones that fail — that is the funnel data the old "delete everything" delete threw away.

    A repeat signup while a record is still open updates it in place instead of adding a row: the
    verification email can be requested more than once, and each attempt is not a separate trial.
    Once the deletion closes a record, the next signup opens a fresh one, so trial history is kept.
    """
    record = open_record_for_email(payload.email)
    now = timezone.now()
    if record is None:
        return TrialSignup.objects.create(
            email=payload.email,
            org_name=payload.org_name,
            role=payload.role,
            signed_up_at=now,
        )

    record.org_name = payload.org_name
    record.role = payload.role
    record.signed_up_at = now
    record.save(update_fields=["org_name", "role", "signed_up_at"])
    return record


def record_tnc_accepted(payload: ActivationTokenData) -> TrialSignup:
    """Mark this email as having accepted the terms, on "Accept and Continue".

    Called from POST /trial/activate as early as the email is known — BEFORE the password is
    validated, the user row is written or the clone is enqueued. Every one of those can fail, and
    an acceptance that already happened must survive the failure: the open row (a failed clone is
    torn down but never deleted, so `deleted_at` stays NULL) is what a later follow-up mail is
    sent against.

    Creates the record when there is no open one, rather than no-op'ing. `record_signup` is
    best-effort at its call site, so a blip there used to cascade: no row to mark here, none to
    stamp at clone-success, none to close at the day-14 delete — a trial that ran its full 14 days
    and left no trace, the exact thing this table exists to prevent. The activation token carries
    the same email/org_name/role the signup form submitted, so this write point can rebuild the
    row on its own. Only `signed_up_at` is approximated (activate time, minutes after the real
    submission) — the alternative is a NOT NULL violation, and an approximate date beats no row.

    One-way: never written back to False. A repeat POST /trial/signup while the record is open
    updates org_name/role/signed_up_at and deliberately leaves this field alone — asking for a
    second verification email is not withdrawing consent.
    """
    record = open_record_for_email(payload.email)
    if record is None:
        logger.warning(
            f"no open trial signup record for {payload.email} at tnc acceptance; creating one"
        )
        return TrialSignup.objects.create(
            email=payload.email,
            org_name=payload.org_name,
            role=payload.role,
            signed_up_at=timezone.now(),
            tnc_accepted=True,
        )

    record.tnc_accepted = True
    record.save(update_fields=["tnc_accepted"])
    return record


def record_trial_start(email: str, trial_start: datetime) -> None:
    """Stamp the trial's start date on the open record, once a clone has fully succeeded.

    Stamped at the END of the clone, not in step 1 where the org and its plan are created: a
    clone that fails is torn down and will never be deleted (there is no org left to expire), so a
    record stamped at org-creation time would read as a live trial forever.

    Idempotent for a retry that eventually succeeds: the second success simply overwrites with the
    second clone's start date, which is the window the trial actually runs on.
    """
    record = open_record_for_email(email)
    if record is None:
        # no record to stamp: the signup predates this table, or the clone was started by
        # `manage.py clone_template_org` rather than the public signup flow.
        logger.info(f"no open trial signup record for {email}; not stamping trial_start_date")
        return

    record.trial_start_date = trial_start
    record.save(update_fields=["trial_start_date"])


def record_deletion(email: str) -> int:
    """Close the open record(s) for this email at delete time. Returns how many were closed.

    `.update()` on the queryset rather than one row: if a signup race did leave two open records,
    the deletion happens once and both records must reflect that. Rows already carrying a
    deleted_at are untouched, so re-running the cleanup command for the same email keeps the
    original deletion timestamp.
    """
    return TrialSignup.objects.filter(email=email, deleted_at__isnull=True).update(
        deleted_at=timezone.now()
    )
