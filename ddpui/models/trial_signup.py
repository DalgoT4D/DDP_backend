"""Standalone record of every free-trial signup — the one row that outlives the trial.

The day-14 delete (`manage.py cleanup_trial_clone --expired`) deletes the trial Org, its OrgUser,
its OrgPlans row, its UserPreferences and the Django User, so after it runs there is no trace
left anywhere of who the trial belonged to. This table is that trace.

It deliberately has NO foreign keys. An FK to Org or OrgUser would either cascade the row away
with the very teardown it exists to survive, or PROTECT the teardown and break the hourly deletion sweep.
Everything here is a plain column, copied at write time.

Lifecycle of one row:

    POST /trial/signup       -> row created (email, org_name, role, signed_up_at)
    POST /trial/activate     -> tnc_accepted set True ("Accept and Continue" on the consent screen)
    clone completes          -> trial_start_date stamped
    day-14 delete              -> deleted_at stamped

At most one OPEN row (deleted_at IS NULL) exists per email; the deletion closes it, so a later
signup for the same email opens a fresh row and the earlier trial's record stays intact.
"""

from django.db import models


class TrialSignup(models.Model):
    """One free-trial signup, kept after the trial's org/user are deleted."""

    # username/login of the (eventual) Django user. Not unique: an email that trials, gets
    # deleted, and signs up again gets one row per trial. Indexed because both the signup write
    # and the deletion look rows up by email.
    email = models.CharField(max_length=254, db_index=True)
    # the org name typed on the public signup form — NOT Org.name (which the clone prefixes with
    # "Trial <hash8> " and truncates to 50 chars). This keeps what the user actually wrote.
    org_name = models.CharField(max_length=255, null=True, blank=True)
    # job title from the signup form. Metadata only — never an RBAC role_slug.
    role = models.CharField(max_length=255, null=True, blank=True)
    # when the signup form was submitted. Stamped before email verification, so rows exist for
    # people who never clicked the link or whose clone failed.
    signed_up_at = models.DateTimeField()
    # whether the user ticked "I've read and accept the Privacy Policy" and pressed "Accept and
    # Continue" on the consent screen. False on a fresh signup row and stamped True by
    # POST /trial/activate — that endpoint is only reachable from the consent screen, whose button
    # stays disabled until the box is ticked, so reaching it IS the acceptance. Stays False for
    # anyone who signed up, got the email, and never accepted; never reset back to False once True,
    # because a repeat signup (allowed while the record is open) does not un-accept an acceptance.
    # Stamped BEFORE the account is created, so a failed activation or a failed clone still leaves
    # an open row that records the acceptance — that row is the audience for follow-up mail.
    tnc_accepted = models.BooleanField(default=False)
    # start of the trial window (mirrors OrgPlans.start_date). NULL until a clone succeeds. No
    # end_date column: the window is a fixed TRIAL_DURATION_DAYS long, and `deleted_at` already
    # records when the trial actually ended.
    trial_start_date = models.DateTimeField(null=True, blank=True)
    # when the deletion deleted the org/user for this trial. NULL means the row is still open —
    # either a live trial, or a signup that never became one.
    deleted_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        """newest signup first — the order any human reading this table wants"""

        ordering = ["-signed_up_at"]

    def __str__(self) -> str:
        return f"TrialSignup({self.email}, signed_up_at={self.signed_up_at})"
