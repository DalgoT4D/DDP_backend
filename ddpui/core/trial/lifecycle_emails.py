"""Automated free-trial lifecycle emails — decision ladder and hourly sweep.

Five emails are driven from here: three progress-based (day-3 not-started, day-3 in-progress,
completion) and two date-based (midpoint, pre-end). An hourly Celery task calls
`run_trial_lifecycle_sweep`, which sends at most ONE email per trial per run and records what
was sent in `UserPreferences.trial_emails_sent` so nothing goes out twice.

Design: docs/superpowers/specs/2026-08-09-trial-lifecycle-emails-design.md
"""

from datetime import timedelta

from ddpui.core.trial.clone_service import TRIAL_DURATION_DAYS
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.lifecycle_emails")

# The walkthrough flows these emails track. `product_tour` is deliberately excluded — product
# decided it neither counts toward completion nor appears as a checklist row.
TRACKED_FLOWS = ("insights", "automate_pipeline")

# Keys written into UserPreferences.trial_emails_sent. The day-3 not-started and in-progress
# emails share ONE key because only one of them can ever fire for a given user.
EMAIL_DAY3 = "day3"
EMAIL_COMPLETION = "completion"
EMAIL_MIDPOINT = "midpoint"
EMAIL_PRE_END = "pre_end"

# Days elapsed since OrgPlans.start_date before each rule becomes eligible.
DAY3_THRESHOLD_DAYS = 3
MIDPOINT_THRESHOLD_DAYS = 7
# The pre-end warning goes out this many days before OrgPlans.end_date.
PRE_END_DAYS_BEFORE = 2


def completed_flows(trial_walkthrough: dict) -> list:
    """Tracked walkthrough flows this user has COMPLETED, in TRACKED_FLOWS order.

    `skipped: true` is not completion — a user who dismissed a walkthrough has not seen what it
    teaches, so they still deserve the nudge. Malformed entries (None, a bare string) count as
    not completed rather than raising, because this JSON is written by the frontend.
    """
    walkthrough = trial_walkthrough or {}
    done = []
    for flow in TRACKED_FLOWS:
        entry = walkthrough.get(flow)
        if isinstance(entry, dict) and entry.get("completed") is True:
            done.append(flow)
    return done


def trial_window(start_date, end_date, now) -> tuple:
    """Return `(day_number, total_days)` for a trial.

    `day_number` floors, so "day 3" means a full 72 hours have elapsed.

    `total_days` is derived from the plan's own dates rather than TRIAL_DURATION_DAYS, so that a
    trial an admin extended or shortened via `createorgplan` renders its real window. It falls
    back to TRIAL_DURATION_DAYS ONLY when the window is non-positive — `createorgplan` sets the
    two dates independently with no validation, and a zero-length window would raise
    ZeroDivisionError inside `_render_trial_progress_bar`. Deliberately not
    `max(total_days, TRIAL_DURATION_DAYS)`: that would silently render a legitimate 7-day trial
    as "Day 7 of 14" on the day it ends.
    """
    day_number = (now - start_date).days
    total_days = (end_date - start_date).days
    if total_days <= 0:
        logger.warning(
            "trial window is non-positive (start=%s end=%s); falling back to %s days",
            start_date,
            end_date,
            TRIAL_DURATION_DAYS,
        )
        total_days = TRIAL_DURATION_DAYS
    return day_number, total_days


# Which flag keys each decision writes once its email has been sent. The completion email
# stamps both `completion` and `day3` to prevent the in-progress email from firing on top of it.
# This is belt-and-braces: rule 2's `EMAIL_COMPLETION not in flags` guard independently prevents
# it, AND the stamp prevents a later run from seeing "no day3 flag" and firing the nudge. Both
# mechanisms should remain — deleting either invites the duplicate.
FLAGS_STAMPED_BY = {
    EMAIL_DAY3: (EMAIL_DAY3,),
    EMAIL_COMPLETION: (EMAIL_COMPLETION, EMAIL_DAY3),
    EMAIL_MIDPOINT: (EMAIL_MIDPOINT,),
    EMAIL_PRE_END: (EMAIL_PRE_END,),
}


def decide_email(day_number: int, completed_count: int, flags: dict, now, end_date):
    """Pick the ONE email due for this trial right now, or None.

    Rules are checked in order and the first match wins — at most one email per trial per run.
    A rule that was also eligible fires on a later run an hour later, so two emails never land
    in the same inbox at the same moment.

    Args:
        day_number: full days elapsed since OrgPlans.start_date
        completed_count: how many of TRACKED_FLOWS are complete (0, 1 or 2)
        flags: the user's `trial_emails_sent` dict
        now / end_date: used only by the pre-end rule

    Returns:
        one of the EMAIL_* constants, or None
    """
    flags = flags or {}

    # 1. Both walkthroughs done, on or after day 3. Sits above the day-3 rule so a user who
    #    finished everything by day 3 gets the congratulations, not the "pick up where you
    #    left off" nudge.
    if (
        day_number >= DAY3_THRESHOLD_DAYS
        and completed_count == len(TRACKED_FLOWS)
        and EMAIL_COMPLETION not in flags
    ):
        return EMAIL_COMPLETION

    # 2. The day-3 nudge — which of the two templates is chosen by the caller from
    #    completed_count. Guarded on the completion flag as well as its own so it can never
    #    follow a congratulations email.
    if (
        day_number >= DAY3_THRESHOLD_DAYS
        and EMAIL_DAY3 not in flags
        and EMAIL_COMPLETION not in flags
    ):
        return EMAIL_DAY3

    # 3 & 4. Date-driven lifecycle nudges. Unconditional on progress — they still fire for a
    #        user who already received the completion email, because an upgrade prompt and an
    #        expiry warning serve a different purpose from the progress nudges.
    if day_number >= MIDPOINT_THRESHOLD_DAYS and EMAIL_MIDPOINT not in flags:
        return EMAIL_MIDPOINT

    if now >= end_date - timedelta(days=PRE_END_DAYS_BEFORE) and EMAIL_PRE_END not in flags:
        return EMAIL_PRE_END

    return None
