from typing import Dict, Optional, Literal
from ninja import Schema


class CreateUserPreferencesSchema(Schema):
    """Schema for creating user preferences for the user."""

    enable_email_notifications: bool
    enable_schema_change_notifications: Optional[bool] = None
    disclaimer_shown: Optional[bool] = None
    last_visited_transform_tab: Optional[Literal["ui", "github"]] = None


class UpdateUserPreferencesSchema(Schema):
    """Schema for updating user preferences for the user."""

    enable_email_notifications: Optional[bool] = None
    enable_schema_change_notifications: Optional[bool] = None
    disclaimer_shown: Optional[bool] = None
    last_visited_transform_tab: Optional[Literal["ui", "github"]] = None


# The first three are guided walkthroughs. The `*_nudge` keys are one-shot feature coachmarks
# shown on every visit to /reports, /alerts or /metrics until dismissed — they belong to no flow
# and only ever write completed=True. Both kinds share this Literal, and the endpoint, because
# both are "has this user been shown X" bookkeeping on the same dict.
TrialWalkthroughFlow = Literal[
    "product_tour",
    "insights",
    "automate_pipeline",
    "reports_nudge",
    "alerts_nudge",
    "metrics_nudge",
]


class UpdateTrialWalkthroughSchema(Schema):
    """Marks one trial-walkthrough flow skipped or completed. Never both true — completing
    a previously-skipped flow clears skipped on the same write.

    A feature nudge only ever writes completed=True (dismissed); it has no skipped state."""

    flow: TrialWalkthroughFlow
    skipped: Optional[bool] = None
    completed: Optional[bool] = None


class TrialWalkthroughFlowState(Schema):
    """One entry inside the `UserPreferences.trial_walkthrough` JSONField.

    The column is `TrialWalkthrough` — this schema is a single value in it::

        {
            "product_tour":      {"skipped": false, "completed": true},
            "insights":          {"skipped": true,  "completed": false},
            "automate_pipeline": {"skipped": false, "completed": true}
        }

    It starts as `{}` for every user and gains one key at a time, written only by
    `PUT /api/userpreferences/trial-walkthrough` when the frontend reports a flow finished or
    dismissed. An absent key means the user has neither completed nor skipped that flow, so
    `{}` and the three-key example above are both valid states of the same column.

    The two flags are mutually exclusive by construction: the endpoint replaces the whole entry
    on every write, so completing a flow skipped earlier clears `skipped` in the same write.
    Only the FINAL state lives here — per-step progress and which fork the user took (sample
    data vs their own) stay in the frontend's localStorage.

    NB `product_tour` is stored here but deliberately does NOT count toward trial completion —
    see `TRACKED_FLOWS` in `ddpui/core/trial/lifecycle_emails.py`, which counts only `insights`
    and `automate_pipeline`.
    """

    skipped: bool = False
    completed: bool = False


# The `UserPreferences.trial_walkthrough` column in one line: keyed by flow, one state each.
# An alias rather than a model because the column is a mapping whose keys are the values of a
# Literal — a model would have to restate those three names a second time, which is exactly the
# duplication that lets a schema drift from the code that writes it.
TrialWalkthrough = Dict[TrialWalkthroughFlow, TrialWalkthroughFlowState]


class TrialEmailsSentState(Schema):
    """The whole `UserPreferences.trial_emails_sent` JSONField — every key it can hold::

        {
            "day3":       "2026-08-12T09:04:11.201+00:00",
            "completion": "2026-08-14T17:04:09.882+00:00",
            "midpoint":   "2026-08-16T09:04:10.417+00:00",
            "pre_end":    "2026-08-21T09:04:12.006+00:00"
        }

    Each value is the ISO-8601 timestamp of when that email went out; a key is present if and
    only if the email has been sent, which is what stops the hourly sweep sending it twice. So
    a trial on day 5 that has had one email looks like `{"day3": "2026-08-12T09:04:11.201+00:00"}`
    and a brand-new trial looks like `{}`. Written only by
    `ddpui/core/trial/lifecycle_emails.py`, never by the frontend, and never returned by
    `GET /api/userpreferences/`.

    `day3` covers BOTH day-3 templates (not-started and in-progress) because only one of them
    can ever fire for a given user. The completion email stamps `completion` AND `day3`, so a
    congratulations email can never be followed by a "pick up where you left off" nudge.
    """

    day3: Optional[str] = None
    completion: Optional[str] = None
    midpoint: Optional[str] = None
    pre_end: Optional[str] = None
