from typing import Optional, Literal
from ninja import Schema


class CreateUserPreferencesSchema(Schema):
    """Schema for creating user preferences for the user."""

    enable_email_notifications: bool
    disclaimer_shown: Optional[bool] = None
    last_visited_transform_tab: Optional[Literal["ui", "github"]] = None


class UpdateUserPreferencesSchema(Schema):
    """Schema for updating user preferences for the user."""

    enable_email_notifications: Optional[bool] = None
    disclaimer_shown: Optional[bool] = None
    last_visited_transform_tab: Optional[Literal["ui", "github"]] = None


TrialWalkthroughFlow = Literal["product_tour", "insights", "automate_pipeline"]


class UpdateTrialWalkthroughSchema(Schema):
    """Marks one trial-walkthrough flow skipped or completed. Never both true — completing
    a previously-skipped flow clears skipped on the same write."""

    flow: TrialWalkthroughFlow
    skipped: Optional[bool] = None
    completed: Optional[bool] = None


class TrialWalkthroughFlowState(Schema):
    """One flow's entry inside the `UserPreferences.trial_walkthrough` JSONField.

    The two flags are mutually exclusive by construction: the endpoint replaces the whole
    entry on every write, so completing a flow skipped earlier clears `skipped` in the same
    write. Only the FINAL state lives here — per-step progress and which fork the user took
    (sample data vs their own) stay in the frontend's localStorage.
    """

    skipped: bool = False
    completed: bool = False


class TrialWalkthroughState(Schema):
    """The whole `UserPreferences.trial_walkthrough` JSONField — every key it can hold.

    Starts as `{}` for every user and gains one key at a time, written only by
    `PUT /api/userpreferences/trial-walkthrough` when the frontend reports a flow finished or
    dismissed. A key that is absent means the user has neither completed nor skipped that flow.

    NB `product_tour` is tracked here but deliberately does NOT count toward trial completion —
    see `TRACKED_FLOWS` in `ddpui/core/trial/lifecycle_emails.py`, which counts only `insights`
    and `automate_pipeline`.

    This schema is the contract for the column; it is not what the endpoint accepts (that is
    `UpdateTrialWalkthroughSchema`, one flow at a time).
    """

    product_tour: Optional[TrialWalkthroughFlowState] = None
    insights: Optional[TrialWalkthroughFlowState] = None
    automate_pipeline: Optional[TrialWalkthroughFlowState] = None


class TrialEmailsSentState(Schema):
    """The whole `UserPreferences.trial_emails_sent` JSONField — every key it can hold.

    Each value is the ISO-8601 timestamp of when that email went out; a key is present if and
    only if the email has been sent, which is what stops the hourly sweep sending it twice.
    Written only by `ddpui/core/trial/lifecycle_emails.py`, never by the frontend, and never
    returned by `GET /api/userpreferences/`.

    `day3` covers BOTH day-3 templates (not-started and in-progress) because only one of them
    can ever fire for a given user. The completion email stamps `completion` AND `day3`, so a
    congratulations email can never be followed by a "pick up where you left off" nudge.
    """

    day3: Optional[str] = None
    completion: Optional[str] = None
    midpoint: Optional[str] = None
    pre_end: Optional[str] = None
