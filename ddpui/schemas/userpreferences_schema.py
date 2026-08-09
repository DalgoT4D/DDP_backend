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


# The first three are guided walkthroughs; the `*_nudge` keys are one-shot feature
# coachmarks (see the model's trial_walkthrough comment). They share this Literal — and the
# endpoint below — because both are "has this user been shown X" bookkeeping on the same dict.
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
