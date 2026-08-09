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
