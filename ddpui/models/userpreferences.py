from django.db import models
from django.utils import timezone
from ddpui.models.org_user import OrgUser


class UserPreferences(models.Model):
    """Model to store user preferences for notifications"""

    orguser = models.OneToOneField(OrgUser, on_delete=models.CASCADE, related_name="preferences")
    enable_discord_notifications = models.BooleanField(default=False)  # deprecated
    discord_webhook = models.URLField(blank=True, null=True)  # deprecated
    enable_email_notifications = models.BooleanField(default=False)
    disclaimer_shown = models.BooleanField(default=False)
    last_visited_transform_tab = models.CharField(
        max_length=10,
        choices=[("ui", "UI Transform"), ("github", "DBT Transform")],
        null=True,
        blank=True,
    )
    # Two kinds of entry, same {"skipped": bool, "completed": bool} value shape — never both
    # true:
    #  - guided flows, keyed "product_tour" | "insights" | "automate_pipeline". Per-step
    #    progress and which fork (sample/own_data) stays in the frontend's localStorage; this
    #    is only the final-state gate deciding whether to offer that flow again.
    #  - one-shot feature nudges, keyed "reports_nudge" | "alerts_nudge" | "metrics_nudge".
    #    These belong to no flow: a trial user landing on /reports, /alerts or /metrics gets a
    #    coachmark explaining the feature, shown on every visit until they dismiss it, which
    #    writes completed=True here. They never set skipped.
    # An absent key means "not decided" / "not yet dismissed" for both kinds.
    trial_walkthrough = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(default=timezone.now)

    def to_json(self) -> dict:
        """Return a dict representation of the model"""
        return {
            "enable_email_notifications": self.enable_email_notifications,
            "disclaimer_shown": self.disclaimer_shown,
            "last_visited_transform_tab": self.last_visited_transform_tab,
            "trial_walkthrough": self.trial_walkthrough,
        }
