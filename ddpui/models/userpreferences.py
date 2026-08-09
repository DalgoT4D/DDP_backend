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
    # Keyed by flow name ("product_tour" | "insights" | "automate_pipeline"), each value
    # {"skipped": bool, "completed": bool} — never both true. Per-step progress and which
    # fork (sample/own_data) stays in the frontend's localStorage; this is only the
    # final-state gate deciding whether to offer that flow again.
    trial_walkthrough = models.JSONField(default=dict, blank=True)
    # Which automated trial emails have already gone out, keyed by email kind
    # ("day3" | "completion" | "midpoint" | "pre_end") with an ISO-8601 send timestamp as the
    # value. A sibling of trial_walkthrough rather than a key inside it: that field is keyed by
    # flow name and is iterated by the frontend's flow-gate logic, which must not trip over
    # send-flags. Written only by the trial lifecycle-email sweep.
    trial_emails_sent = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(default=timezone.now)

    def to_json(self) -> dict:
        """Return a dict representation of the model"""
        return {
            "enable_email_notifications": self.enable_email_notifications,
            "disclaimer_shown": self.disclaimer_shown,
            "last_visited_transform_tab": self.last_visited_transform_tab,
            "trial_walkthrough": self.trial_walkthrough,
            "trial_emails_sent": self.trial_emails_sent,
        }
