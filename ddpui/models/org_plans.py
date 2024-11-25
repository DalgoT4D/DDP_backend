from django.db import models
from django.utils import timezone
from ddpui.models.org import Org


class OrgPlans(models.Model):
    """Model to store org preferences for settings panel"""

    org = models.OneToOneField(Org, on_delete=models.CASCADE, related_name="org_plans")
    base_plan = models.CharField(
        null=True, max_length=255, default=None
    )  # plan DALGO or FREE TRAIL
    superset_included = models.BooleanField(null=True, default=False)
    subscription_duration = models.CharField(
        null=True, max_length=255, default=None
    )  # monthly, quatery.
    features = models.JSONField(
        null=True, default=None
    )  # put the features as json and map in the frontend.
    start_date = models.DateTimeField(null=True, blank=True, default=None)
    end_date = models.DateTimeField(null=True, blank=True, default=None)
    can_upgrade_plan = models.BooleanField(default=False)
    upgrade_requested = models.BooleanField(default=False)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(default=timezone.now)

    def to_json(self) -> dict:
        """Return a dict representation of the model"""
        return {
            "org": {
                "name": self.org.name,
                "slug": self.org.slug,
                "type": self.org.type,
            },
            "base_plan": self.base_plan,
            "superset_included": self.superset_included,
            "subscription_duration": self.subscription_duration,
            "features": self.features,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "can_upgrade_plan": self.can_upgrade_plan,
            "upgrade_requested": self.upgrade_requested,
        }
