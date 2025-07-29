from enum import Enum

from django.db import models
from django.utils import timezone
from ddpui.models.org import Org


class OrgPlanType(str, Enum):
    FREE_TRIAL = "Free Trial"
    DALGO = "Dalgo"
    INTERNAL = "Internal"

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class OrgPlans(models.Model):
    """Model to store org preferences for settings panel"""

    org = models.OneToOneField(Org, on_delete=models.CASCADE, related_name="org_plans")
    base_plan = models.CharField(
        null=True,
        max_length=255,
        default=None,
    )  # OrgPlanType.choices()
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

    def __str__(self):
        start_date = self.start_date.strftime("%Y-%m-%d") if self.start_date else "Not started"
        end_date = self.end_date.strftime("%Y-%m-%d") if self.end_date else "No end"
        return (
            f"Org={self.org.slug} Plan={self.base_plan} Superset={str(self.superset_included)} "
            + f"Duration={self.subscription_duration} Start={start_date} End={end_date} "
            + f"Can Upgrade={str(self.can_upgrade_plan)} Upgrade requested={self.upgrade_requested}"
        )
