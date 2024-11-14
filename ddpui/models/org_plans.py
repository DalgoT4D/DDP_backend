from django.db import models
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from django.utils import timezone


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
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(default=timezone.now)
