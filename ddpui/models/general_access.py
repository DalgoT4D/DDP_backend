"""Shared choice enums for general-access (org-wide sharing) fields.

Used by Dashboard, ReportSnapshot, Metric, KPI, and Alert to control who in
the org can see/edit a resource by default, before any per-user grants
(added in a later task) narrow or widen that.
"""

from django.db import models


class GeneralAudience(models.TextChoices):
    """Who, org-wide, gets the default (non-grant) level of access."""

    PRIVATE = "private"
    ADMINS = "admins"
    ANALYSTS_PLUS = "analysts_plus"
    ALL_USERS = "all_users"


class GeneralLevel(models.TextChoices):
    """What the general audience can do: view or edit."""

    VIEW = "view"
    EDIT = "edit"
