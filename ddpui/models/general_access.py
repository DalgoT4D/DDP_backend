"""Shared choice enums for general-access (org-wide sharing) fields.

Used by Dashboard, ReportSnapshot, Metric, KPI, and Alert to control what
each role (Analyst, Member) can do with a resource by default, before any
per-user grants narrow or widen that. Admins are never stored here — they
always resolve to full (edit) access (``access_resolver.effective_permission``
step 1); there is deliberately no ``admin_level`` field/choice.

D1 (permission-model rework): this replaces the old single
``GeneralAudience`` (private/admins/analysts_plus/all_users) x
``GeneralLevel`` (view/edit) pair with one independent ``AccessLevel`` per
role -- ``analyst_level`` and ``member_level`` -- so "Analyst=Edit,
Member=View" is storable, which the old audience-threshold model could
never express (an "analysts_plus" audience gave everyone at or above that
tier the SAME single level).
"""

from django.db import models


class AccessLevel(models.TextChoices):
    """What a given role (Analyst or Member) can do with a resource by
    default: nothing, view, or edit. Independently settable per role."""

    NONE = "none"
    VIEW = "view"
    EDIT = "edit"


# Ordering for the narrowing warn-and-offer protocol (sharing_actions) and
# any other "did this get more/less permissive" comparison. Kept alongside
# the enum since it's a structural property of AccessLevel, not sharing
# business logic.
ACCESS_LEVEL_RANK = {
    AccessLevel.NONE: 0,
    AccessLevel.VIEW: 1,
    AccessLevel.EDIT: 2,
}


class GeneralLevel(models.TextChoices):
    """A grant's permission level: view or edit. UNCHANGED by D1 -- this is
    ``ResourceShare.permission`` and access-request ``requested_permission``,
    not the per-role general-access level above."""

    VIEW = "view"
    EDIT = "edit"
