"""Shared choice enums for general-access fields: what each role (Analyst,
Member) can do with a resource by default, before per-user grants. Admins
always resolve to full access — there is deliberately no ``admin_level``.
"""

from django.db import models


class AccessLevel(models.TextChoices):
    """What a given role (Analyst or Member) can do with a resource by
    default: nothing, view, or edit. Independently settable per role."""

    NONE = "none"
    VIEW = "view"
    EDIT = "edit"


# Ordering for "did this get more/less permissive" comparisons.
ACCESS_LEVEL_RANK = {
    AccessLevel.NONE: 0,
    AccessLevel.VIEW: 1,
    AccessLevel.EDIT: 2,
}


class GeneralLevel(models.TextChoices):
    """A grant's permission level: view or edit — ``ResourceShare.permission``
    and access-request ``requested_permission``, not the per-role level above."""

    VIEW = "view"
    EDIT = "edit"
