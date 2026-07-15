"""Task 11 Part C: seed org-default General access at resource creation.

D1 (permission-model rework): every shareable rtype (dashboard/report/alert/
metric/kpi -- the ``general`` entries in ``shareable_types.RESOURCE_TYPES``)
has ``analyst_level``/``member_level`` columns whose Django field defaults
are ``none``/``none``. This helper lets a create path override those with
the org's configured preference (``OrgPreferences.default_analyst_level``/
``default_member_level``) instead, without each of the 5 create paths
re-implementing the same "read OrgPreferences, fall back" lookup.
"""

from typing import Tuple

from ddpui.models.general_access import AccessLevel
from ddpui.models.org_preferences import OrgPreferences


def get_org_role_level_defaults(org_id: int) -> Tuple[str, str]:
    """(analyst_level, member_level) to seed a newly created shareable
    resource with: the org's configured defaults if it has a preferences
    row, else the model defaults (``none``/``none``)."""
    prefs = (
        OrgPreferences.objects.filter(org_id=org_id)
        .only("default_analyst_level", "default_member_level")
        .first()
    )
    if prefs is None:
        return AccessLevel.NONE, AccessLevel.NONE
    return prefs.default_analyst_level, prefs.default_member_level
