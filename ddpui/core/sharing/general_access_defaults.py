"""Task 11 Part C: seed org-default General access at resource creation.

Every shareable rtype (dashboard/report/alert/metric/kpi -- the ``general``
entries in ``shareable_types.RESOURCE_TYPES``) has ``general_audience``/
``general_level`` columns whose Django field defaults are
``all_users``/``view``. This helper lets a create path override those with
the org's configured preference (``OrgPreferences.default_general_audience``/
``default_general_level``, Task 1) instead, without each of the 5 create
paths re-implementing the same "read OrgPreferences, fall back" lookup.
"""

from typing import Tuple

from ddpui.models.general_access import GeneralAudience, GeneralLevel
from ddpui.models.org_preferences import OrgPreferences


def get_org_general_defaults(org_id: int) -> Tuple[str, str]:
    """(general_audience, general_level) to seed a newly created shareable
    resource with: the org's configured defaults if it has a preferences
    row, else the model defaults (``all_users``/``view``)."""
    prefs = (
        OrgPreferences.objects.filter(org_id=org_id)
        .only("default_general_audience", "default_general_level")
        .first()
    )
    if prefs is None:
        return GeneralAudience.ALL_USERS, GeneralLevel.VIEW
    return prefs.default_general_audience, prefs.default_general_level
