"""Seed org-default general access at resource creation: one shared
"read OrgPreferences, fall back" lookup for every create path.
"""

from typing import Tuple

from ddpui.models.general_access import AccessLevel
from ddpui.models.org_preferences import OrgPreferences


def get_org_role_level_defaults(org_id: int) -> Tuple[str, str]:
    """(analyst_level, member_level) to seed a new shareable resource: the
    org's configured defaults, else (view, view). The fallback is deliberately
    not the model field defaults (none/none) — it preserves the pre-existing
    product default for orgs that never touched this setting."""
    prefs = (
        OrgPreferences.objects.filter(org_id=org_id)
        .only("default_analyst_level", "default_member_level")
        .first()
    )
    if prefs is None:
        return AccessLevel.VIEW, AccessLevel.VIEW
    return prefs.default_analyst_level, prefs.default_member_level
