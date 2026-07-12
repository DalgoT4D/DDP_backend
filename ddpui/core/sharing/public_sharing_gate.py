"""Task 11 Part A: the org-level public-sharing kill switch.

``OrgPreferences.allow_public_sharing`` (Task 1) is a per-org master switch
for ALL public/token-gated sharing (dashboards, reports, and any future
public-link rtype). It is read fresh on every request -- flipping it does
NOT touch any resource's ``is_public`` flag or ``public_share_token``:

- Toggle endpoints (``dashboard_native_api.toggle_dashboard_sharing``,
  ``report_api.toggle_report_sharing``, and the bulk ``toggle_public``
  action) all flip the flag through ``sharing_actions.set_public``, which
  refuses to newly publish or re-enable a link while the switch is off.
  Turning a link OFF stays allowed always -- people must be able to clean up
  even with the org switch off.
- Public render endpoints (``public_api.py``) treat every existing public
  link as dead (404, matching each endpoint's own "token not found"
  response) while the switch is off. Flipping the switch back on revives
  every existing link immediately -- since no data was ever touched, there
  is nothing to "restore", the very next request just reads a True flag.

An org with no ``OrgPreferences`` row is treated as switch-ON, matching the
column's ``default=True`` (Task 1) -- never 500 for orgs that never created
a preferences row.
"""

from ninja.errors import HttpError

from ddpui.models.org_preferences import OrgPreferences


def org_allows_public_sharing(org_id: int) -> bool:
    """True if ``org_id``'s public-sharing kill switch is on (or unset)."""
    prefs = OrgPreferences.objects.filter(org_id=org_id).only("allow_public_sharing").first()
    return True if prefs is None else prefs.allow_public_sharing


def require_public_sharing_enabled(org) -> None:
    """Raise ``HttpError(403)`` if ``org``'s public-sharing switch is off.

    Only call this on the enable / re-enable path of a share toggle --
    turning a link off must always be allowed regardless of the switch.
    """
    if not org_allows_public_sharing(org.id):
        raise HttpError(
            403,
            "Public sharing is disabled for this organization. Ask an org admin to re-enable it.",
        )
