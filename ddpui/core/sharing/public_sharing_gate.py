"""The org-level public-sharing kill switch.

Read fresh on every request; flipping it never touches any resource's
``is_public`` flag or token. While off: enabling a link is refused,
disabling stays allowed, and public render endpoints 404. Flipping it back
on revives every existing link immediately. An org with no
``OrgPreferences`` row is treated as switch-on (the column default).
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
