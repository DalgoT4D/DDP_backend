"""Registry of resource types that participate in the sharing system.

Adding a new rtype means one entry here — the grants API, resolver, and
list filter all consume this table. Keep entries minimal; anything that
grows past model + fetch belongs on the model itself.
"""

from typing import Optional, TypedDict

from django.db.models import Model

from ddpui.models.dashboard import Dashboard
from ddpui.models.org import Org
from ddpui.models.visualization import Chart


class ShareableTypeEntry(TypedDict):
    model: type[Model]
    # The `id_kwarg` names the URL param used by decorators when they
    # eventually extract the resource. Kept here so the enforcement pass
    # can consume the same registry.
    id_kwarg: str


RTYPES: dict[str, ShareableTypeEntry] = {
    "dashboard": {"model": Dashboard, "id_kwarg": "dashboard_id"},
    "chart": {"model": Chart, "id_kwarg": "chart_id"},
    # report, kpi, alert appended as we wire each rtype.
}


def get_rtype_entry(rtype: str) -> ShareableTypeEntry:
    """Return the registry entry for an rtype; raise if unknown (fail-fast at
    the API boundary — unknown rtypes are a client bug, not a 404)."""
    if rtype not in RTYPES:
        raise ValueError(f"unknown rtype: {rtype}")
    return RTYPES[rtype]


def get_resource(org: Org, rtype: str, resource_id) -> Optional[Model]:
    """Org-scoped fetch. Returns None when the resource does not exist in this
    org — a cross-org resource must be indistinguishable from a missing one so
    the API returns 404 without leaking whether it exists elsewhere.
    """
    entry = get_rtype_entry(rtype)
    return entry["model"].objects.filter(org=org, pk=resource_id).first()
