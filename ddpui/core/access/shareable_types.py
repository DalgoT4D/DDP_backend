"""Registry of resource types that participate in the sharing system.

Adding a new rtype means one entry here — the grants API, resolver, and
list filter all consume this table. Keep entries minimal; anything that
grows past model + fetch belongs on the model itself.
"""

from typing import Optional, TypedDict

from django.db.models import Model

from ddpui.models.dashboard import Dashboard
from ddpui.models.metric import KPI
from ddpui.models.org import Org
from ddpui.models.report import ReportSnapshot
from ddpui.models.resource_share import ResourceType
from ddpui.models.visualization import Chart


class ShareableTypeEntry(TypedDict):
    model: type[Model]


RTYPES: dict[ResourceType, ShareableTypeEntry] = {
    ResourceType.DASHBOARD: {"model": Dashboard},
    ResourceType.CHART: {"model": Chart},
    ResourceType.REPORT: {"model": ReportSnapshot},
    ResourceType.KPI: {"model": KPI},
}


def get_rtype_entry(rtype: ResourceType) -> ShareableTypeEntry:
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
