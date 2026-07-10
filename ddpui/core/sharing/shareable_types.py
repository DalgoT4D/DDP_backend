"""The menu: which resource types can be shared, and what capabilities they
support. DATA ONLY — no logic, no branching. This is the ONE place resource
types are enumerated; ``access_resolver.py`` must never branch on
``if resource_type == "dashboard"`` — it reads the registry instead.

Every registered entry's model must satisfy the shareable contract: it has
``general_audience``, ``general_level``, ``owner``, ``created_by``, ``org``,
and a string-able pk. ``chart`` is deliberately NOT registered — charts ride
along with their dashboard and are not independently shareable.
"""

from dataclasses import dataclass
from typing import Optional, Type

from django.db.models import Model

from ddpui.models.alert import Alert
from ddpui.models.dashboard import Dashboard
from ddpui.models.metric import KPI, Metric
from ddpui.models.report import ReportSnapshot


@dataclass(frozen=True)
class ShareableType:
    """One registry entry: a shareable resource type and its capability flags."""

    rtype: str
    model: Type[Model]
    general: bool  # supports Layer 1 general access (general_audience/general_level)
    grants: bool  # supports Layer 2 per-principal ResourceShare grants
    public_link: bool  # supports a public share link
    requests: bool  # supports "request access" flow
    share_permission_slug: str  # RBAC slug gating this rtype's sharing mutations


RESOURCE_TYPES: dict[str, ShareableType] = {
    "dashboard": ShareableType(
        rtype="dashboard",
        model=Dashboard,
        general=True,
        grants=True,
        public_link=True,
        requests=True,
        share_permission_slug="can_share_dashboards",
    ),
    "report": ShareableType(
        rtype="report",
        model=ReportSnapshot,
        general=True,
        grants=True,
        public_link=True,
        requests=True,
        share_permission_slug="can_share_reports",
    ),
    "alert": ShareableType(
        rtype="alert",
        model=Alert,
        general=True,
        grants=True,
        public_link=False,
        requests=True,
        share_permission_slug="can_share_alerts",
    ),
    "metric": ShareableType(
        rtype="metric",
        model=Metric,
        general=True,
        grants=False,
        public_link=False,
        requests=True,
        share_permission_slug="can_share_metrics",
    ),
    "kpi": ShareableType(
        rtype="kpi",
        model=KPI,
        general=True,
        grants=False,
        public_link=False,
        requests=True,
        share_permission_slug="can_share_kpis",
    ),
}


def is_valid_rtype(rtype: str) -> bool:
    """True if ``rtype`` is a registered shareable resource type."""
    return rtype in RESOURCE_TYPES


def get_resource_type(rtype: str) -> Optional[ShareableType]:
    """Look up the registry entry for ``rtype``. Returns None on unknown."""
    return RESOURCE_TYPES.get(rtype)


def get_model_for_rtype(rtype: str) -> Optional[Type[Model]]:
    """Return the model class registered for ``rtype``, or None if unknown."""
    entry = RESOURCE_TYPES.get(rtype)
    return entry.model if entry else None
