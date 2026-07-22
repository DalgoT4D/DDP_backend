"""Registry of shareable resource types and their capabilities. Data only —
the one place resource types are enumerated; consumers read the registry
instead of branching on rtype. Every registered model must satisfy the
shareable contract: ``analyst_level``, ``member_level``, ``owner``,
``created_by``, ``org``, and a string-able pk.
"""

from dataclasses import dataclass
from typing import Optional, Type

from django.db.models import Model

from ddpui.models.alert import Alert
from ddpui.models.dashboard import Dashboard
from ddpui.models.metric import KPI, Metric
from ddpui.models.report import ReportSnapshot
from ddpui.models.visualization import Chart


@dataclass(frozen=True)
class ShareableType:
    """One registry entry: a shareable resource type and its capability flags."""

    rtype: str
    model: Type[Model]
    general: bool  # supports Layer 1 general access (analyst_level/member_level)
    grants: bool  # supports Layer 2 per-principal ResourceShare grants
    public_link: bool  # supports a public share link
    requests: bool  # supports "request access" flow
    share_permission_slug: str  # RBAC slug gating this rtype's sharing mutations
    # False = Member sharing is deferred for this rtype: member_level pinned
    # to "none", Member grants/invites/requests rejected, and the resolver
    # gives Member viewers nothing beyond ownership.
    member_sharing: bool = True
    # v1.2 flat-pool flip (plan §5), rolled out per rtype: True = a Member's
    # edit grant is honored as real edit; False = capped at view (v1 behavior).
    member_edit_grants: bool = False


RESOURCE_TYPES: dict[str, ShareableType] = {
    "chart": ShareableType(
        rtype="chart",
        model=Chart,
        general=True,
        grants=True,
        public_link=False,
        requests=True,
        share_permission_slug="can_share_charts",
        member_sharing=False,
    ),
    "dashboard": ShareableType(
        rtype="dashboard",
        model=Dashboard,
        general=True,
        grants=True,
        public_link=True,
        requests=True,
        share_permission_slug="can_share_dashboards",
        member_edit_grants=True,
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
        grants=True,
        public_link=False,
        requests=True,
        share_permission_slug="can_share_metrics",
    ),
    "kpi": ShareableType(
        rtype="kpi",
        model=KPI,
        general=True,
        grants=True,
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
