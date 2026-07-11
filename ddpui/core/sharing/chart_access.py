"""The render path for charts, which are NOT shareable (plan Sec 3.3).

A chart is visible wherever its dashboards are visible. This module holds
the two pieces of that contract:

- ``require_chart_view_access`` — the 403 gate for the by-id chart GETs.
  With a ``dashboard_id`` access context it demands BOTH that the chart is
  actually ON that dashboard (membership — without it, ``dashboard_id`` is
  an oracle to read arbitrary charts; plan Sec 5) and that the resolver
  grants >= view on that same-org dashboard. Without one (standalone:
  builder / Charts page) it keeps today's Analyst+ behavior, admits the
  chart's owner, and denies plain Members.

- ``require_analyst_plus`` — the same standalone role-rank rule, exposed
  for the two table/map POST endpoints' config-only path (a raw,
  not-yet-saved chart config with no ``chart_id``, so there's no ``Chart``
  row to check ownership against — the chart builder's live preview).
  Task 6b.

- ``run_chart_query`` — the single choke-point every warehouse-bound chart
  execution on the gated paths routes through. A pass-through today; it
  exists so Layer 2/3 (row-level policies, public-link constraints) has
  exactly one seam to hook. ``ViewerContext`` already admits
  ``PublicLinkContext`` so public-token renders can be rewired through it
  later without changing the signature.

Like ``gates.py``, this module raises HTTP errors but never decides access
itself beyond the chart contract — ``effective_permission`` stays the
single source of truth for the dashboard decision.
"""

from dataclasses import dataclass
from typing import Callable, Optional, Set, Union

from ninja.errors import HttpError

from ddpui.auth import ANALYST_ROLE
from ddpui.core.sharing.access_resolver import ROLE_RANK, effective_permission
from ddpui.models.dashboard import Dashboard, DashboardComponentType
from ddpui.models.org_user import OrgUser
from ddpui.models.visualization import Chart


@dataclass(frozen=True)
class PublicLinkContext:
    """Viewer context for public token renders. Not wired to any endpoint in
    this task — it only shapes the ``run_chart_query`` seam so Layer 2 can
    route public renders through the same choke-point."""

    org_id: int
    share_token: str = ""


ViewerContext = Union[OrgUser, PublicLinkContext]


@dataclass(frozen=True)
class ChartRenderContext:
    """Access context for one chart render: which dashboard (if any) framed
    the request. ``dashboard_id=None`` means standalone (builder/Charts page).
    Distinct from the ``dashboard_filters`` request param, which is a
    filter-values payload, not an access context."""

    dashboard_id: Optional[int] = None


def _dashboard_chart_ids(dashboard: Dashboard) -> Set[int]:
    """Ids of every chart placed as a tile on this dashboard, across tabs.
    Mirrors the tabs->components->config.chartId walk in
    ChartService.get_chart_dashboards / DashboardService.get_dashboard_charts."""
    chart_ids: Set[int] = set()
    for tab in dashboard.tabs or []:
        for component in (tab.get("components") or {}).values():
            if component.get("type") == DashboardComponentType.CHART.value:
                chart_id = component.get("config", {}).get("chartId")
                if chart_id is not None:
                    chart_ids.add(chart_id)
    return chart_ids


def _is_chart_owner(orguser: OrgUser, chart: Chart) -> bool:
    """owner_id wins; created_by is the fallback when owner is null. Mirrors
    ddpui.core.ownership.can_delete_resource (and the resolver's _is_owner)."""
    owner_id = getattr(chart, "owner_id", None)
    if owner_id is not None:
        return owner_id == orguser.id
    created_by_id = getattr(chart, "created_by_id", None)
    return created_by_id is not None and created_by_id == orguser.id


def _is_analyst_plus(orguser: OrgUser) -> bool:
    role = getattr(orguser, "new_role", None)
    rank = ROLE_RANK.get(getattr(role, "slug", None) if role is not None else None)
    return rank is not None and rank >= ROLE_RANK[ANALYST_ROLE]


def require_chart_view_access(
    orguser: OrgUser, chart: Chart, dashboard_id: Optional[int] = None
) -> None:
    """Raise unless ``orguser`` may view ``chart`` in this context.

    Dashboard context (``dashboard_id`` given): 404 if the dashboard does
    not exist in the viewer's org (cross-org ids are indistinguishable from
    nonexistent ones, matching the detail-GET convention); 403 if the chart
    is not on that dashboard or the resolver denies view on it.

    Standalone: Analyst+ passes (today's behavior), the chart's owner
    passes, everyone else (plain Members, null/legacy roles) gets 403.
    """
    if dashboard_id is not None:
        try:
            dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
        except Dashboard.DoesNotExist:
            raise HttpError(404, "Dashboard not found") from None
        if chart.id not in _dashboard_chart_ids(dashboard):
            raise HttpError(403, "You do not have access to this chart")
        if effective_permission(orguser, "dashboard", dashboard) is None:
            raise HttpError(403, "You do not have access to this chart")
        return

    if _is_analyst_plus(orguser):
        return
    if _is_chart_owner(orguser, chart):
        return
    raise HttpError(403, "You do not have access to this chart")


def require_analyst_plus(orguser: OrgUser) -> None:
    """Raise 403 unless ``orguser``'s role ranks Analyst or above.

    For contexts that have no ``Chart`` row to check ownership against --
    the chart-builder's live/unsaved-config preview on the table/map POST
    endpoints (``chart-data-preview``, ``map-data-overlay``): a raw
    schema/table/metrics payload with no ``chart_id`` yet, so there's no
    owner to fall back to and no dashboard to frame it. Members can't reach
    the builder, so this keeps them out; Analyst+ keeps today's behavior.
    Shares ``_is_analyst_plus`` with ``require_chart_view_access``'s
    standalone branch so the role-rank rule lives in exactly one place.
    """
    if not _is_analyst_plus(orguser):
        raise HttpError(403, "You do not have access to this data")


def run_chart_query(
    viewer_ctx: ViewerContext,  # pylint: disable=unused-argument
    chart: Chart,  # pylint: disable=unused-argument
    context: ChartRenderContext,  # pylint: disable=unused-argument
    execute: Callable[[], dict],
) -> dict:
    """Execute a warehouse-bound chart query. Access no-op today — a pure
    pass-through to ``execute`` (injected by the caller, because query
    construction currently lives in the API layer and core must not import
    it). Layer 2/3 hooks its checks here, in front of ``execute()``."""
    return execute()
