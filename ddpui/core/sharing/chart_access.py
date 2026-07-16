"""The render path for charts (plan Sec 3.3; v1.1: charts are now
independently shareable).

The two-layer contract (v1.1 spec Sec 3): a chart renders INLINE wherever
its containing dashboard renders — the dashboard-context branch below is
that rule and is unchanged from v1. The chart's OWN general access +
grants govern its STANDALONE surfaces (Charts page, builder, pickers,
by-id data endpoints without a dashboard context). This module holds:

- ``require_chart_view_access`` — the 403 gate for the by-id chart GETs.
  With a ``dashboard_id`` access context it demands BOTH that the chart is
  actually ON that dashboard (membership — without it, ``dashboard_id`` is
  an oracle to read arbitrary charts; plan Sec 5) and that the resolver
  grants >= view on that same-org dashboard. Without one (standalone:
  builder / Charts page) it asks the resolver for view on the CHART itself
  (v1.1) — admins, owners, analysts the chart's ``analyst_level``/grants
  admit; plain Members stay denied (Member chart sharing is deferred, and
  the resolver excludes Member grant contributions for charts).

- ``require_analyst_plus`` — the same standalone role-rank rule, exposed
  for the two table/map POST endpoints' config-only path (a raw,
  not-yet-saved chart config with no ``chart_id``, so there's no ``Chart``
  row to check ownership against — the chart builder's live preview).
  Task 6b.

- ``require_payload_within_chart_config`` — the column-set guard for the
  three table/map POST endpoints' dashboard-context path (Task 6d).
  ``require_chart_view_access`` + the endpoints' schema/table match prove
  the viewer may see THIS chart's table — but those endpoints take the
  chart CONFIG in the request body, so a context-admitted Member could
  still name OTHER columns of that table (or probe single rows via novel
  filters). This guard pins every submitted column reference to the saved
  chart's config; filter columns may additionally come from the framing
  dashboard's configured filters (dashboard filtering adds clauses —
  columns fixed, values free). Applies ONLY when a dashboard context
  admitted the request; Analyst+ standalone/config-only stays untouched.

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

import json
from dataclasses import dataclass
from typing import Any, Callable, Optional, Set, Union

from ninja.errors import HttpError

from ddpui.auth import ANALYST_ROLE
from ddpui.core.sharing.access_resolver import ROLE_RANK, effective_permission
from ddpui.models.dashboard import Dashboard, DashboardComponentType, DashboardFilter
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


def chart_ids_in_tabs(tabs) -> Set[int]:
    """Ids of every chart tile in a raw ``tabs`` JSON structure (a list of
    tab dicts). THE one tabs->components->config.chartId walk (M2
    consolidation) — ``dashboard_chart_ids`` wraps it for a Dashboard row;
    ``update_dashboard``'s tile validation calls it on the incoming payload.

    Fails CLOSED on malformed shapes (M0 review follow-up): a non-list
    ``tabs``, a non-dict tab/component/config, or a non-integer ``chartId``
    contributes nothing instead of raising — a membership check against the
    result then denies, and a tile listing simply omits the junk entry.
    ``bool`` is excluded explicitly (it subclasses ``int``; ``True`` must
    not read as chart id 1)."""
    chart_ids: Set[int] = set()
    if not isinstance(tabs, list):
        return chart_ids
    for tab in tabs:
        if not isinstance(tab, dict):
            continue
        components = tab.get("components")
        if not isinstance(components, dict):
            continue
        for component in components.values():
            if not isinstance(component, dict):
                continue
            if component.get("type") != DashboardComponentType.CHART.value:
                continue
            config = component.get("config")
            if not isinstance(config, dict):
                continue
            chart_id = config.get("chartId")
            if isinstance(chart_id, int) and not isinstance(chart_id, bool):
                chart_ids.add(chart_id)
    return chart_ids


def dashboard_chart_ids(dashboard: Dashboard) -> Set[int]:
    """Ids of every chart placed as a tile on this dashboard, across tabs.
    Public as of M2: the single tile-walk shared by the render gate (below),
    the public chart endpoints (M0 leak fix), the coverage service, and the
    chart<->dashboard listing services."""
    return chart_ids_in_tabs(dashboard.tabs)


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
    is not on that dashboard or the resolver denies view on it. This branch
    is the v1 inline-rendering rule, UNCHANGED in v1.1 — inline access never
    consults the chart's own levels ("no locked tiles, ever").

    Standalone (v1.1): the resolver decides on the CHART itself — admins
    and owners pass (ladder steps 1-2), Analysts pass via the chart's
    ``analyst_level`` (backfilled to "edit", so day-one behavior is
    unchanged) or a grant; plain Members are denied (``member_level`` is
    pinned to "none" and Member grant contributions are excluded for
    charts).
    """
    if dashboard_id is not None:
        try:
            dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
        except Dashboard.DoesNotExist:
            raise HttpError(404, "Dashboard not found") from None
        if chart.id not in dashboard_chart_ids(dashboard):
            raise HttpError(403, "You do not have access to this chart")
        if effective_permission(orguser, "dashboard", dashboard) is None:
            raise HttpError(403, "You do not have access to this chart")
        return

    if effective_permission(orguser, "chart", chart) is None:
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


# Generic on purpose: matches the endpoints' schema/table-mismatch message and
# never names the offending column, so a probing caller learns nothing about
# which columns exist or are protected (no oracle).
_PAYLOAD_MISMATCH = "Payload does not match the referenced chart"

# Saved-config keys whose string value is one warehouse column, across every
# chart_type's config shape (bar/line: dimension_column+extra_dimension_column;
# map: geographic/value plus legacy aggregate_column; legacy raw axes).
_SINGLE_COLUMN_KEYS = (
    "dimension_column",
    "extra_dimension_column",
    "x_axis_column",
    "y_axis_column",
    "geographic_column",
    "value_column",
    "aggregate_column",
)

# Saved-config keys holding a plain list of column-name strings.
_COLUMN_LIST_KEYS = ("dimension_columns", "table_columns")


def _clean_columns(values) -> Set[str]:
    """Non-empty string column names out of a possibly messy iterable."""
    return {v for v in values if isinstance(v, str) and v.strip()}


def _metric_field(metric: Any, name: str) -> Any:
    """Read a metric field off a ChartMetric instance or a raw config dict."""
    if isinstance(metric, dict):
        return metric.get(name)
    return getattr(metric, name, None)


def _clause_columns(clauses: Any) -> Set[str]:
    """Column names from a filters/sort clause list ([{column, ...}, ...])."""
    if not isinstance(clauses, list):
        return set()
    return _clean_columns(c.get("column") for c in clauses if isinstance(c, dict))


def _config_query_columns(config: dict) -> Set[str]:
    """Every column a chart config references in a SELECT/GROUP BY role:
    dimensions (all shapes), axes, geo/value columns, metric columns, map
    layers, and the geographic drill-down hierarchy. Deliberately EXCLUDES
    the config's filter columns — a saved filter constrains rows without
    displaying its column, so it must not become a readable dimension."""
    columns: Set[str] = set()
    columns |= _clean_columns(config.get(key) for key in _SINGLE_COLUMN_KEYS)
    for key in _COLUMN_LIST_KEYS:
        value = config.get(key)
        if isinstance(value, list):
            columns |= _clean_columns(value)
    dimensions = config.get("dimensions")
    if isinstance(dimensions, list):
        columns |= _clean_columns(d.get("column") if isinstance(d, dict) else d for d in dimensions)
    metrics = config.get("metrics")
    if isinstance(metrics, list):
        columns |= _clean_columns(_metric_field(m, "column") for m in metrics)
    layers = config.get("layers")
    if isinstance(layers, list):
        for layer in layers:
            if isinstance(layer, dict):
                columns |= _clean_columns(
                    (layer.get("geographic_column"), layer.get("value_column"))
                )
    hierarchy = config.get("geographic_hierarchy")
    if isinstance(hierarchy, dict):
        levels = [hierarchy.get("base_level"), *(hierarchy.get("drill_down_levels") or [])]
        for level in levels:
            if isinstance(level, dict):
                columns |= _clean_columns((level.get("column"), level.get("parent_column")))
    return columns


def _config_metric_expressions(config: dict) -> Set[str]:
    metrics = config.get("metrics")
    if not isinstance(metrics, list):
        return set()
    return {
        expr
        for expr in (_metric_field(m, "column_expression") for m in metrics)
        if isinstance(expr, str) and expr.strip()
    }


def _config_saved_metric_ids(config: dict) -> Set[str]:
    metrics = config.get("metrics")
    if not isinstance(metrics, list):
        return set()
    return {
        str(mid)
        for mid in (_metric_field(m, "saved_metric_id") for m in metrics)
        if mid is not None
    }


def _dashboard_filter_value_ids(dashboard_filters: Union[str, dict, None]) -> Set[str]:
    """Filter ids named by a dashboard_filters values payload — either the
    already-parsed {filter_id: value} dict (map-data-overlay body) or the
    raw JSON string query param (preview endpoints). Mirrors the handlers'
    tolerant parsing: invalid JSON / non-dict is ignored there, so it names
    no filters here."""
    if isinstance(dashboard_filters, str):
        try:
            dashboard_filters = json.loads(dashboard_filters)
        except json.JSONDecodeError:
            return set()
    if not isinstance(dashboard_filters, dict):
        return set()
    return {str(key) for key in dashboard_filters}


def require_payload_within_chart_config(
    chart: Chart,
    dashboard_id: int,
    payload: Any,
    dashboard_filters: Union[str, dict, None] = None,
) -> None:
    """Raise 403 unless every column the payload references is derivable
    from ``chart``'s saved config (Task 6d).

    For the three table/map POST endpoints' dashboard-context path only —
    the caller must have already passed ``require_chart_view_access`` with
    this ``dashboard_id`` (so the dashboard exists in-org and admits the
    viewer) and the schema/table match. ``payload`` is duck-typed: a
    ``ChartDataPayload`` or the map endpoint's ``MapDataOverlayPayload``
    (which lives in the API layer, so core cannot import its type).

    The rules:

    - Query columns (dimensions, axes, metric columns, geographic/value
      columns — top-level or nested in ``extra_config``) must be a subset
      of the columns the SAVED config references, including map layers and
      the drill-down hierarchy (drill-down legitimately re-points
      ``geographic_column`` at a deeper hierarchy level).
    - Metric ``column_expression`` is raw SQL (``literal_column``): allowed
      only verbatim from the saved config. ``saved_metric_id`` likewise
      only ids the saved config already names.
    - Filter columns (``extra_config.filters`` and the map endpoint's
      drill-down ``filters`` keys) may also be saved filter columns or the
      framing dashboard's configured filter columns — dashboard filtering
      and drill-down add clauses to tile queries; their VALUES stay free,
      novel COLUMNS do not (single-row probe primitive).
    - Sort columns may also be submitted metric aliases (already-guarded
      aggregates; ``apply_chart_sorting`` resolves aliases first) or saved
      sort columns.
    - The ``dashboard_filters`` {filter_id: value} payload may only name
      the framing dashboard's own filters — a foreign filter id would
      smuggle in that filter's column.

    Violations all raise the same generic 403 (no column echoed — no
    oracle). Analyst+ standalone / config-only requests must never be
    routed here; their behavior is unchanged by Task 6d.
    """
    saved = chart.extra_config if isinstance(chart.extra_config, dict) else {}
    allowed_query_columns = _config_query_columns(saved)
    saved_expressions = _config_metric_expressions(saved)
    saved_metric_ids = _config_saved_metric_ids(saved)
    saved_filter_columns = _clause_columns(saved.get("filters"))
    saved_sort_columns = _clause_columns(saved.get("sort"))

    dashboard_filter_rows = DashboardFilter.objects.filter(dashboard_id=dashboard_id).values_list(
        "id", "column_name"
    )
    allowed_filter_ids = {str(fid) for fid, _ in dashboard_filter_rows}
    dashboard_filter_columns = _clean_columns(col for _, col in dashboard_filter_rows)

    submitted_extra = getattr(payload, "extra_config", None)
    submitted_extra = submitted_extra if isinstance(submitted_extra, dict) else {}

    # Query columns: top-level payload fields + anything config-shaped
    # smuggled inside extra_config.
    submitted_columns = _clean_columns(
        getattr(payload, field, None)
        for field in (
            "x_axis",
            "y_axis",
            "dimension_col",
            "extra_dimension",
            "geographic_column",
            "value_column",
        )
    )
    submitted_columns |= _clean_columns(getattr(payload, "dimensions", None) or [])
    submitted_columns |= _config_query_columns(submitted_extra)

    # Metrics: columns join the query-column check; expressions and saved
    # ids must come verbatim from the saved config.
    submitted_metrics = list(getattr(payload, "metrics", None) or [])
    extra_metrics = submitted_extra.get("metrics")
    if isinstance(extra_metrics, list):
        submitted_metrics += extra_metrics
    metric_aliases: Set[str] = set()
    for metric in submitted_metrics:
        submitted_columns |= _clean_columns((_metric_field(metric, "column"),))
        expression = _metric_field(metric, "column_expression")
        if isinstance(expression, str) and expression.strip():
            if expression not in saved_expressions:
                raise HttpError(403, _PAYLOAD_MISMATCH)
        metric_id = _metric_field(metric, "saved_metric_id")
        if metric_id is not None and str(metric_id) not in saved_metric_ids:
            raise HttpError(403, _PAYLOAD_MISMATCH)
        metric_aliases |= _clean_columns((_metric_field(metric, "alias"),))

    if not submitted_columns <= allowed_query_columns:
        raise HttpError(403, _PAYLOAD_MISMATCH)

    # Filter columns: extra_config.filters clauses + the map endpoint's
    # drill-down filters dict ({column: value}).
    submitted_filter_columns = _clause_columns(submitted_extra.get("filters"))
    drill_filters = getattr(payload, "filters", None)
    if isinstance(drill_filters, dict):
        submitted_filter_columns |= _clean_columns(drill_filters.keys())
    allowed_filter_columns = allowed_query_columns | saved_filter_columns | dashboard_filter_columns
    if not submitted_filter_columns <= allowed_filter_columns:
        raise HttpError(403, _PAYLOAD_MISMATCH)

    # Sort columns.
    submitted_sort_columns = _clause_columns(submitted_extra.get("sort"))
    if not submitted_sort_columns <= (allowed_query_columns | saved_sort_columns | metric_aliases):
        raise HttpError(403, _PAYLOAD_MISMATCH)

    # dashboard_filters values payload: only the framing dashboard's filters.
    if not _dashboard_filter_value_ids(dashboard_filters) <= allowed_filter_ids:
        raise HttpError(403, _PAYLOAD_MISMATCH)


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
