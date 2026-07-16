"""Chart render-path access: inline (via a dashboard) vs standalone.

A chart renders inline wherever its containing dashboard renders; the
chart's own general access + grants govern its standalone surfaces
(Charts page, builder, pickers, by-id data endpoints).

- ``require_chart_view_access`` — the 403 gate for by-id chart GETs. With a
  dashboard context it checks both tile membership (without it,
  ``dashboard_id`` is an oracle to read arbitrary charts) and resolver view
  on the dashboard; standalone it asks the resolver for view on the chart.
- ``require_analyst_plus`` — the role-rank rule for the config-only preview
  path, where no Chart row exists yet.
- ``require_payload_within_chart_config`` — pins every submitted column
  reference to the saved chart config, so a context-admitted viewer cannot
  name other columns of the table or probe rows via novel filters.
- ``run_chart_query`` — the single choke-point for gated warehouse-bound
  chart executions; a pass-through today, the seam for row-level policies.

Like ``gates.py``, this module raises HTTP errors but never decides
dashboard access itself — ``effective_permission`` stays the single source
of truth.
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
    """Viewer context for public token renders. Not wired to any endpoint yet —
    it shapes the ``run_chart_query`` seam so public renders can route through it."""

    org_id: int
    share_token: str = ""


ViewerContext = Union[OrgUser, PublicLinkContext]


@dataclass(frozen=True)
class ChartRenderContext:
    """Which dashboard (if any) framed this chart render; None means standalone.
    Distinct from the ``dashboard_filters`` param, which is filter values."""

    dashboard_id: Optional[int] = None


def chart_ids_in_tabs(tabs) -> Set[int]:
    """Ids of every chart tile in a raw ``tabs`` JSON structure — the one
    tabs->components->config.chartId walk. Fails closed on malformed shapes:
    junk contributes nothing instead of raising. bool is excluded explicitly
    (it subclasses int; True must not read as chart id 1)."""
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
    """Ids of every chart placed as a tile on this dashboard, across tabs."""
    return chart_ids_in_tabs(dashboard.tabs)


def _is_analyst_plus(orguser: OrgUser) -> bool:
    role = getattr(orguser, "new_role", None)
    rank = ROLE_RANK.get(getattr(role, "slug", None) if role is not None else None)
    return rank is not None and rank >= ROLE_RANK[ANALYST_ROLE]


def require_chart_view_access(
    orguser: OrgUser, chart: Chart, dashboard_id: Optional[int] = None
) -> None:
    """Raise unless ``orguser`` may view ``chart`` in this context.

    Dashboard context: 404 if the dashboard doesn't exist in the viewer's org;
    403 if the chart isn't on that dashboard or the resolver denies view on it —
    inline access never consults the chart's own levels. Standalone: the
    resolver decides on the chart itself; plain Members are denied.
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
    """Raise 403 unless ``orguser``'s role ranks Analyst or above. For the
    builder's live/unsaved-config preview, where no Chart row exists yet to
    check ownership against."""
    if not _is_analyst_plus(orguser):
        raise HttpError(403, "You do not have access to this data")


# Generic on purpose: never names the offending column, so a probing caller
# learns nothing about which columns exist (no oracle).
_PAYLOAD_MISMATCH = "Payload does not match the referenced chart"

# Saved-config keys whose string value is one warehouse column, across every
# chart_type's config shape (including legacy keys).
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
    """Every column a chart config references in a SELECT/GROUP BY role.
    Deliberately excludes the config's filter columns — a saved filter
    constrains rows without displaying its column, so it must not become
    a readable dimension."""
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
    """Filter ids named by a dashboard_filters values payload — a parsed dict
    or the raw JSON string query param. Invalid JSON / non-dict names no filters,
    mirroring the handlers' tolerant parsing."""
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
    """Raise 403 unless every column the payload references is derivable from
    ``chart``'s saved config. Dashboard-context path only — the caller must
    have already passed ``require_chart_view_access`` and the schema/table match.

    - Query columns (top-level or nested in ``extra_config``) must be a subset
      of what the saved config references, including map layers and drill-down.
    - Metric ``column_expression`` is raw SQL: allowed only verbatim from the
      saved config; ``saved_metric_id`` likewise.
    - Filter columns may also be saved filter columns or the framing dashboard's
      configured filter columns — values stay free, novel columns do not.
    - Sort columns may also be submitted metric aliases or saved sort columns.
    - ``dashboard_filters`` may only name the framing dashboard's own filters.

    Violations all raise the same generic 403 (no column echoed).
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
    """Execute a warehouse-bound chart query. A pure pass-through to ``execute``
    today (query construction lives in the API layer, which core must not
    import); future row-level checks hook here, in front of ``execute()``."""
    return execute()
