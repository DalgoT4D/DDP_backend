"""Deep-copy the template org's native viz objects onto the trial org.

Covers metrics, KPIs, charts, dashboards, dashboard filters, alerts and report snapshots —
rewriting cross-object id references (metric_id, kpi_id, chart_id, dashboard_id) that live either
as real FKs or as ids embedded in JSON blobs (`Chart.extra_config`, `Dashboard.tabs`) via old->new
maps built as each stage runs.

Insert order follows FK dependency: Metric -> KPI -> Chart -> Dashboard -> DashboardFilter ->
Alert -> ReportSnapshot. Every row's org/created_by/last_modified_by point at the trial
org/admin OrgUser (`run.trial_orguser`, set in Step 1). Uses per-row `.create()` (not
bulk_create) so the code stays simple to read at trial scale and any future save()-time signal on
these models still fires.

No @receiver/save() override exists on any of Metric/KPI/Chart/Dashboard/DashboardFilter/Alert/
ReportSnapshot (verified by grep) — plain `.create()` is safe here.
"""

import copy
import secrets

from django.utils import timezone

from ddpui.models.alert import Alert
from ddpui.models.dashboard import Dashboard, DashboardFilter
from ddpui.models.geojson import GeoJSON
from ddpui.models.metric import KPI, Metric
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.report import ReportSnapshot
from ddpui.models.visualization import Chart
from ddpui.services.dashboard_service import DashboardService
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.viz_clone")


def _preserve_ordering_timestamps(instance, template_row) -> None:
    """Copy the template row's created_at/updated_at onto the freshly-created clone.

    The trial's list pages sort by `-updated_at` (charts, dashboards, metrics, kpis, alerts) or
    `-created_at` (report snapshots). `.create()` stamps auto_now_add/auto_now at clone time, so
    every cloned row lands within the same ~second — that DESC sort then collapses to an arbitrary
    tiebreak and the template's on-screen arrangement is lost. Writing the template's real
    timestamps via a queryset `.update()` (which bypasses auto_now/auto_now_add, unlike save())
    makes the trial list order match the template exactly. Fields absent on a model are skipped.
    """
    fields = {
        name: getattr(template_row, name)
        for name in ("created_at", "updated_at")
        if hasattr(template_row, name)
    }
    if fields:
        type(instance).objects.filter(pk=instance.pk).update(**fields)


def _clone_metrics(template_org: Org, trial_org: Org, trial_orguser: OrgUser) -> dict:
    """Copy every template Metric onto the trial org. Returns {old Metric.id: new Metric}."""
    metric_map: dict = {}
    for m in Metric.objects.filter(org=template_org):
        new_m = Metric.objects.create(
            name=m.name,
            description=m.description,
            schema_name=m.schema_name,
            table_name=m.table_name,
            column=m.column,
            aggregation=m.aggregation,
            column_expression=m.column_expression,
            org=trial_org,
            created_by=trial_orguser,
            last_modified_by=trial_orguser,
        )
        _preserve_ordering_timestamps(new_m, m)
        metric_map[m.id] = new_m
    return metric_map


def _clone_kpis(
    template_org: Org, trial_org: Org, trial_orguser: OrgUser, metric_map: dict
) -> dict:
    """Copy every template KPI onto the trial org, remapping `.metric` via `metric_map`.
    Returns {old KPI.id: new KPI}."""
    kpi_map: dict = {}
    for k in KPI.objects.filter(org=template_org):
        new_k = KPI.objects.create(
            metric=metric_map[k.metric_id],
            name=k.name,
            target_value=k.target_value,
            direction=k.direction,
            green_threshold_pct=k.green_threshold_pct,
            amber_threshold_pct=k.amber_threshold_pct,
            time_grain=k.time_grain,
            time_dimension_column=k.time_dimension_column,
            metric_type_tag=k.metric_type_tag,
            program_tags=k.program_tags,
            annotations=k.annotations,
            extra_config=k.extra_config,
            display_order=k.display_order,
            org=trial_org,
            created_by=trial_orguser,
            last_modified_by=trial_orguser,
        )
        _preserve_ordering_timestamps(new_k, k)
        kpi_map[k.id] = new_k
    return kpi_map


def _remap_chart_extra_config(extra_config: dict, metric_map: dict) -> dict:
    """Deep-copy `extra_config` (never mutates the template chart's dict) and remap every
    `metrics[i].saved_metric_id` via `metric_map`.

    `selected_geojson_id` is remapped ONLY if the referenced GeoJSON is org-owned (i.e. it
    belongs to the template org itself); system-default geojsons (`org=None`) are shared across
    every org, so their id is left untouched. An org-owned geojson is NOT cloned by this phase
    (rare in practice — template geojsons are typically system defaults) — a warning is logged so
    the gap is visible rather than silently leaving the trial chart pointed at the template's row.
    """
    ec = copy.deepcopy(extra_config) or {}
    for metric_ref in ec.get("metrics") or []:
        old_id = metric_ref.get("saved_metric_id")
        if old_id in metric_map:
            metric_ref["saved_metric_id"] = metric_map[old_id].id

    geojson_id = ec.get("selected_geojson_id")
    if geojson_id is not None:
        geojson = GeoJSON.objects.filter(id=geojson_id).first()
        if geojson is not None and geojson.org_id is not None:
            logger.warning(
                f"chart references org-owned geojson {geojson_id}; not cloned by _step_viz "
                "(known limitation — trial chart keeps the template's geojson id)"
            )
    return ec


def _clone_charts(
    template_org: Org, trial_org: Org, trial_orguser: OrgUser, metric_map: dict
) -> dict:
    """Copy every template Chart onto the trial org, remapping `extra_config` saved-metric refs.
    Returns {old Chart.id: new Chart}."""
    chart_map: dict = {}
    for c in Chart.objects.filter(org=template_org):
        new_c = Chart.objects.create(
            title=c.title,
            description=c.description,
            chart_type=c.chart_type,
            computation_type=c.computation_type,
            schema_name=c.schema_name,
            table_name=c.table_name,
            extra_config=_remap_chart_extra_config(c.extra_config, metric_map),
            org=trial_org,
            created_by=trial_orguser,
            last_modified_by=trial_orguser,
        )
        _preserve_ordering_timestamps(new_c, c)
        chart_map[c.id] = new_c
    return chart_map


def _remap_dashboard_tabs(tabs: list, chart_map: dict, kpi_map: dict) -> list:
    """Deep-copy `tabs` (never mutates the template dashboard's list) and remap every
    `components[*].config.chartId`/`kpiId` via `chart_map`/`kpi_map`.

    `components` is a dict keyed by component id (confirmed against
    `ddpui/core/reports/report_service.py::_extract_chart_ids`), not a list.
    """
    new_tabs = copy.deepcopy(tabs) or []
    for tab in new_tabs:
        components = tab.get("components") or {}
        for component in components.values():
            cfg = component.get("config") or {}
            if "chartId" in cfg and cfg["chartId"] in chart_map:
                cfg["chartId"] = chart_map[cfg["chartId"]].id
            if "kpiId" in cfg and cfg["kpiId"] in kpi_map:
                cfg["kpiId"] = kpi_map[cfg["kpiId"]].id
    return new_tabs


def _clone_dashboards(
    template_org: Org, trial_org: Org, trial_orguser: OrgUser, chart_map: dict, kpi_map: dict
) -> dict:
    """Copy every template Dashboard onto the trial org, remapping `tabs` chart/kpi refs.

    Preserves the template's private/public state AND its org-default flag so the trial looks like
    the template:
    - A PUBLIC template dashboard clones as public too, but with a FRESH unique share token —
      `public_share_token` is `unique=True` and the public view does
      `Dashboard.objects.get(token, is_public=True)`, so reusing the template's token would both
      violate the unique constraint and make that lookup ambiguous. A private one clones private
      (no token). Public-access analytics (count / last-accessed) start clean.
    - `is_org_default` is copied (scoped per-org; template has at most one True), so the Impact
      page shows the same landing dashboard.
    Returns {old Dashboard.id: new Dashboard}."""
    dash_map: dict = {}
    for d in Dashboard.objects.filter(org=template_org):
        is_public = d.is_public
        new_d = Dashboard.objects.create(
            title=d.title,
            description=d.description,
            dashboard_type=d.dashboard_type,
            grid_columns=d.grid_columns,
            target_screen_size=d.target_screen_size,
            tabs=_remap_dashboard_tabs(d.tabs, chart_map, kpi_map),
            filter_layout=d.filter_layout,
            is_published=d.is_published,
            is_public=is_public,
            public_share_token=(secrets.token_urlsafe(48) if is_public else None),
            public_shared_at=(timezone.now() if is_public else None),
            is_org_default=d.is_org_default,
            org=trial_org,
            created_by=trial_orguser,
            last_modified_by=trial_orguser,
        )
        _preserve_ordering_timestamps(new_d, d)
        dash_map[d.id] = new_d
    return dash_map


def _clone_dashboard_filters(template_org: Org, dash_map: dict) -> int:
    """Copy every DashboardFilter belonging to a template Dashboard, remapping `.dashboard` via
    `dash_map`, then rewrite each cloned dashboard's `tabs` so filter references
    (`filter-<id>` component keys, `layout_config[].i`, `config.filterId`) point at the NEW
    filter ids — via the same `DashboardService.copy_tabs_with_filter_remapping` the
    duplicate-dashboard flow uses. Without the rewrite the cloned tabs keep the TEMPLATE org's
    filter ids and every dashboard filter is broken on the trial.

    The rewritten tabs are written with a queryset `.update()` (not `.save()`) so the
    template timestamps applied by `_preserve_ordering_timestamps` survive (save() would
    re-stamp auto_now). Returns the number of filters copied."""
    count = 0
    # old dashboard id -> {old filter id (str) -> new filter id (str)}
    filter_maps: dict = {}
    for f in DashboardFilter.objects.filter(dashboard__org=template_org):
        new_f = DashboardFilter.objects.create(
            dashboard=dash_map[f.dashboard_id],
            name=f.name,
            filter_type=f.filter_type,
            schema_name=f.schema_name,
            table_name=f.table_name,
            column_name=f.column_name,
            settings=f.settings,
            order=f.order,
        )
        filter_maps.setdefault(f.dashboard_id, {})[str(f.id)] = str(new_f.id)
        count += 1

    for old_dash_id, filter_id_mapping in filter_maps.items():
        new_d = dash_map[old_dash_id]
        new_tabs = DashboardService.copy_tabs_with_filter_remapping(
            new_d.tabs or [], filter_id_mapping
        )
        Dashboard.objects.filter(pk=new_d.pk).update(tabs=new_tabs)
        new_d.tabs = new_tabs  # keep the in-memory instance consistent
    return count


def _clone_alerts(
    template_org: Org,
    trial_org: Org,
    trial_orguser: OrgUser,
    metric_map: dict,
    kpi_map: dict,
) -> int:
    """Copy every template Alert onto the trial org, remapping `metric`/`kpi` and resetting
    delivery/evaluation state — a clone must not fire alerts at the template's Slack webhook or
    external recipients, and has no evaluation history yet. Returns the number of alerts copied."""
    count = 0
    for a in Alert.objects.filter(org=template_org):
        new_a = Alert.objects.create(
            org=trial_org,
            name=a.name,
            alert_type=a.alert_type,
            # fail-loud indexing (like the KPI/Chart remaps): a metric_id/kpi_id that is set but
            # absent from the map means a broken clone — surface it instead of silently producing
            # a sourceless alert.
            metric=metric_map[a.metric_id] if a.metric_id else None,
            kpi=kpi_map[a.kpi_id] if a.kpi_id else None,
            standalone_config=a.standalone_config,
            condition=a.condition,
            schedule_cron=a.schedule_cron,
            delivery_channels=["email"],
            slack_webhook_url=None,
            message_template=a.message_template,
            recipients=[{"type": "orguser", "orguser_id": trial_orguser.id}],
            is_active=a.is_active,
            last_evaluated_at=None,
            created_by=trial_orguser,
            last_modified_by=trial_orguser,
        )
        _preserve_ordering_timestamps(new_a, a)
        count += 1
    return count


def _clone_report_snapshots(template_org: Org, trial_org: Org, trial_orguser: OrgUser) -> int:
    """Copy every template ReportSnapshot onto the trial org, resetting public sharing. The
    frozen JSON blobs (`frozen_dashboard`/`frozen_chart_configs`) are historical — copied as-is,
    ids inside them are never remapped (ReportSnapshot has no FK to Dashboard/Chart; ids embedded
    there describe what the template looked like at freeze time, not the trial clone). Returns
    the number of snapshots copied."""
    count = 0
    for r in ReportSnapshot.objects.filter(org=template_org):
        new_r = ReportSnapshot.objects.create(
            title=r.title,
            date_column=r.date_column,
            period_start=r.period_start,
            period_end=r.period_end,
            frozen_dashboard=r.frozen_dashboard,
            frozen_chart_configs=r.frozen_chart_configs,
            summary=r.summary,
            is_public=False,
            public_share_token=None,
            org=trial_org,
            created_by=trial_orguser,
            last_modified_by=trial_orguser,
        )
        _preserve_ordering_timestamps(new_r, r)
        count += 1
    return count


def clone_viz(template_org: Org, trial_org: Org, trial_orguser: OrgUser) -> dict:
    """Clone every native viz object from `template_org` onto `trial_org`, attributed to
    `trial_orguser`. Returns a manifest dict of counts per model, suitable for `run.manifest["viz"]`.
    """
    metric_map = _clone_metrics(template_org, trial_org, trial_orguser)
    kpi_map = _clone_kpis(template_org, trial_org, trial_orguser, metric_map)
    chart_map = _clone_charts(template_org, trial_org, trial_orguser, metric_map)
    dash_map = _clone_dashboards(template_org, trial_org, trial_orguser, chart_map, kpi_map)
    filter_count = _clone_dashboard_filters(template_org, dash_map)
    alert_count = _clone_alerts(template_org, trial_org, trial_orguser, metric_map, kpi_map)
    report_count = _clone_report_snapshots(template_org, trial_org, trial_orguser)

    manifest = {
        "metrics": len(metric_map),
        "kpis": len(kpi_map),
        "charts": len(chart_map),
        "dashboards": len(dash_map),
        "dashboard_filters": filter_count,
        "alerts": alert_count,
        "report_snapshots": report_count,
    }
    logger.info(f"cloned viz from org={template_org.id} to org={trial_org.id}: {manifest}")
    return manifest
