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

from ddpui.core.alerts.alert_service import AlertService
from ddpui.core.kpi.kpi_service import KPIService
from ddpui.core.metric.metric_service import MetricService
from ddpui.models.alert import Alert
from ddpui.models.dashboard import Dashboard, DashboardFilter
from ddpui.models.geojson import GeoJSON
from ddpui.models.metric import KPI, Metric
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.report import ReportSnapshot
from ddpui.models.visualization import Chart
from ddpui.schemas.alert_schema import AlertCreate
from ddpui.schemas.kpi_schema import KPICreate, KPIExtraConfig
from ddpui.services.chart_service import ChartData, ChartService
from ddpui.services.dashboard_service import DashboardService, FilterData
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.viz_clone")


def _preserve_ordering_timestamps(instance, template_row, skip: tuple = ()) -> None:
    """Copy the template row's created_at/updated_at onto the freshly-created clone.

    The trial's list pages sort by `-updated_at` (charts, dashboards, metrics, kpis, alerts) or
    `-created_at` (report snapshots). `.create()` stamps auto_now_add/auto_now at clone time, so
    every cloned row lands within the same ~second — that DESC sort then collapses to an arbitrary
    tiebreak and the template's on-screen arrangement is lost. Writing the template's real
    timestamps via a queryset `.update()` (which bypasses auto_now/auto_now_add, unlike save())
    makes the trial list order match the template exactly. Fields absent on a model are skipped.

    `skip` drops named fields from that copy, for models where a backdated timestamp is not
    inert — see `_clone_alerts` and `scheduling.is_due`.
    """
    fields = {
        name: getattr(template_row, name)
        for name in ("created_at", "updated_at")
        if hasattr(template_row, name) and name not in skip
    }
    if fields:
        type(instance).objects.filter(pk=instance.pk).update(**fields)


def _clone_metrics(  # pylint: disable=unused-argument
    template_org: Org, trial_org: Org, trial_orguser: OrgUser
) -> dict:
    """Copy every template Metric onto the trial org via `MetricService.create_metric` — the
    same creation path the normal UI-driven "create a metric" flow uses, so a cloned metric
    gets the same org-scoped name-uniqueness check and live warehouse-query validation a
    user-created one would. The trial warehouse already holds a full data copy of the
    template's (Step 2's server-side `CREATE DATABASE ... TEMPLATE` copy), so the validation
    query resolves against real, matching tables rather than an empty scaffold.

    Returns {old Metric.id: new Metric}. `trial_org` is unused directly (the service derives
    org from `trial_orguser.org`, which is always this same org) — kept for signature/test
    stability.
    """
    metric_map: dict = {}
    for m in Metric.objects.filter(org=template_org):
        new_m = MetricService.create_metric(
            name=m.name,
            description=m.description,
            schema_name=m.schema_name,
            table_name=m.table_name,
            column=m.column,
            aggregation=m.aggregation,
            column_expression=m.column_expression,
            orguser=trial_orguser,
        )
        _preserve_ordering_timestamps(new_m, m)
        metric_map[m.id] = new_m
    return metric_map


def _clone_kpis(  # pylint: disable=unused-argument
    template_org: Org, trial_org: Org, trial_orguser: OrgUser, metric_map: dict
) -> dict:
    """Copy every template KPI onto the trial org via `KPIService.create_kpi` — the same
    creation path the normal UI-driven "create a KPI" flow uses (field validation +
    metric-FK org-scoping check), remapping `.metric` via `metric_map`.

    `KPICreate` has no `annotations`/`display_order` fields (they're update-only on the
    normal KPI API), so `create_kpi` can't carry them — patched on afterward via a direct
    `.update()`, same pattern as `_preserve_ordering_timestamps` below.

    Returns {old KPI.id: new KPI}. `trial_org` is unused directly (the service derives org
    from `trial_orguser.org`) — kept for signature/test stability.
    """
    kpi_map: dict = {}
    for k in KPI.objects.filter(org=template_org):
        kpi_payload = KPICreate(
            metric_id=metric_map[k.metric_id].id,
            name=k.name,
            target_value=k.target_value,
            direction=k.direction,
            green_threshold_pct=k.green_threshold_pct,
            amber_threshold_pct=k.amber_threshold_pct,
            time_grain=k.time_grain,
            time_dimension_column=k.time_dimension_column,
            metric_type_tag=k.metric_type_tag,
            program_tags=k.program_tags,
            extra_config=KPIExtraConfig(**(k.extra_config or {})),
        )
        new_k = KPIService.create_kpi(kpi_payload, trial_orguser)
        KPI.objects.filter(pk=new_k.pk).update(
            annotations=k.annotations, display_order=k.display_order
        )
        new_k.annotations = k.annotations
        new_k.display_order = k.display_order
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


def _clone_charts(  # pylint: disable=unused-argument
    template_org: Org, trial_org: Org, trial_orguser: OrgUser, metric_map: dict
) -> dict:
    """Copy every template Chart onto the trial org via `ChartService.create_chart` — the same
    creation path the normal UI-driven "create a chart" flow uses — remapping `extra_config`
    saved-metric refs. `ChartData` has no `computation_type` field: the `Chart` model docstring
    marks it deprecated/"no longer used in chart logic", so it's left at its model default on
    the clone rather than threading a dead field through the service layer.

    Returns {old Chart.id: new Chart}. `trial_org` is unused directly (the service derives org
    from `trial_orguser.org`) — kept for signature/test stability.
    """
    chart_map: dict = {}
    for c in Chart.objects.filter(org=template_org):
        chart_data = ChartData(
            title=c.title,
            description=c.description,
            chart_type=c.chart_type,
            schema_name=c.schema_name,
            table_name=c.table_name,
            extra_config=_remap_chart_extra_config(c.extra_config, metric_map),
        )
        new_c = ChartService.create_chart(chart_data, trial_orguser)
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

    Deliberately NOT routed through `DashboardService.create_dashboard`: that service always
    forces a single fresh default tab and has no fields for `is_public`/`public_share_token`/
    `public_shared_at`/`is_org_default`/`is_published`/`dashboard_type`/`target_screen_size` —
    using it would mean generating a throwaway tab only to immediately overwrite it plus every
    other field via a follow-up `.update()`, for no validation gained (the service does none).

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
        new_dashboard = dash_map[f.dashboard_id]
        filter_data = FilterData(
            filter_type=f.filter_type,
            schema_name=f.schema_name,
            table_name=f.table_name,
            column_name=f.column_name,
            name=f.name,
            settings=f.settings,
            order=f.order,
        )
        # org comes from the already-cloned trial Dashboard instance, not a separate param —
        # DashboardService.create_filter re-fetches the dashboard scoped to this org.
        new_f = DashboardService.create_filter(new_dashboard.id, new_dashboard.org, filter_data)
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


def _clone_alerts(  # pylint: disable=unused-argument
    template_org: Org,
    trial_org: Org,
    trial_orguser: OrgUser,
    metric_map: dict,
    kpi_map: dict,
) -> int:
    """Copy every template Alert onto the trial org via `AlertService.create_alert` — the same
    creation path the normal UI-driven "create an alert" flow uses (name uniqueness, cron
    validation, condition-shape validation, delivery-channel/recipient validation, metric/kpi
    FK org-scoping) — remapping `metric`/`kpi` and resetting delivery/evaluation state: a clone
    must not fire alerts at the template's Slack webhook or external recipients, and has no
    evaluation history yet.

    `AlertCreate`/`create_alert` hard-codes `is_active=True` on creation (no way to pass a
    different initial value through the schema) — patched to the template's real value
    afterward via a direct `.update()`, same as `_preserve_ordering_timestamps` below.

    Returns the number of alerts copied."""
    count = 0
    for a in Alert.objects.filter(org=template_org):
        # fail-loud indexing (like the KPI/Chart remaps): a metric_id/kpi_id that is set but
        # absent from the map means a broken clone — surface it instead of silently producing
        # a sourceless alert.
        alert_payload = AlertCreate(
            name=a.name,
            alert_type=a.alert_type,
            metric_id=metric_map[a.metric_id].id if a.metric_id else None,
            kpi_id=kpi_map[a.kpi_id].id if a.kpi_id else None,
            standalone_config=a.standalone_config,
            condition=a.condition,
            schedule_cron=a.schedule_cron,
            delivery_channels=["email"],
            slack_webhook_url=None,
            message_template=a.message_template,
            recipients=[{"type": "orguser", "orguser_id": trial_orguser.id}],
        )
        new_a = AlertService.create_alert(alert_payload, trial_orguser)
        if new_a.is_active != a.is_active:
            Alert.objects.filter(pk=new_a.pk).update(is_active=a.is_active)
            new_a.is_active = a.is_active

        # Claim the tick that has already passed today. `scheduling.is_due` treats an alert as
        # due when its most recent cron tick is later than `last_evaluated_at or created_at` —
        # a clone starts with last_evaluated_at NULL, so without this stamp the trial's alerts
        # all fire on the dispatcher's next 60s pass (a "0 9 * * *" alert cloned at 2pm emails
        # the user immediately). Stamping now means the first email lands at the alert's next
        # real scheduled tick.
        claimed_at = timezone.now()
        Alert.objects.filter(pk=new_a.pk).update(last_evaluated_at=claimed_at)
        new_a.last_evaluated_at = claimed_at

        # created_at is excluded: for every other model a backdated created_at only affects list
        # ordering, but on Alert it is the is_due() floor that keeps a fresh alert from firing —
        # copying the template's (months old) created_at would defeat the stamp above the moment
        # last_evaluated_at is cleared or an evaluation resets it.
        _preserve_ordering_timestamps(new_a, a, skip=("created_at",))
        count += 1
    return count


def _remap_frozen_chart_configs(frozen_chart_configs: dict, metric_map: dict) -> dict:
    """Deep-copy a snapshot's `frozen_chart_configs` and remap each CHART entry's
    `extra_config` saved-metric refs via `metric_map`.

    The frozen blobs are mostly historical, BUT `saved_metric_id` inside a frozen chart's
    `extra_config` is resolved LIVE at every render: `get_report_chart_data` feeds the frozen
    config through `build_chart_data_payload` → `_resolve_saved_metrics`, which does
    `Metric.objects.get(id=...)` with no org filter — so an un-remapped id makes every render of
    the trial snapshot read (and depend on) the TEMPLATE org's live Metric row: template edits
    silently change trial reports, template deletion silently drops the series. Remapping via
    `metric_map` points the frozen config at the trial's own cloned Metric instead.

    Chart entries are the values carrying an `extra_config` key; frozen KPI entries capture
    computed values with no live refs and are copied untouched. `_remap_chart_extra_config`
    (shared with `_clone_charts`) does the per-entry work, including the org-owned-geojson
    warning."""
    remapped: dict = {}
    for key, entry in (frozen_chart_configs or {}).items():
        if isinstance(entry, dict) and "extra_config" in entry:
            entry = dict(entry)
            entry["extra_config"] = _remap_chart_extra_config(entry["extra_config"], metric_map)
        else:
            entry = copy.deepcopy(entry)
        remapped[key] = entry
    return remapped


def _clone_report_snapshots(
    template_org: Org, trial_org: Org, trial_orguser: OrgUser, metric_map: dict
) -> int:
    """Copy every template ReportSnapshot onto the trial org, resetting public sharing. The
    frozen JSON blobs are copied verbatim EXCEPT the live-resolved saved-metric refs inside
    `frozen_chart_configs`, which are remapped onto the trial's cloned Metrics — see
    `_remap_frozen_chart_configs`. Returns the number of snapshots copied.

    Deliberately NOT routed through `ReportService.create_snapshot`: that function *freezes
    the current live state of an existing Dashboard* (re-resolving date columns against the
    warehouse and recomputing `frozen_dashboard`/`frozen_chart_configs` from scratch) — it is
    not a "copy these exact stored field values" operation, so using it here would silently
    replace the template snapshot's real historical content with a fresh freeze of whatever the
    (already-cloned) trial dashboard looks like right now. No general-purpose creation service
    exists for a verbatim snapshot copy, so this stays a direct `.create()`."""
    count = 0
    for r in ReportSnapshot.objects.filter(org=template_org):
        new_r = ReportSnapshot.objects.create(
            title=r.title,
            date_column=r.date_column,
            period_start=r.period_start,
            period_end=r.period_end,
            frozen_dashboard=r.frozen_dashboard,
            frozen_chart_configs=_remap_frozen_chart_configs(r.frozen_chart_configs, metric_map),
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
    report_count = _clone_report_snapshots(template_org, trial_org, trial_orguser, metric_map)

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
