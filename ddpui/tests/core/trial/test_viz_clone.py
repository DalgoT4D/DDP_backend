import pytest
from django.contrib.auth.models import User

from ddpui.core.trial import viz_clone
from ddpui.models.alert import Alert
from ddpui.models.dashboard import Dashboard, DashboardFilter
from ddpui.models.geojson import GeoJSON
from ddpui.models.metric import KPI, Metric
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.report import ReportSnapshot
from ddpui.models.visualization import Chart

pytestmark = pytest.mark.django_db


def _make_org(slug: str) -> Org:
    return Org.objects.create(name=slug, slug=slug)


def _make_orguser(org: Org, email: str) -> OrgUser:
    user = User.objects.create(username=email, email=email)
    return OrgUser.objects.create(user=user, org=org, email_verified=False)


# ---------------------------------------------------------------------------
# Metric
# ---------------------------------------------------------------------------


def test_clone_metrics_remaps_org_and_created_by():
    template_org = _make_org("tmpl-metric")
    trial_org = _make_org("trial-metric")
    template_user = _make_orguser(template_org, "tmpl-metric@x.org")
    trial_user = _make_orguser(trial_org, "trial-metric@x.org")

    m = Metric.objects.create(
        name="Total beneficiaries",
        description="count of beneficiaries",
        schema_name="analytics",
        table_name="beneficiaries",
        column="id",
        aggregation="count",
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )

    metric_map = viz_clone._clone_metrics(template_org, trial_org, trial_user)

    assert set(metric_map.keys()) == {m.id}
    new_m = metric_map[m.id]
    assert new_m.id != m.id
    assert new_m.org_id == trial_org.id
    assert new_m.created_by_id == trial_user.id
    assert new_m.last_modified_by_id == trial_user.id
    assert new_m.name == m.name
    assert new_m.schema_name == "analytics"
    assert new_m.table_name == "beneficiaries"
    assert new_m.column == "id"
    assert new_m.aggregation == "count"
    assert Metric.objects.filter(org=trial_org).count() == 1
    # template row is untouched
    m.refresh_from_db()
    assert m.org_id == template_org.id


# ---------------------------------------------------------------------------
# KPI
# ---------------------------------------------------------------------------


def test_clone_kpis_remaps_metric_fk():
    template_org = _make_org("tmpl-kpi")
    trial_org = _make_org("trial-kpi")
    template_user = _make_orguser(template_org, "tmpl-kpi@x.org")
    trial_user = _make_orguser(trial_org, "trial-kpi@x.org")

    m = Metric.objects.create(
        name="Metric1",
        schema_name="analytics",
        table_name="t",
        column="id",
        aggregation="count",
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )
    k = KPI.objects.create(
        metric=m,
        name="KPI1",
        target_value=100.0,
        direction="increase",
        time_grain="monthly",
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )

    metric_map = viz_clone._clone_metrics(template_org, trial_org, trial_user)
    kpi_map = viz_clone._clone_kpis(template_org, trial_org, trial_user, metric_map)

    assert set(kpi_map.keys()) == {k.id}
    new_k = kpi_map[k.id]
    assert new_k.metric_id == metric_map[m.id].id
    assert new_k.org_id == trial_org.id
    assert new_k.created_by_id == trial_user.id
    assert new_k.name == "KPI1"
    assert new_k.target_value == 100.0


# ---------------------------------------------------------------------------
# Chart
# ---------------------------------------------------------------------------


def test_clone_charts_remaps_saved_metric_id_and_does_not_mutate_template():
    template_org = _make_org("tmpl-chart")
    trial_org = _make_org("trial-chart")
    template_user = _make_orguser(template_org, "tmpl-chart@x.org")
    trial_user = _make_orguser(trial_org, "trial-chart@x.org")

    m = Metric.objects.create(
        name="Metric1",
        schema_name="analytics",
        table_name="t",
        column="id",
        aggregation="count",
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )
    original_extra_config = {"metrics": [{"saved_metric_id": m.id, "label": "M1"}]}
    c = Chart.objects.create(
        title="Chart1",
        chart_type="bar",
        schema_name="analytics",
        table_name="t",
        extra_config=original_extra_config,
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )

    metric_map = viz_clone._clone_metrics(template_org, trial_org, trial_user)
    chart_map = viz_clone._clone_charts(template_org, trial_org, trial_user, metric_map)

    new_c = chart_map[c.id]
    assert new_c.org_id == trial_org.id
    assert new_c.extra_config["metrics"][0]["saved_metric_id"] == metric_map[m.id].id

    # template chart's extra_config must be untouched (deepcopy, not mutate-in-place)
    c.refresh_from_db()
    assert c.extra_config["metrics"][0]["saved_metric_id"] == m.id
    assert c.extra_config == original_extra_config


def test_clone_charts_keeps_system_default_geojson_id_unchanged():
    template_org = _make_org("tmpl-chart-geo")
    trial_org = _make_org("trial-chart-geo")
    template_user = _make_orguser(template_org, "tmpl-chart-geo@x.org")
    trial_user = _make_orguser(trial_org, "trial-chart-geo@x.org")

    from ddpui.models.georegion import GeoRegion

    region = GeoRegion.objects.create(
        name="India",
        type="country",
        country_code="IND",
        region_code="IN",
        display_name="India",
    )
    system_geo = GeoJSON.objects.create(
        region=region,
        geojson_data={"type": "FeatureCollection", "features": []},
        properties_key="name",
        is_default=True,
        org=None,
        name="india-default",
    )
    c = Chart.objects.create(
        title="MapChart",
        chart_type="map",
        schema_name="analytics",
        table_name="t",
        extra_config={"selected_geojson_id": system_geo.id},
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )

    chart_map = viz_clone._clone_charts(template_org, trial_org, trial_user, {})

    new_c = chart_map[c.id]
    assert new_c.extra_config["selected_geojson_id"] == system_geo.id


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------


def test_clone_dashboards_remaps_ids_preserves_public_state_with_fresh_token_and_org_default():
    template_org = _make_org("tmpl-dash")
    trial_org = _make_org("trial-dash")
    template_user = _make_orguser(template_org, "tmpl-dash@x.org")
    trial_user = _make_orguser(trial_org, "trial-dash@x.org")

    m = Metric.objects.create(
        name="Metric1",
        schema_name="analytics",
        table_name="t",
        column="id",
        aggregation="count",
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )
    k = KPI.objects.create(
        metric=m,
        name="KPI1",
        direction="increase",
        time_grain="monthly",
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )
    c = Chart.objects.create(
        title="Chart1",
        chart_type="bar",
        schema_name="analytics",
        table_name="t",
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )
    original_tabs = [
        {
            "id": "tab1",
            "components": {
                "comp1": {"type": "chart", "config": {"chartId": c.id}},
                "comp2": {"type": "kpi", "config": {"kpiId": k.id}},
            },
        }
    ]
    d = Dashboard.objects.create(
        title="Dash1",
        tabs=original_tabs,
        is_published=True,
        is_public=True,
        public_share_token="abc123",
        is_org_default=True,
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )

    metric_map = viz_clone._clone_metrics(template_org, trial_org, trial_user)
    kpi_map = viz_clone._clone_kpis(template_org, trial_org, trial_user, metric_map)
    chart_map = viz_clone._clone_charts(template_org, trial_org, trial_user, metric_map)
    dash_map = viz_clone._clone_dashboards(template_org, trial_org, trial_user, chart_map, kpi_map)

    new_d = dash_map[d.id]
    new_chart_id = new_d.tabs[0]["components"]["comp1"]["config"]["chartId"]
    new_kpi_id = new_d.tabs[0]["components"]["comp2"]["config"]["kpiId"]
    assert new_chart_id == chart_map[c.id].id
    assert new_kpi_id == kpi_map[k.id].id
    # public state PRESERVED, but with a fresh unique token (never the template's) so the unique
    # constraint holds and the public-view .get(token, is_public=True) stays unambiguous
    assert new_d.is_public is True
    assert new_d.public_share_token is not None
    assert new_d.public_share_token != "abc123"
    assert new_d.public_shared_at is not None
    assert new_d.public_access_count == 0  # analytics start clean
    assert new_d.is_org_default is True  # org-default PRESERVED so the Impact page shows it
    assert new_d.is_published is True  # publish state preserved (drives the "Published" badge)

    # template dashboard untouched
    d.refresh_from_db()
    assert d.tabs[0]["components"]["comp1"]["config"]["chartId"] == c.id
    assert d.is_public is True
    assert d.public_share_token == "abc123"
    assert d.is_org_default is True


# ---------------------------------------------------------------------------
# DashboardFilter
# ---------------------------------------------------------------------------


def test_clone_dashboard_filters_remaps_dashboard_fk():
    template_org = _make_org("tmpl-filt")
    trial_org = _make_org("trial-filt")
    template_user = _make_orguser(template_org, "tmpl-filt@x.org")
    trial_user = _make_orguser(trial_org, "trial-filt@x.org")

    d = Dashboard.objects.create(
        title="Dash1",
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )
    f = DashboardFilter.objects.create(
        dashboard=d,
        name="Region filter",
        filter_type="value",
        schema_name="analytics",
        table_name="t",
        column_name="region",
    )

    dash_map = viz_clone._clone_dashboards(template_org, trial_org, trial_user, {}, {})
    count = viz_clone._clone_dashboard_filters(template_org, dash_map)

    assert count == 1
    new_filter = DashboardFilter.objects.get(dashboard=dash_map[d.id])
    assert new_filter.name == "Region filter"
    assert new_filter.column_name == "region"


def test_clone_dashboard_filters_remaps_filter_ids_inside_tabs():
    """Cloned DashboardFilter rows get NEW ids — the cloned dashboard's tabs must reference
    those new ids (component keys `filter-<id>`, `layout_config[].i`, `config.filterId`), not
    the template org's filter ids."""
    template_org = _make_org("tmpl-filtremap")
    trial_org = _make_org("trial-filtremap")
    template_user = _make_orguser(template_org, "tmpl-filtremap@x.org")
    trial_user = _make_orguser(trial_org, "trial-filtremap@x.org")

    d = Dashboard.objects.create(
        title="Dash1",
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )
    f = DashboardFilter.objects.create(
        dashboard=d,
        name="Region filter",
        filter_type="value",
        schema_name="analytics",
        table_name="t",
        column_name="region",
    )
    d.tabs = [
        {
            "id": "tab1",
            "layout_config": [{"i": f"filter-{f.id}", "x": 0, "y": 0}],
            "components": {
                f"filter-{f.id}": {"type": "filter", "config": {"filterId": f.id}},
            },
        }
    ]
    d.save()

    dash_map = viz_clone._clone_dashboards(template_org, trial_org, trial_user, {}, {})
    viz_clone._clone_dashboard_filters(template_org, dash_map)

    new_d = Dashboard.objects.get(pk=dash_map[d.id].pk)
    new_filter = DashboardFilter.objects.get(dashboard=new_d)
    assert new_filter.id != f.id

    tab = new_d.tabs[0]
    # layout_config remapped
    assert tab["layout_config"][0]["i"] == f"filter-{new_filter.id}"
    # component key + config.filterId remapped; old key gone
    assert f"filter-{f.id}" not in tab["components"]
    assert tab["components"][f"filter-{new_filter.id}"]["config"]["filterId"] == new_filter.id

    # template dashboard untouched
    d.refresh_from_db()
    assert d.tabs[0]["components"][f"filter-{f.id}"]["config"]["filterId"] == f.id


# ---------------------------------------------------------------------------
# Alert
# ---------------------------------------------------------------------------


def test_clone_alerts_remaps_metric_kpi_and_resets_delivery_state():
    template_org = _make_org("tmpl-alert")
    trial_org = _make_org("trial-alert")
    template_user = _make_orguser(template_org, "tmpl-alert@x.org")
    trial_user = _make_orguser(trial_org, "trial-alert@x.org")

    m = Metric.objects.create(
        name="Metric1",
        schema_name="analytics",
        table_name="t",
        column="id",
        aggregation="count",
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )
    a = Alert.objects.create(
        org=template_org,
        name="Alert1",
        alert_type="metric_threshold",
        metric=m,
        condition={"operator": "lt", "value": 10},
        schedule_cron="0 9 * * *",
        delivery_channels=["email", "slack"],
        slack_webhook_url="https://hooks.slack.com/tmpl",
        message_template="{{value}}",
        recipients=[{"type": "external", "email": "tmpl-owner@x.org"}],
        last_evaluated_at="2026-01-01T00:00:00Z",
        created_by=template_user,
        last_modified_by=template_user,
    )

    metric_map = viz_clone._clone_metrics(template_org, trial_org, trial_user)
    count = viz_clone._clone_alerts(template_org, trial_org, trial_user, metric_map, {})

    assert count == 1
    new_alert = Alert.objects.get(org=trial_org, name="Alert1")
    assert new_alert.metric_id == metric_map[m.id].id
    assert new_alert.kpi_id is None
    assert new_alert.recipients == [{"type": "orguser", "orguser_id": trial_user.id}]
    assert new_alert.delivery_channels == ["email"]
    assert new_alert.slack_webhook_url is None
    assert new_alert.last_evaluated_at is None


# ---------------------------------------------------------------------------
# ReportSnapshot
# ---------------------------------------------------------------------------


def test_clone_report_snapshots_remaps_org_and_resets_public_share():
    template_org = _make_org("tmpl-report")
    trial_org = _make_org("trial-report")
    template_user = _make_orguser(template_org, "tmpl-report@x.org")
    trial_user = _make_orguser(trial_org, "trial-report@x.org")

    r = ReportSnapshot.objects.create(
        title="Q1 report",
        frozen_dashboard={"title": "Dash1", "tabs": []},
        frozen_chart_configs={"1": {"title": "Chart1"}},
        is_public=True,
        public_share_token="report-token",
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )

    count = viz_clone._clone_report_snapshots(template_org, trial_org, trial_user)

    assert count == 1
    new_r = ReportSnapshot.objects.get(org=trial_org, title="Q1 report")
    assert new_r.is_public is False
    assert new_r.public_share_token is None
    assert new_r.frozen_dashboard == {"title": "Dash1", "tabs": []}
    assert new_r.frozen_chart_configs == {"1": {"title": "Chart1"}}
    assert new_r.created_by_id == trial_user.id


# ---------------------------------------------------------------------------
# List ordering preservation
# ---------------------------------------------------------------------------


def test_clone_preserves_list_order_by_copying_timestamps():
    """List pages sort by -updated_at; the clone must copy the template's timestamps so the trial
    shows the SAME order. Without the copy every clone gets ~equal clone-time stamps and the order
    is lost. Uses 3 charts with distinct, deliberately non-insertion-order timestamps."""
    from datetime import datetime, timezone

    template_org = _make_org("tmpl-order")
    trial_org = _make_org("trial-order")
    template_user = _make_orguser(template_org, "tmpl-order@x.org")
    trial_user = _make_orguser(trial_org, "trial-order@x.org")

    # Insertion order (Alpha, Bravo, Charlie) is intentionally DIFFERENT from the desired display
    # order — Bravo is newest, then Alpha, then Charlie — so a passing test can only come from the
    # copied timestamps, not from incidental pk/insertion order.
    stamps = {
        "Alpha": datetime(2026, 2, 1, tzinfo=timezone.utc),
        "Bravo": datetime(2026, 3, 1, tzinfo=timezone.utc),
        "Charlie": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    for title in ("Alpha", "Bravo", "Charlie"):
        c = Chart.objects.create(
            title=title,
            chart_type="bar",
            schema_name="analytics",
            table_name="t",
            org=template_org,
            created_by=template_user,
            last_modified_by=template_user,
        )
        Chart.objects.filter(pk=c.pk).update(created_at=stamps[title], updated_at=stamps[title])

    template_order = list(
        Chart.objects.filter(org=template_org)
        .order_by("-updated_at")
        .values_list("title", flat=True)
    )
    assert template_order == ["Bravo", "Alpha", "Charlie"]  # sanity: not insertion order

    viz_clone._clone_charts(template_org, trial_org, trial_user, {})

    trial_order = list(
        Chart.objects.filter(org=trial_org).order_by("-updated_at").values_list("title", flat=True)
    )
    assert trial_order == template_order  # arrangement preserved

    for title in ("Alpha", "Bravo", "Charlie"):
        trial_chart = Chart.objects.get(org=trial_org, title=title)
        assert trial_chart.updated_at == stamps[title]  # real template stamp, not clone-time
        assert trial_chart.created_at == stamps[title]


# ---------------------------------------------------------------------------
# Full integration
# ---------------------------------------------------------------------------


def test_clone_viz_builds_full_graph_and_returns_manifest_counts():
    template_org = _make_org("tmpl-full")
    trial_org = _make_org("trial-full")
    template_user = _make_orguser(template_org, "tmpl-full@x.org")
    trial_user = _make_orguser(trial_org, "trial-full@x.org")

    m = Metric.objects.create(
        name="Metric1",
        schema_name="analytics",
        table_name="t",
        column="id",
        aggregation="count",
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )
    k = KPI.objects.create(
        metric=m,
        name="KPI1",
        direction="increase",
        time_grain="monthly",
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )
    c = Chart.objects.create(
        title="Chart1",
        chart_type="bar",
        schema_name="analytics",
        table_name="t",
        extra_config={"metrics": [{"saved_metric_id": m.id}]},
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )
    d = Dashboard.objects.create(
        title="Dash1",
        tabs=[
            {
                "id": "tab1",
                "components": {
                    "comp1": {"type": "chart", "config": {"chartId": c.id}},
                    "comp2": {"type": "kpi", "config": {"kpiId": k.id}},
                },
            }
        ],
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )
    DashboardFilter.objects.create(
        dashboard=d,
        name="Region",
        filter_type="value",
        schema_name="analytics",
        table_name="t",
        column_name="region",
    )
    Alert.objects.create(
        org=template_org,
        name="Alert1",
        alert_type="kpi_rag",
        kpi=k,
        condition={"rag_states": ["red"]},
        schedule_cron="0 9 * * *",
        message_template="{{value}}",
        created_by=template_user,
        last_modified_by=template_user,
    )
    ReportSnapshot.objects.create(
        title="Q1 report",
        org=template_org,
        created_by=template_user,
        last_modified_by=template_user,
    )

    manifest = viz_clone.clone_viz(template_org, trial_org, trial_user)

    assert manifest == {
        "metrics": 1,
        "kpis": 1,
        "charts": 1,
        "dashboards": 1,
        "dashboard_filters": 1,
        "alerts": 1,
        "report_snapshots": 1,
    }
    assert Metric.objects.filter(org=trial_org).count() == 1
    assert KPI.objects.filter(org=trial_org).count() == 1
    assert Chart.objects.filter(org=trial_org).count() == 1
    assert Dashboard.objects.filter(org=trial_org).count() == 1
    assert DashboardFilter.objects.filter(dashboard__org=trial_org).count() == 1
    assert Alert.objects.filter(org=trial_org).count() == 1
    assert ReportSnapshot.objects.filter(org=trial_org).count() == 1

    trial_chart = Chart.objects.get(org=trial_org)
    trial_metric = Metric.objects.get(org=trial_org)
    assert trial_chart.extra_config["metrics"][0]["saved_metric_id"] == trial_metric.id

    trial_dashboard = Dashboard.objects.get(org=trial_org)
    # this template dashboard is private → clone stays private, no share token
    assert trial_dashboard.is_public is False
    assert trial_dashboard.public_share_token is None
    trial_kpi = KPI.objects.get(org=trial_org)
    comps = trial_dashboard.tabs[0]["components"]
    assert comps["comp1"]["config"]["chartId"] == trial_chart.id
    assert comps["comp2"]["config"]["kpiId"] == trial_kpi.id

    # template rows untouched
    assert Metric.objects.filter(org=template_org).count() == 1
    assert Chart.objects.filter(org=template_org).count() == 1
