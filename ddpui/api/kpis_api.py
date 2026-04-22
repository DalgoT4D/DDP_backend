"""API endpoints for the KPI tracked layer + the append-only KPIEntry timeline.

Mirrors the old `/api/metrics/` surface but is semantically the KPI layer —
Metric is the reusable primitive (see metrics_api.py).
"""

from typing import List

from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.models.org import OrgWarehouse
from ddpui.models.metrics import Metric, KPI, KPIEntry
from ddpui.core.metrics_service import (
    MetricCompileError,
    compile_metric_value_expression,
    compute_rag_status,
    fetch_current_value,
    fetch_kpis_data,
)
from ddpui.schemas.kpi_schema import (
    KPICreate,
    KPIUpdate,
    KPIResponse,
    KPIDataRequest,
    KPIDataPoint,
    KPIEntryCreate,
    KPIEntryResponse,
    LatestEntriesRequest,
    LatestEntryResponse,
)
from ddpui.schemas.metric_schema import MetricResponse
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory

logger = CustomLogger("ddpui")

kpis_router = Router()


# ── helpers ──────────────────────────────────────────────────────────────────


def _serialize_metric(metric: Metric) -> MetricResponse:
    return MetricResponse(
        id=metric.id,
        name=metric.name,
        description=metric.description,
        tags=metric.tags or [],
        schema_name=metric.schema_name,
        table_name=metric.table_name,
        time_column=metric.time_column,
        default_time_grain=metric.default_time_grain,
        creation_mode=metric.creation_mode,
        simple_terms=metric.simple_terms or [],
        simple_formula=metric.simple_formula,
        sql_expression=metric.sql_expression,
        filters=metric.filters or [],
        created_at=metric.created_at,
        updated_at=metric.updated_at,
    )


def _serialize_kpi(kpi: KPI) -> KPIResponse:
    return KPIResponse(
        id=kpi.id,
        metric=_serialize_metric(kpi.metric),
        target_value=kpi.target_value,
        direction=kpi.direction,
        amber_threshold_pct=kpi.amber_threshold_pct,
        green_threshold_pct=kpi.green_threshold_pct,
        trend_grain=kpi.trend_grain,
        trend_periods=kpi.trend_periods,
        metric_type_tag=kpi.metric_type_tag,
        program_tag=kpi.program_tag,
        tags=kpi.tags or [],
        display_order=kpi.display_order,
        created_at=kpi.created_at,
        updated_at=kpi.updated_at,
    )


def _serialize_entry(entry: KPIEntry) -> KPIEntryResponse:
    created_by_name = ""
    try:
        created_by_name = entry.created_by.user.email or ""
    except Exception:
        pass
    return KPIEntryResponse(
        id=entry.id,
        entry_type=entry.entry_type,
        period_key=entry.period_key,
        content=entry.content,
        attribution=entry.attribution,
        snapshot_value=entry.snapshot_value,
        snapshot_rag=entry.snapshot_rag,
        snapshot_achievement_pct=entry.snapshot_achievement_pct,
        created_at=entry.created_at,
        created_by_name=created_by_name,
    )


def _resolve_metric_from_payload(org, payload: KPICreate, orguser) -> Metric:
    """Returns a Metric — either an existing one or the inline-created one."""
    if payload.metric_id is not None:
        try:
            return Metric.objects.get(id=payload.metric_id, org=org)
        except Metric.DoesNotExist:
            raise HttpError(404, "Metric not found")

    if payload.inline_metric is not None:
        inline = payload.inline_metric
        stub = Metric(
            name=inline.name,
            schema_name=inline.schema_name,
            table_name=inline.table_name,
            creation_mode=inline.creation_mode,
            simple_terms=[t.dict() for t in inline.simple_terms],
            simple_formula=inline.simple_formula,
            sql_expression=inline.sql_expression,
        )
        try:
            compile_metric_value_expression(stub)
        except MetricCompileError as e:
            raise HttpError(400, f"Invalid inline metric: {e}")
        return Metric.objects.create(
            org=org,
            name=inline.name,
            description=inline.description,
            tags=inline.tags,
            schema_name=inline.schema_name,
            table_name=inline.table_name,
            time_column=inline.time_column,
            default_time_grain=inline.default_time_grain,
            creation_mode=inline.creation_mode,
            simple_terms=[t.dict() for t in inline.simple_terms],
            simple_formula=inline.simple_formula,
            sql_expression=inline.sql_expression,
            filters=[f.dict() for f in inline.filters],
            created_by=orguser,
        )

    raise HttpError(400, "Provide either metric_id or inline_metric")


# ── KPI CRUD ─────────────────────────────────────────────────────────────────


@kpis_router.get("/", response=List[KPIResponse])
@has_permission(["can_view_charts"])
def list_kpis(request):
    """List every KPI in the org, ordered by display_order then created_at."""
    org = request.orguser.org
    return [
        _serialize_kpi(k)
        for k in KPI.objects.filter(org=org)
        .select_related("metric")
        .order_by("display_order", "-created_at")
    ]


# /data/ and bulk-entry endpoints before /{kpi_id}/ routes (Ninja first-match).


@kpis_router.post("/data/", response=List[KPIDataPoint])
@has_permission(["can_view_charts"])
def fetch_kpi_data(request, payload: KPIDataRequest):
    """Bulk-fetch current value + trend + RAG + period-over-period for a list of KPIs."""
    org = request.orguser.org
    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    kpis = list(
        KPI.objects.filter(id__in=payload.kpi_ids, org=org).select_related("metric")
    )
    if not kpis:
        return []
    if not org_warehouse:
        return [
            {
                "kpi_id": k.id,
                "current_value": None,
                "rag_status": "grey",
                "achievement_pct": None,
                "trend": [],
                "period_over_period_delta": None,
                "period_over_period_pct": None,
                "error": "No warehouse configured for this organization",
            }
            for k in kpis
        ]
    return fetch_kpis_data(org_warehouse, kpis)


@kpis_router.post("/latest-entries/", response=List[LatestEntryResponse])
@has_permission(["can_view_charts"])
def fetch_latest_entries(request, payload: LatestEntriesRequest):
    """Most-recent KPIEntry per KPI. KPIs with no entries are omitted."""
    org = request.orguser.org
    kpis = KPI.objects.filter(id__in=payload.kpi_ids, org=org)
    entries = (
        KPIEntry.objects.filter(kpi__in=kpis)
        .order_by("kpi_id", "-created_at")
        .select_related("kpi", "created_by__user")
    )
    seen: set = set()
    out: list = []
    for e in entries:
        if e.kpi_id in seen:
            continue
        seen.add(e.kpi_id)
        out.append(LatestEntryResponse(kpi_id=e.kpi_id, entry=_serialize_entry(e)))
    return out


@kpis_router.post("/", response=KPIResponse)
@has_permission(["can_create_charts"])
def create_kpi(request, payload: KPICreate):
    """Create a KPI from either an existing Metric or an inline new Metric."""
    orguser = request.orguser
    org = orguser.org
    metric = _resolve_metric_from_payload(org, payload, orguser)

    trend_grain = payload.trend_grain or metric.default_time_grain
    trend_periods = payload.trend_periods if payload.trend_periods is not None else 12

    kpi = KPI.objects.create(
        org=org,
        metric=metric,
        target_value=payload.target_value,
        direction=payload.direction,
        amber_threshold_pct=payload.amber_threshold_pct,
        green_threshold_pct=payload.green_threshold_pct,
        trend_grain=trend_grain,
        trend_periods=trend_periods,
        metric_type_tag=payload.metric_type_tag,
        program_tag=payload.program_tag,
        tags=payload.tags,
        display_order=payload.display_order,
        created_by=orguser,
    )
    return _serialize_kpi(kpi)


@kpis_router.get("/{kpi_id}/", response=KPIResponse)
@has_permission(["can_view_charts"])
def get_kpi(request, kpi_id: int):
    org = request.orguser.org
    try:
        kpi = KPI.objects.select_related("metric").get(id=kpi_id, org=org)
    except KPI.DoesNotExist:
        raise HttpError(404, "KPI not found")
    return _serialize_kpi(kpi)


@kpis_router.put("/{kpi_id}/", response=KPIResponse)
@has_permission(["can_edit_charts"])
def update_kpi(request, kpi_id: int, payload: KPIUpdate):
    org = request.orguser.org
    try:
        kpi = KPI.objects.select_related("metric").get(id=kpi_id, org=org)
    except KPI.DoesNotExist:
        raise HttpError(404, "KPI not found")

    for field, value in payload.dict(exclude_unset=True).items():
        setattr(kpi, field, value)
    kpi.save()
    return _serialize_kpi(kpi)


@kpis_router.delete("/{kpi_id}/")
@has_permission(["can_edit_charts"])
def delete_kpi(request, kpi_id: int):
    org = request.orguser.org
    try:
        kpi = KPI.objects.get(id=kpi_id, org=org)
    except KPI.DoesNotExist:
        raise HttpError(404, "KPI not found")

    # Blocked while any alert references this KPI.
    if kpi.alerts.exists():
        raise HttpError(
            400,
            "This KPI is linked to one or more alerts. Remove those alerts first.",
        )
    kpi.delete()
    return {"success": True}


# ── KPI entries (timeline) ──────────────────────────────────────────────────


@kpis_router.get("/{kpi_id}/entries/", response=List[KPIEntryResponse])
@has_permission(["can_view_charts"])
def list_entries(request, kpi_id: int):
    """Timeline for a KPI, newest-first."""
    org = request.orguser.org
    try:
        kpi = KPI.objects.get(id=kpi_id, org=org)
    except KPI.DoesNotExist:
        raise HttpError(404, "KPI not found")
    entries = (
        KPIEntry.objects.filter(kpi=kpi)
        .select_related("created_by__user")
        .order_by("-period_key", "-created_at")
    )
    return [_serialize_entry(e) for e in entries]


@kpis_router.post("/{kpi_id}/entries/", response=KPIEntryResponse)
@has_permission(["can_edit_charts"])
def create_entry(request, kpi_id: int, payload: KPIEntryCreate):
    """Create an entry; snapshots KPI's current value + RAG at save time."""
    orguser = request.orguser
    org = orguser.org
    try:
        kpi = KPI.objects.select_related("metric").get(id=kpi_id, org=org)
    except KPI.DoesNotExist:
        raise HttpError(404, "KPI not found")

    # Snapshot the KPI's current state so the entry is stable even as data drifts.
    snapshot_value, snapshot_rag, snapshot_ach = None, "", None
    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if org_warehouse:
        try:
            wclient = WarehouseFactory.get_warehouse_client(org_warehouse)
            value, err = fetch_current_value(wclient, kpi.metric)
            if err is None:
                snapshot_value = value
                snapshot_rag, snapshot_ach = compute_rag_status(
                    value,
                    kpi.target_value,
                    kpi.amber_threshold_pct,
                    kpi.green_threshold_pct,
                    kpi.direction,
                )
        except Exception as e:
            # Entry save should not fail just because the snapshot couldn't be captured.
            logger.warning(f"KPI {kpi.id} entry snapshot failed: {e}")

    entry = KPIEntry.objects.create(
        kpi=kpi,
        entry_type=payload.entry_type,
        period_key=payload.period_key,
        content=payload.content,
        attribution=payload.attribution,
        snapshot_value=snapshot_value,
        snapshot_rag=snapshot_rag or "",
        snapshot_achievement_pct=snapshot_ach,
        created_by=orguser,
    )
    return _serialize_entry(entry)


@kpis_router.delete("/{kpi_id}/entries/{entry_id}/")
@has_permission(["can_edit_charts"])
def delete_entry(request, kpi_id: int, entry_id: int):
    """Delete an entry. Author or anyone with edit permission on the KPI."""
    org = request.orguser.org
    try:
        entry = KPIEntry.objects.select_related("kpi").get(
            id=entry_id, kpi_id=kpi_id, kpi__org=org
        )
    except KPIEntry.DoesNotExist:
        raise HttpError(404, "Entry not found")
    entry.delete()
    return {"success": True}
