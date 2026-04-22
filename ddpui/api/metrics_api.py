"""API endpoints for the Metric primitive library (Batch 1).

Responsibilities:
  - CRUD on Metric (the reusable aggregation primitive)
  - Reference tracking (which KPIs / alerts / charts consume a Metric)
  - Bulk data fetch for library preview
  - Dry-run validation of Calculated-SQL expressions before save
"""

from typing import List

from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.models.org import OrgWarehouse
from ddpui.models.metrics import Metric, KPI
from ddpui.models.alert import Alert
from ddpui.core.metrics_service import (
    MetricCompileError,
    compile_metric_value_expression,
    fetch_metrics_data,
    validate_sql_against_warehouse,
)
from ddpui.schemas.metric_schema import (
    MetricCreate,
    MetricUpdate,
    MetricResponse,
    MetricDetailResponse,
    MetricReferencesResponse,
    MetricDataRequest,
    MetricDataPoint,
    ValidateSqlRequest,
    ValidateSqlResponse,
)
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

metrics_router = Router()


# ── helpers ──────────────────────────────────────────────────────────────────


def _serialize(metric: Metric) -> MetricResponse:
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


def _references_for(metric: Metric) -> MetricReferencesResponse:
    kpi_ids = list(KPI.objects.filter(metric=metric).values_list("id", flat=True))
    alert_ids = list(Alert.objects.filter(metric=metric).values_list("id", flat=True))
    return MetricReferencesResponse(
        metric_id=metric.id,
        kpi_count=len(kpi_ids),
        alert_count=len(alert_ids),
        chart_count=0,  # Charts land in Batch 6
        kpi_ids=kpi_ids,
        alert_ids=alert_ids,
    )


def _validate_payload(payload) -> None:
    """Pre-flight: reject Metric create/update payloads that won't compile."""
    stub = Metric(
        name=getattr(payload, "name", "") or "",
        schema_name=payload.schema_name or "",
        table_name=payload.table_name or "",
        creation_mode=payload.creation_mode or "simple",
        simple_terms=[t.dict() for t in (payload.simple_terms or [])],
        simple_formula=payload.simple_formula or "",
        sql_expression=payload.sql_expression or "",
    )
    try:
        compile_metric_value_expression(stub)
    except MetricCompileError as e:
        raise HttpError(400, f"Invalid metric definition: {e}")


# ── Metric CRUD ──────────────────────────────────────────────────────────────


@metrics_router.get("/", response=List[MetricResponse])
@has_permission(["can_view_charts"])
def list_metrics(request):
    """List every Metric in the org, ordered by name."""
    org = request.orguser.org
    return [_serialize(m) for m in Metric.objects.filter(org=org).order_by("name")]


# NOTE: /data/ and /validate-sql/ must come BEFORE /{metric_id}/ routes
# (Ninja path matching is first-match).


@metrics_router.post("/data/", response=List[MetricDataPoint])
@has_permission(["can_view_charts"])
def fetch_metric_data(request, payload: MetricDataRequest):
    """Bulk-fetch current value (and optional trend) for a list of Metrics."""
    org = request.orguser.org
    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    metrics = list(Metric.objects.filter(id__in=payload.metric_ids, org=org))
    if not metrics:
        return []
    if not org_warehouse:
        return [
            {
                "metric_id": m.id,
                "current_value": None,
                "trend": [],
                "error": "No warehouse configured for this organization",
            }
            for m in metrics
        ]
    return fetch_metrics_data(org_warehouse, metrics, include_trend=payload.include_trend)


@metrics_router.post("/validate-sql/", response=ValidateSqlResponse)
@has_permission(["can_create_charts"])
def validate_sql(request, payload: ValidateSqlRequest):
    """Dry-run a Calculated-SQL expression: compile-safe + runs + returns a scalar?"""
    org = request.orguser.org
    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        return ValidateSqlResponse(ok=False, error="No warehouse configured for this organization")
    result = validate_sql_against_warehouse(
        org_warehouse=org_warehouse,
        schema_name=payload.schema_name,
        table_name=payload.table_name,
        sql_expression=payload.sql_expression,
        filters=[f.dict() for f in payload.filters],
    )
    return ValidateSqlResponse(**result)


@metrics_router.post("/", response=MetricResponse)
@has_permission(["can_create_charts"])
def create_metric(request, payload: MetricCreate):
    """Create a new Metric primitive."""
    orguser = request.orguser
    org = orguser.org
    _validate_payload(payload)

    metric = Metric.objects.create(
        org=org,
        name=payload.name,
        description=payload.description,
        tags=payload.tags,
        schema_name=payload.schema_name,
        table_name=payload.table_name,
        time_column=payload.time_column,
        default_time_grain=payload.default_time_grain,
        creation_mode=payload.creation_mode,
        simple_terms=[t.dict() for t in payload.simple_terms],
        simple_formula=payload.simple_formula,
        sql_expression=payload.sql_expression,
        filters=[f.dict() for f in payload.filters],
        created_by=orguser,
    )
    return _serialize(metric)


@metrics_router.get("/{metric_id}/", response=MetricDetailResponse)
@has_permission(["can_view_charts"])
def get_metric(request, metric_id: int):
    """Return a single Metric along with its references (kpi/alert/chart counts)."""
    org = request.orguser.org
    try:
        metric = Metric.objects.get(id=metric_id, org=org)
    except Metric.DoesNotExist:
        raise HttpError(404, "Metric not found")
    return MetricDetailResponse(metric=_serialize(metric), references=_references_for(metric))


@metrics_router.put("/{metric_id}/", response=MetricResponse)
@has_permission(["can_edit_charts"])
def update_metric(request, metric_id: int, payload: MetricUpdate):
    """Partial-update a Metric. Rejects if the new shape fails to compile."""
    org = request.orguser.org
    try:
        metric = Metric.objects.get(id=metric_id, org=org)
    except Metric.DoesNotExist:
        raise HttpError(404, "Metric not found")

    update_fields = payload.dict(exclude_unset=True)

    # Normalize nested schemas to plain dicts for JSONField storage.
    if "simple_terms" in update_fields:
        update_fields["simple_terms"] = [
            t if isinstance(t, dict) else t.dict()
            for t in (update_fields["simple_terms"] or [])
        ]
    if "filters" in update_fields:
        update_fields["filters"] = [
            f if isinstance(f, dict) else f.dict()
            for f in (update_fields["filters"] or [])
        ]

    for field, value in update_fields.items():
        setattr(metric, field, value)

    # Compile-check before persisting.
    try:
        compile_metric_value_expression(metric)
    except MetricCompileError as e:
        raise HttpError(400, f"Invalid metric definition: {e}")

    metric.save()
    return _serialize(metric)


@metrics_router.delete("/{metric_id}/")
@has_permission(["can_edit_charts"])
def delete_metric(request, metric_id: int):
    """Delete a Metric. Blocked while any KPI or alert references it."""
    org = request.orguser.org
    try:
        metric = Metric.objects.get(id=metric_id, org=org)
    except Metric.DoesNotExist:
        raise HttpError(404, "Metric not found")

    refs = _references_for(metric)
    if refs.kpi_count or refs.alert_count or refs.chart_count:
        raise HttpError(
            400,
            f"Metric is used by {refs.kpi_count} KPI(s), "
            f"{refs.alert_count} alert(s), and {refs.chart_count} chart(s). "
            "Remove those references first.",
        )
    metric.delete()
    return {"success": True}


@metrics_router.get("/{metric_id}/references/", response=MetricReferencesResponse)
@has_permission(["can_view_charts"])
def get_references(request, metric_id: int):
    """Blast-radius summary used by the 'you're about to edit this' confirm dialog."""
    org = request.orguser.org
    try:
        metric = Metric.objects.get(id=metric_id, org=org)
    except Metric.DoesNotExist:
        raise HttpError(404, "Metric not found")
    return _references_for(metric)
