"""API endpoints for the My Metrics feature"""

from typing import List

from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.models.org import OrgWarehouse
from ddpui.models.metrics import MetricDefinition, MetricAnnotation
from ddpui.core.alerts.alert_service import AlertService
from ddpui.core.metrics_service import fetch_metrics_data
from ddpui.schemas.metric_schema import (
    MetricCreate,
    MetricUpdate,
    MetricResponse,
    MetricDataRequest,
    MetricDataPoint,
    AnnotationCreate,
    AnnotationResponse,
    LatestAnnotationsRequest,
    LatestAnnotationEntry,
)
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

metrics_router = Router()


# ── Metric CRUD ──────────────────────────────────────────────────────────────


@metrics_router.get("/", response=List[MetricResponse])
@has_permission(["can_view_charts"])
def list_metrics(request):
    """List all metric definitions for the org"""
    org = request.orguser.org
    metrics = MetricDefinition.objects.filter(org=org).order_by("display_order", "name")
    return [
        MetricResponse(
            id=m.id,
            name=m.name,
            schema_name=m.schema_name,
            table_name=m.table_name,
            column=m.column,
            aggregation=m.aggregation,
            time_column=m.time_column,
            time_grain=m.time_grain,
            direction=m.direction,
            target_value=m.target_value,
            amber_threshold_pct=m.amber_threshold_pct,
            green_threshold_pct=m.green_threshold_pct,
            program_tag=m.program_tag,
            metric_type_tag=m.metric_type_tag,
            trend_periods=m.trend_periods,
            display_order=m.display_order,
            created_at=m.created_at,
            updated_at=m.updated_at,
        )
        for m in metrics
    ]


# ── Metric Data (live warehouse queries) ─────────────────────────────────────
# NOTE: This must be defined BEFORE /{metric_id}/ routes.
# Django Ninja 0.21 generates <metric_id> without int: converter, so the string
# "data" would match <metric_id> before reaching this literal path if ordered after.


@metrics_router.post("/data/", response=List[MetricDataPoint])
@has_permission(["can_view_charts"])
def fetch_metric_data(request, payload: MetricDataRequest):
    """
    Fetch current values + trend data for a list of metrics.
    Runs warehouse queries in parallel.
    """
    org = request.orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()

    metrics = MetricDefinition.objects.filter(id__in=payload.metric_ids, org=org)

    if not metrics.exists():
        return []

    if not org_warehouse:
        # Return a graceful per-metric error rather than a 400 so the frontend
        # can display "Data unavailable" on each card instead of leaving them
        # stuck on "Awaiting data" (which happens when the whole request fails).
        return [
            {
                "metric_id": m.id,
                "current_value": None,
                "rag_status": "grey",
                "achievement_pct": None,
                "trend": [],
                "error": "No warehouse configured for this organization",
            }
            for m in metrics
        ]

    results = fetch_metrics_data(org_warehouse, list(metrics))
    return results


@metrics_router.post("/", response=MetricResponse)
@has_permission(["can_create_charts"])
def create_metric(request, payload: MetricCreate):
    """Create a new metric definition"""
    orguser = request.orguser
    org = orguser.org

    metric = MetricDefinition.objects.create(
        org=org,
        name=payload.name,
        schema_name=payload.schema_name,
        table_name=payload.table_name,
        column=payload.column,
        aggregation=payload.aggregation,
        time_column=payload.time_column,
        time_grain=payload.time_grain,
        direction=payload.direction,
        target_value=payload.target_value,
        amber_threshold_pct=payload.amber_threshold_pct,
        green_threshold_pct=payload.green_threshold_pct,
        program_tag=payload.program_tag,
        metric_type_tag=payload.metric_type_tag,
        trend_periods=payload.trend_periods,
        display_order=payload.display_order,
        created_by=orguser,
    )

    return MetricResponse(
        id=metric.id,
        name=metric.name,
        schema_name=metric.schema_name,
        table_name=metric.table_name,
        column=metric.column,
        aggregation=metric.aggregation,
        time_column=metric.time_column,
        time_grain=metric.time_grain,
        direction=metric.direction,
        target_value=metric.target_value,
        amber_threshold_pct=metric.amber_threshold_pct,
        green_threshold_pct=metric.green_threshold_pct,
        program_tag=metric.program_tag,
        metric_type_tag=metric.metric_type_tag,
        trend_periods=metric.trend_periods,
        display_order=metric.display_order,
        created_at=metric.created_at,
        updated_at=metric.updated_at,
    )


@metrics_router.put("/{metric_id}/", response=MetricResponse)
@has_permission(["can_edit_charts"])
def update_metric(request, metric_id: int, payload: MetricUpdate):
    """Update an existing metric definition"""
    org = request.orguser.org

    try:
        metric = MetricDefinition.objects.get(id=metric_id, org=org)
    except MetricDefinition.DoesNotExist:
        raise HttpError(404, "Metric not found")

    # Update only fields that were explicitly included in the request body.
    # Do NOT skip None values — a client sending null for an optional field
    # (e.g. time_column) is intentionally clearing it.
    update_fields = payload.dict(exclude_unset=True)
    if update_fields.get(
        "aggregation"
    ) == "count_distinct" and AlertService.metric_has_linked_alerts(metric):
        raise HttpError(400, "Linked alerts do not support COUNT DISTINCT metrics")

    for field, value in update_fields.items():
        setattr(metric, field, value)

    metric.save()
    AlertService.sync_alerts_for_metric(metric)

    return MetricResponse(
        id=metric.id,
        name=metric.name,
        schema_name=metric.schema_name,
        table_name=metric.table_name,
        column=metric.column,
        aggregation=metric.aggregation,
        time_column=metric.time_column,
        time_grain=metric.time_grain,
        direction=metric.direction,
        target_value=metric.target_value,
        amber_threshold_pct=metric.amber_threshold_pct,
        green_threshold_pct=metric.green_threshold_pct,
        program_tag=metric.program_tag,
        metric_type_tag=metric.metric_type_tag,
        trend_periods=metric.trend_periods,
        display_order=metric.display_order,
        created_at=metric.created_at,
        updated_at=metric.updated_at,
    )


@metrics_router.delete("/{metric_id}/")
@has_permission(["can_edit_charts"])
def delete_metric(request, metric_id: int):
    """Delete a metric definition"""
    org = request.orguser.org

    try:
        metric = MetricDefinition.objects.get(id=metric_id, org=org)
    except MetricDefinition.DoesNotExist:
        raise HttpError(404, "Metric not found")

    if AlertService.metric_has_linked_alerts(metric):
        raise HttpError(
            400,
            "This metric is linked to one or more alerts. Remove or relink those alerts first.",
        )

    metric.delete()
    return {"success": True}


# ── Annotations ──────────────────────────────────────────────────────────────


@metrics_router.post("/latest-annotations/", response=List[LatestAnnotationEntry])
@has_permission(["can_view_charts"])
def fetch_latest_annotations(request, payload: LatestAnnotationsRequest):
    """
    Return the most-recent annotation for each requested metric.
    Metrics with no annotation are omitted from the response.
    """
    org = request.orguser.org

    metrics = MetricDefinition.objects.filter(id__in=payload.metric_ids, org=org)

    # Fetch all annotations for the requested metrics in one query,
    # ordered newest-first so we can pick the first per metric.
    annotations = (
        MetricAnnotation.objects.filter(metric__in=metrics)
        .order_by("metric_id", "-period_key")
        .select_related("metric")
    )

    # Keep only the latest annotation per metric
    seen = set()
    results = []
    for a in annotations:
        if a.metric_id not in seen:
            seen.add(a.metric_id)
            results.append(
                LatestAnnotationEntry(
                    metric_id=a.metric_id,
                    id=a.id,
                    period_key=a.period_key,
                    rationale=a.rationale,
                    quote_text=a.quote_text,
                    quote_attribution=a.quote_attribution,
                    created_at=a.created_at,
                    updated_at=a.updated_at,
                )
            )

    return results


@metrics_router.get("/{metric_id}/annotations/", response=List[AnnotationResponse])
@has_permission(["can_view_charts"])
def list_annotations(request, metric_id: int):
    """Get all annotations for a metric"""
    org = request.orguser.org

    try:
        metric = MetricDefinition.objects.get(id=metric_id, org=org)
    except MetricDefinition.DoesNotExist:
        raise HttpError(404, "Metric not found")

    annotations = MetricAnnotation.objects.filter(metric=metric).order_by("-period_key")
    return [
        AnnotationResponse(
            id=a.id,
            period_key=a.period_key,
            rationale=a.rationale,
            quote_text=a.quote_text,
            quote_attribution=a.quote_attribution,
            created_at=a.created_at,
            updated_at=a.updated_at,
        )
        for a in annotations
    ]


@metrics_router.post("/{metric_id}/annotations/", response=AnnotationResponse)
@has_permission(["can_edit_charts"])
def create_or_update_annotation(request, metric_id: int, payload: AnnotationCreate):
    """Create or update an annotation for a metric + period"""
    orguser = request.orguser
    org = orguser.org

    try:
        metric = MetricDefinition.objects.get(id=metric_id, org=org)
    except MetricDefinition.DoesNotExist:
        raise HttpError(404, "Metric not found")

    annotation, created = MetricAnnotation.objects.update_or_create(
        metric=metric,
        period_key=payload.period_key,
        defaults={
            "rationale": payload.rationale,
            "quote_text": payload.quote_text,
            "quote_attribution": payload.quote_attribution,
            "created_by": orguser,
        },
    )

    return AnnotationResponse(
        id=annotation.id,
        period_key=annotation.period_key,
        rationale=annotation.rationale,
        quote_text=annotation.quote_text,
        quote_attribution=annotation.quote_attribution,
        created_at=annotation.created_at,
        updated_at=annotation.updated_at,
    )


@metrics_router.delete("/{metric_id}/annotations/{annotation_id}/")
@has_permission(["can_edit_charts"])
def delete_annotation(request, metric_id: int, annotation_id: int):
    """Delete an annotation for a metric."""
    org = request.orguser.org

    try:
        metric = MetricDefinition.objects.get(id=metric_id, org=org)
    except MetricDefinition.DoesNotExist:
        raise HttpError(404, "Metric not found")

    try:
        annotation = MetricAnnotation.objects.get(id=annotation_id, metric=metric)
    except MetricAnnotation.DoesNotExist:
        raise HttpError(404, "Annotation not found")

    annotation.delete()
    return {"success": True}
