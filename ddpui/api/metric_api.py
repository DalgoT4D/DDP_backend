"""Metric API endpoints"""

from typing import List

from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser
from ddpui.schemas.metric_schema import (
    MetricCreate,
    MetricUpdate,
    MetricResponse,
    MetricListResponse,
    MetricPreviewResponse,
    MetricConsumersResponse,
)
from ddpui.services.metric_service import (
    MetricService,
    MetricNotFoundError,
    MetricValidationError,
    MetricDeleteBlockedError,
)
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

metric_router = Router()


@metric_router.get("/", response=MetricListResponse)
@has_permission(["can_view_metrics"])
def list_metrics(
    request,
    page: int = 1,
    page_size: int = 10,
    search: str = None,
    schema_name: str = None,
    table_name: str = None,
):
    """List metrics for the organization with pagination and filtering"""
    orguser: OrgUser = request.orguser

    if page < 1:
        page = 1
    if page_size < 1 or page_size > 100:
        page_size = 10

    metrics, total = MetricService.list_metrics(
        org=orguser.org,
        page=page,
        page_size=page_size,
        search=search,
        schema_name=schema_name,
        table_name=table_name,
    )

    total_pages = (total + page_size - 1) // page_size

    metric_responses = [
        MetricResponse(
            id=m.id,
            name=m.name,
            description=m.description,
            schema_name=m.schema_name,
            table_name=m.table_name,
            column=m.column,
            aggregation=m.aggregation,
            column_expression=m.column_expression,
            created_at=m.created_at,
            updated_at=m.updated_at,
        )
        for m in metrics
    ]

    return MetricListResponse(
        data=metric_responses,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


@metric_router.post("/", response=MetricResponse)
@has_permission(["can_create_metrics"])
def create_metric(request, payload: MetricCreate):
    """Create a new metric"""
    orguser: OrgUser = request.orguser

    try:
        metric = MetricService.create_metric(
            name=payload.name,
            description=payload.description,
            schema_name=payload.schema_name,
            table_name=payload.table_name,
            column=payload.column,
            aggregation=payload.aggregation,
            column_expression=payload.column_expression,
            orguser=orguser,
        )
    except MetricValidationError as e:
        raise HttpError(400, e.message) from None

    return MetricResponse(
        id=metric.id,
        name=metric.name,
        description=metric.description,
        schema_name=metric.schema_name,
        table_name=metric.table_name,
        column=metric.column,
        aggregation=metric.aggregation,
        column_expression=metric.column_expression,
        created_at=metric.created_at,
        updated_at=metric.updated_at,
    )


@metric_router.get("/{metric_id}/", response=MetricResponse)
@has_permission(["can_view_metrics"])
def get_metric(request, metric_id: int):
    """Get a specific metric"""
    orguser: OrgUser = request.orguser

    try:
        metric = MetricService.get_metric(metric_id, orguser.org)
    except MetricNotFoundError:
        raise HttpError(404, "Metric not found") from None

    return MetricResponse(
        id=metric.id,
        name=metric.name,
        description=metric.description,
        schema_name=metric.schema_name,
        table_name=metric.table_name,
        column=metric.column,
        aggregation=metric.aggregation,
        column_expression=metric.column_expression,
        created_at=metric.created_at,
        updated_at=metric.updated_at,
    )


@metric_router.put("/{metric_id}/", response=MetricResponse)
@has_permission(["can_edit_metrics"])
def update_metric(request, metric_id: int, payload: MetricUpdate):
    """Update a metric"""
    orguser: OrgUser = request.orguser

    try:
        metric = MetricService.update_metric(
            metric_id=metric_id,
            org=orguser.org,
            orguser=orguser,
            name=payload.name,
            description=payload.description,
            schema_name=payload.schema_name,
            table_name=payload.table_name,
            column=payload.column,
            aggregation=payload.aggregation,
            column_expression=payload.column_expression,
        )
    except MetricNotFoundError:
        raise HttpError(404, "Metric not found") from None
    except MetricValidationError as e:
        raise HttpError(400, e.message) from None

    return MetricResponse(
        id=metric.id,
        name=metric.name,
        description=metric.description,
        schema_name=metric.schema_name,
        table_name=metric.table_name,
        column=metric.column,
        aggregation=metric.aggregation,
        column_expression=metric.column_expression,
        created_at=metric.created_at,
        updated_at=metric.updated_at,
    )


@metric_router.delete("/{metric_id}/")
@has_permission(["can_delete_metrics"])
def delete_metric(request, metric_id: int):
    """Delete a metric (blocked if referenced by charts or KPIs)"""
    orguser: OrgUser = request.orguser

    try:
        MetricService.delete_metric(metric_id, orguser.org, orguser)
    except MetricNotFoundError:
        raise HttpError(404, "Metric not found") from None
    except MetricDeleteBlockedError as e:
        raise HttpError(409, e.message) from None

    return {"success": True}


@metric_router.post("/{metric_id}/preview/", response=MetricPreviewResponse)
@has_permission(["can_view_metrics"])
def preview_metric(request, metric_id: int):
    """Compute the current value of a metric"""
    orguser: OrgUser = request.orguser

    try:
        result = MetricService.preview_metric_value(metric_id, orguser.org)
    except MetricNotFoundError:
        raise HttpError(404, "Metric not found") from None

    return MetricPreviewResponse(**result)


@metric_router.get("/{metric_id}/consumers/", response=MetricConsumersResponse)
@has_permission(["can_view_metrics"])
def get_metric_consumers(request, metric_id: int):
    """List charts and KPIs that reference this metric"""
    orguser: OrgUser = request.orguser

    try:
        consumers = MetricService.get_metric_consumers(metric_id, orguser.org)
    except MetricNotFoundError:
        raise HttpError(404, "Metric not found") from None

    return MetricConsumersResponse(**consumers)
