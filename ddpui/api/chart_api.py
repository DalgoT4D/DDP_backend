from ninja import Router
from ninja.pagination import paginate, LimitOffsetPagination
from typing import List, Optional, Dict, Any
from django.http import JsonResponse
from django.core.exceptions import ValidationError
from ddpui.models.chart import ChartSnapshot
from django.shortcuts import get_object_or_404
from ddpui.utils.custom_logger import CustomLogger
from django.core.cache import cache
from django.utils import timezone
from datetime import timedelta

logger = CustomLogger("chart_api")

# Rate limiting constants
RATE_LIMIT_WINDOW = 60  # 1 minute
RATE_LIMIT_REQUESTS = 10  # 10 requests per minute per user
RATE_LIMIT_GENERATION_WINDOW = 300  # 5 minutes
RATE_LIMIT_GENERATION_REQUESTS = 5  # 5 chart generation requests per 5 minutes per user


def rate_limit_check(user_id: int, endpoint: str, limit: int, window: int) -> bool:
    """Check if user has exceeded rate limit for a specific endpoint"""
    cache_key = f"rate_limit:{user_id}:{endpoint}"
    current_time = timezone.now()

    # Get current requests from cache
    requests = cache.get(cache_key, [])

    # Remove old requests outside the window
    cutoff_time = current_time - timedelta(seconds=window)
    requests = [req_time for req_time in requests if req_time > cutoff_time]

    # Check if limit exceeded
    if len(requests) >= limit:
        return False

    # Add current request
    requests.append(current_time)
    cache.set(cache_key, requests, window)

    return True


def rate_limit_response(endpoint: str, limit: int, window: int):
    """Return rate limit exceeded response"""
    return JsonResponse(
        {
            "success": False,
            "error": "Rate limit exceeded",
            "message": f"Too many requests to {endpoint}. Limit: {limit} requests per {window} seconds",
            "retry_after": window,
        },
        status=429,
    )


# dependencies
from ddpui import auth
from ddpui.auth import has_permission
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.chart import Chart
from ddpui.core.chart_service import ChartService
from ddpui.schemas.chart_schema import (
    ChartCreateSchema,
    ChartUpdateSchema,
    ChartResponseSchema,
    ChartResponseDataSchema,
    ChartGenerateSchema,
    ChartGenerateResponseSchema,
    ChartListResponseSchema,
    DatabaseObjectResponseSchema,
)

chart_router = Router()


@chart_router.post("/", response=ChartResponseSchema)
@has_permission(["can_create_chart"])
def create_chart(request, payload: ChartCreateSchema):
    """Create a new chart"""
    orguser = request.orguser

    # Rate limiting check
    if not rate_limit_check(
        orguser.user.id, "create_chart", RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW
    ):
        return rate_limit_response("create_chart", RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW)

    try:
        logger.info(
            f"Chart creation request from user {orguser.user.email} for org {orguser.org.slug}"
        )

        chart_service = ChartService(orguser.org, orguser)

        chart = chart_service.create_chart(
            title=payload.title,
            description=payload.description,
            chart_type=payload.chart_type,
            schema_name=payload.schema_name,
            table_name=payload.table,
            config=payload.config.dict() if hasattr(payload.config, "dict") else payload.config,
            is_public=payload.is_public if payload.is_public is not None else False,
        )

        logger.info(f"Successfully created chart {chart.id} for user {orguser.user.email}")
        return JsonResponse(
            {"success": True, "data": chart.to_dict(), "message": "Chart created successfully"}
        )

    except ValidationError as e:
        logger.warning(f"Chart creation validation error for user {orguser.user.email}: {str(e)}")
        return JsonResponse(
            {"success": False, "error": str(e), "message": "Failed to create chart"}, status=400
        )
    except Exception as e:
        logger.error(
            f"Unexpected error creating chart for user {orguser.user.email}: {str(e)}",
            exc_info=True,
        )
        return JsonResponse(
            {
                "success": False,
                "error": "An unexpected error occurred",
                "message": "Internal server error",
            },
            status=500,
        )


@chart_router.get("/", response=List[ChartResponseDataSchema])
@has_permission(["can_view_chart"])
@paginate(LimitOffsetPagination)
def get_charts(
    request,
    chart_type: Optional[str] = None,
    schema_name: Optional[str] = None,
    table_name: Optional[str] = None,
    created_by: Optional[int] = None,
    is_public: Optional[bool] = None,
):
    """Get all charts with optional filters"""
    try:
        orguser = request.orguser
        chart_service = ChartService(orguser.org, orguser)

        filters = {}
        if chart_type:
            filters["chart_type"] = chart_type
        if schema_name:
            filters["schema_name"] = schema_name
        if table_name:
            filters["table_name"] = table_name
        if created_by:
            filters["created_by"] = created_by
        if is_public is not None:
            filters["is_public"] = is_public

        charts = chart_service.get_charts(filters)

        return [chart.to_dict() for chart in charts]

    except ValidationError as e:
        return JsonResponse(
            {"success": False, "error": str(e), "message": "Failed to retrieve charts"}, status=400
        )
    except Exception as e:
        return JsonResponse(
            {"success": False, "error": str(e), "message": "Internal server error"}, status=500
        )


@chart_router.post("/cleanup-cache")
@has_permission(["can_delete_chart"])
def cleanup_cache(request):
    """Clean up expired chart snapshots"""
    try:
        ChartService.clean_expired_snapshots()

        return JsonResponse({"success": True, "message": "Cache cleaned successfully"})

    except Exception as e:
        return JsonResponse(
            {"success": False, "error": str(e), "message": "Failed to clean cache"}, status=500
        )


@chart_router.get("/cache-stats")
@has_permission(["can_view_chart"])
def get_cache_stats(request):
    """Get cache statistics for monitoring"""
    try:
        orguser = request.orguser
        chart_service = ChartService(orguser.org, orguser)

        cache_stats = chart_service.get_cache_stats()
        pool_stats = chart_service._get_connection_pool_stats()

        return JsonResponse(
            {
                "success": True,
                "data": {"cache_stats": cache_stats, "connection_pool_stats": pool_stats},
                "message": "Cache statistics retrieved successfully",
            }
        )

    except Exception as e:
        return JsonResponse(
            {"success": False, "error": str(e), "message": "Failed to retrieve cache statistics"},
            status=500,
        )


@chart_router.post("/generate", response=ChartGenerateResponseSchema)
@has_permission(["can_create_chart"])
def generate_chart_data(request, payload: ChartGenerateSchema):
    """Generate chart data for preview"""
    orguser = request.orguser

    # Rate limiting check for chart generation (more restrictive)
    if not rate_limit_check(
        orguser.user.id,
        "generate_chart",
        RATE_LIMIT_GENERATION_REQUESTS,
        RATE_LIMIT_GENERATION_WINDOW,
    ):
        return rate_limit_response(
            "generate_chart", RATE_LIMIT_GENERATION_REQUESTS, RATE_LIMIT_GENERATION_WINDOW
        )

    try:
        chart_service = ChartService(orguser.org, orguser)

        chart_data = chart_service.generate_chart_data(
            chart_type=payload.chart_type,
            computation_type=payload.computation_type,
            schema_name=payload.schema_name,
            table_name=payload.table_name,
            xaxis=getattr(payload, "xaxis", None),
            yaxis=getattr(payload, "yaxis", None),
            dimensions=getattr(payload, "dimensions", None),
            aggregate_col=getattr(payload, "aggregate_col", None),
            aggregate_func=getattr(payload, "aggregate_func", None),
            aggregate_col_alias=getattr(payload, "aggregate_col_alias", None),
            dimension_col=getattr(payload, "dimension_col", None),
            offset=getattr(payload, "offset", 0),
            limit=getattr(payload, "limit", 100),
        )

        return JsonResponse(
            {"success": True, "data": chart_data, "message": "Chart data generated successfully"}
        )

    except ValidationError as e:
        return JsonResponse(
            {"success": False, "error": str(e), "message": "Failed to generate chart data"},
            status=400,
        )
    except Exception as e:
        return JsonResponse(
            {"success": False, "error": str(e), "message": "Internal server error"}, status=500
        )


@chart_router.get("/{chart_id}", response=ChartResponseSchema)
@has_permission(["can_view_chart"])
def get_chart(request, chart_id: int):
    """Get a specific chart"""
    try:
        orguser = request.orguser
        chart_service = ChartService(orguser.org, orguser)

        chart = chart_service.get_chart(chart_id)

        return JsonResponse(
            {"success": True, "data": chart.to_dict(), "message": "Chart retrieved successfully"}
        )

    except ValidationError as e:
        return JsonResponse(
            {"success": False, "error": str(e), "message": "Failed to retrieve chart"}, status=400
        )
    except Exception as e:
        return JsonResponse(
            {"success": False, "error": str(e), "message": "Internal server error"}, status=500
        )


@chart_router.put("/{chart_id}", response=ChartResponseSchema)
@has_permission(["can_edit_chart"])
def update_chart(request, chart_id: int, payload: ChartUpdateSchema):
    """Update a chart"""
    orguser = request.orguser

    # Rate limiting check
    if not rate_limit_check(
        orguser.user.id, "update_chart", RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW
    ):
        return rate_limit_response("update_chart", RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW)

    try:
        chart_service = ChartService(orguser.org, orguser)

        chart = chart_service.update_chart(
            chart_id=chart_id,
            title=payload.title,
            description=payload.description,
            chart_type=payload.chart_type,
            schema_name=payload.schema_name,
            table_name=payload.table,
            config=payload.config.dict()
            if payload.config and hasattr(payload.config, "dict")
            else payload.config,
            is_public=payload.is_public,
        )

        return JsonResponse(
            {"success": True, "data": chart.to_dict(), "message": "Chart updated successfully"}
        )

    except ValidationError as e:
        return JsonResponse(
            {"success": False, "error": str(e), "message": "Failed to update chart"}, status=400
        )
    except Exception as e:
        return JsonResponse(
            {"success": False, "error": str(e), "message": "Internal server error"}, status=500
        )


@chart_router.delete("/{chart_id}")
@has_permission(["can_delete_chart"])
def delete_chart(request, chart_id: int):
    """Delete a chart"""
    orguser = request.orguser

    # Rate limiting check
    if not rate_limit_check(
        orguser.user.id, "delete_chart", RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW
    ):
        return rate_limit_response("delete_chart", RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW)

    try:
        chart_service = ChartService(orguser.org, orguser)

        chart_service.delete_chart(chart_id)

        return JsonResponse({"success": True, "message": "Chart deleted successfully"})

    except ValidationError as e:
        return JsonResponse(
            {"success": False, "error": str(e), "message": "Failed to delete chart"}, status=400
        )
    except Exception as e:
        return JsonResponse(
            {"success": False, "error": str(e), "message": "Internal server error"}, status=500
        )


@chart_router.post("/{chart_id}/favorite")
@has_permission(["can_view_chart"])
def toggle_favorite(request, chart_id: int):
    """Toggle favorite status of a chart"""
    try:
        orguser = request.orguser
        chart = get_object_or_404(Chart, id=chart_id, org=orguser.org)

        # Check if user can view the chart
        if not chart.can_view(orguser):
            return JsonResponse(
                {
                    "success": False,
                    "error": "Permission denied",
                    "message": "You don't have permission to access this chart",
                },
                status=403,
            )

        chart.is_favorite = not chart.is_favorite
        chart.save()

        return JsonResponse(
            {
                "success": True,
                "data": {"id": chart.id, "is_favorite": chart.is_favorite},
                "message": f"Chart {'added to' if chart.is_favorite else 'removed from'} favorites",
            }
        )

    except Exception as e:
        return JsonResponse(
            {"success": False, "error": str(e), "message": "Failed to toggle favorite"}, status=500
        )
