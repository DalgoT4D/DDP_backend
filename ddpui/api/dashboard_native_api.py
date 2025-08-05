"""Native Dashboard API endpoints"""

from typing import Optional, List
from datetime import datetime, timedelta
import uuid

from ninja import Router, Schema
from ninja.errors import HttpError
from django.utils import timezone
from django.db import transaction
from django.db.models import Q, Prefetch

from ddpui.models.dashboard import (
    Dashboard,
    DashboardFilter,
    DashboardLock,
    DashboardType,
    DashboardComponentType,
    DashboardFilterType,
)
from ddpui.models.org_user import OrgUser
from ddpui.auth import has_permission
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

dashboard_native_router = Router()


# Schemas
class DashboardCreate(Schema):
    """Schema for creating a dashboard"""

    title: str
    description: Optional[str] = None
    grid_columns: int = 12


class DashboardUpdate(Schema):
    """Schema for updating a dashboard"""

    title: Optional[str] = None
    description: Optional[str] = None
    layout_config: Optional[dict] = None
    components: Optional[dict] = None
    is_published: Optional[bool] = None


class FilterCreate(Schema):
    """Schema for creating a filter"""

    filter_type: str
    schema_name: str
    table_name: str
    column_name: str
    settings: dict = {}
    order: int = 0


class FilterUpdate(Schema):
    """Schema for updating a filter"""

    filter_type: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    settings: Optional[dict] = None
    order: Optional[int] = None


class DashboardFilterResponse(Schema):
    """Response schema for dashboard filter"""

    id: int
    dashboard_id: int
    filter_type: str
    schema_name: str
    table_name: str
    column_name: str
    settings: dict
    order: int
    created_at: datetime
    updated_at: datetime


class DashboardResponse(Schema):
    """Response schema for dashboard"""

    id: int
    title: str
    description: Optional[str]
    dashboard_type: str
    grid_columns: int
    layout_config: dict
    components: dict
    is_published: bool
    published_at: Optional[datetime]
    is_locked: bool = False
    locked_by: Optional[str] = None
    created_by: str
    org_id: int
    last_modified_by: Optional[str]
    created_at: datetime
    updated_at: datetime
    filters: List[DashboardFilterResponse] = []


class LockResponse(Schema):
    """Response schema for dashboard lock"""

    lock_token: str
    expires_at: datetime
    locked_by: str


class FilterOptionsResponse(Schema):
    """Response schema for filter options"""

    options: List[str]
    total_count: int


# Helper functions
def get_dashboard_response(dashboard: Dashboard) -> dict:
    """Convert dashboard model to response dict"""
    lock = getattr(dashboard, "lock", None)

    response_data = dashboard.to_json()
    response_data["is_locked"] = bool(lock and not lock.is_expired())
    response_data["locked_by"] = (
        lock.locked_by.user.email if lock and not lock.is_expired() else None
    )

    # Add filters
    response_data["filters"] = [f.to_json() for f in dashboard.filters.all()]

    return response_data


# Endpoints
@dashboard_native_router.get("/", response=List[DashboardResponse])
# @has_permission(["can_view_dashboards"])
def list_dashboards(
    request,
    dashboard_type: Optional[str] = None,
    search: Optional[str] = None,
    is_published: Optional[bool] = None,
):
    """List all dashboards with optional filters"""
    orguser: OrgUser = request.orguser

    # Build query
    query = Q(org=orguser.org)

    if dashboard_type:
        query &= Q(dashboard_type=dashboard_type)

    if search:
        query &= Q(title__icontains=search) | Q(description__icontains=search)

    if is_published is not None:
        query &= Q(is_published=is_published)

    # Fetch dashboards with related data
    dashboards = (
        Dashboard.objects.filter(query)
        .prefetch_related("filters", Prefetch("lock", queryset=DashboardLock.objects.all()))
        .order_by("-updated_at")
    )

    return [get_dashboard_response(d) for d in dashboards]


@dashboard_native_router.get("/{dashboard_id}/", response=DashboardResponse)
# @has_permission(["can_view_dashboards"])
def get_dashboard(request, dashboard_id: int):
    """Get a specific dashboard"""
    orguser: OrgUser = request.orguser

    try:
        dashboard = Dashboard.objects.prefetch_related(
            "filters", Prefetch("lock", queryset=DashboardLock.objects.all())
        ).get(id=dashboard_id, org=orguser.org)
    except Dashboard.DoesNotExist:
        raise HttpError(404, "Dashboard not found")

    return get_dashboard_response(dashboard)


@dashboard_native_router.post("/", response=DashboardResponse)
# @has_permission(["can_create_dashboards"])
def create_dashboard(request, payload: DashboardCreate):
    """Create a new dashboard"""
    orguser: OrgUser = request.orguser

    dashboard = Dashboard.objects.create(
        title=payload.title,
        description=payload.description,
        grid_columns=payload.grid_columns,
        created_by=orguser,
        org=orguser.org,
        last_modified_by=orguser,
    )

    logger.info(f"Created dashboard {dashboard.id} for org {orguser.org.id}")

    return get_dashboard_response(dashboard)


@dashboard_native_router.put("/{dashboard_id}/", response=DashboardResponse)
# @has_permission(["can_edit_dashboards"])
def update_dashboard(request, dashboard_id: int, payload: DashboardUpdate):
    """Update dashboard with auto-save support"""
    orguser: OrgUser = request.orguser

    try:
        dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
    except Dashboard.DoesNotExist:
        raise HttpError(404, "Dashboard not found")

    # Check if dashboard is locked by another user
    if hasattr(dashboard, "lock") and dashboard.lock:
        if not dashboard.lock.is_expired() and dashboard.lock.locked_by != orguser:
            raise HttpError(423, f"Dashboard is locked by {dashboard.lock.locked_by.user.email}")

    # Update fields
    if payload.title is not None:
        dashboard.title = payload.title

    if payload.description is not None:
        dashboard.description = payload.description

    if payload.layout_config is not None:
        dashboard.layout_config = payload.layout_config

    if payload.components is not None:
        dashboard.components = payload.components

    if payload.is_published is not None:
        dashboard.is_published = payload.is_published
        if payload.is_published:
            dashboard.published_at = timezone.now()

    dashboard.last_modified_by = orguser
    dashboard.save()

    return get_dashboard_response(dashboard)


@dashboard_native_router.delete("/{dashboard_id}/")
# @has_permission(["can_delete_dashboards"])
def delete_dashboard(request, dashboard_id: int):
    """Delete a dashboard"""
    orguser: OrgUser = request.orguser

    try:
        dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
    except Dashboard.DoesNotExist:
        raise HttpError(404, "Dashboard not found")

    # Check if dashboard is locked
    if hasattr(dashboard, "lock") and dashboard.lock and not dashboard.lock.is_expired():
        raise HttpError(423, "Cannot delete a locked dashboard")

    dashboard.delete()
    logger.info(f"Deleted dashboard {dashboard_id} for org {orguser.org.id}")

    return {"success": True}


# Dashboard Lock endpoints
@dashboard_native_router.post("/{dashboard_id}/lock/", response=LockResponse)
# @has_permission(["can_edit_dashboards"])
def lock_dashboard(request, dashboard_id: int):
    """Lock dashboard for editing"""
    orguser: OrgUser = request.orguser

    try:
        dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
    except Dashboard.DoesNotExist:
        raise HttpError(404, "Dashboard not found")

    # Check if already locked
    try:
        lock = dashboard.lock
        if not lock.is_expired():
            if lock.locked_by == orguser:
                # Refresh lock
                lock.expires_at = timezone.now() + timedelta(minutes=30)
                lock.save()
                return LockResponse(
                    lock_token=lock.lock_token,
                    expires_at=lock.expires_at,
                    locked_by=lock.locked_by.user.email,
                )
            else:
                raise HttpError(423, f"Dashboard is already locked by {lock.locked_by.user.email}")
        else:
            # Delete expired lock
            lock.delete()
    except DashboardLock.DoesNotExist:
        pass

    # Create new lock
    lock = DashboardLock.objects.create(
        dashboard=dashboard,
        locked_by=orguser,
        lock_token=str(uuid.uuid4()),
        expires_at=timezone.now() + timedelta(minutes=30),
    )

    return LockResponse(
        lock_token=lock.lock_token, expires_at=lock.expires_at, locked_by=lock.locked_by.user.email
    )


@dashboard_native_router.delete("/{dashboard_id}/lock/")
# @has_permission(["can_edit_dashboards"])
def unlock_dashboard(request, dashboard_id: int):
    """Unlock dashboard"""
    orguser: OrgUser = request.orguser

    try:
        dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
    except Dashboard.DoesNotExist:
        raise HttpError(404, "Dashboard not found")

    try:
        lock = dashboard.lock
        if lock.locked_by != orguser:
            raise HttpError(403, "You can only unlock your own locks")
        lock.delete()
    except DashboardLock.DoesNotExist:
        pass

    return {"success": True}


# Filter endpoints
@dashboard_native_router.post("/{dashboard_id}/filters/", response=DashboardFilterResponse)
# @has_permission(["can_edit_dashboards"])
def create_filter(request, dashboard_id: int, payload: FilterCreate):
    """Add a filter to dashboard"""
    orguser: OrgUser = request.orguser

    try:
        dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
    except Dashboard.DoesNotExist:
        raise HttpError(404, "Dashboard not found")

    # Validate filter type
    if payload.filter_type not in [ft.value for ft in DashboardFilterType]:
        raise HttpError(400, f"Invalid filter type: {payload.filter_type}")

    filter = DashboardFilter.objects.create(
        dashboard=dashboard,
        filter_type=payload.filter_type,
        schema_name=payload.schema_name,
        table_name=payload.table_name,
        column_name=payload.column_name,
        settings=payload.settings,
        order=payload.order,
    )

    return filter.to_json()


@dashboard_native_router.put(
    "/{dashboard_id}/filters/{filter_id}/", response=DashboardFilterResponse
)
# @has_permission(["can_edit_dashboards"])
def update_filter(request, dashboard_id: int, filter_id: int, payload: FilterUpdate):
    """Update a dashboard filter"""
    orguser: OrgUser = request.orguser

    try:
        dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
        filter = dashboard.filters.get(id=filter_id)
    except Dashboard.DoesNotExist:
        raise HttpError(404, "Dashboard not found")
    except DashboardFilter.DoesNotExist:
        raise HttpError(404, "Filter not found")

    # Update fields
    if payload.filter_type is not None:
        if payload.filter_type not in [ft.value for ft in DashboardFilterType]:
            raise HttpError(400, f"Invalid filter type: {payload.filter_type}")
        filter.filter_type = payload.filter_type

    if payload.schema_name is not None:
        filter.schema_name = payload.schema_name

    if payload.table_name is not None:
        filter.table_name = payload.table_name

    if payload.column_name is not None:
        filter.column_name = payload.column_name

    if payload.settings is not None:
        filter.settings = payload.settings

    if payload.order is not None:
        filter.order = payload.order

    filter.save()

    return filter.to_json()


@dashboard_native_router.delete("/{dashboard_id}/filters/{filter_id}/")
# @has_permission(["can_edit_dashboards"])
def delete_filter(request, dashboard_id: int, filter_id: int):
    """Delete a dashboard filter"""
    orguser: OrgUser = request.orguser

    try:
        dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
        filter = dashboard.filters.get(id=filter_id)
    except Dashboard.DoesNotExist:
        raise HttpError(404, "Dashboard not found")
    except DashboardFilter.DoesNotExist:
        raise HttpError(404, "Filter not found")

    filter.delete()

    return {"success": True}


# Filter options endpoint
@dashboard_native_router.get("/filter-options/", response=FilterOptionsResponse)
# @has_permission(["can_view_dashboards"])
def get_filter_options(
    request, schema_name: str, table_name: str, column_name: str, limit: int = 100
):
    """Get distinct values for a column to use in filters"""
    orguser: OrgUser = request.orguser

    # Get org warehouse
    from ddpui.models.org import OrgWarehouse

    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(400, "No warehouse configured for organization")

    # Get filter options from service
    from ddpui.services.dashboard_service import DashboardService

    options = DashboardService.generate_filter_options(
        schema=schema_name,
        table=table_name,
        column=column_name,
        org_warehouse=org_warehouse,
        limit=limit,
    )

    return FilterOptionsResponse(options=options, total_count=len(options))


# Dashboard filter data endpoint
@dashboard_native_router.post("/{dashboard_id}/filter-data/")
# @has_permission(["can_view_dashboards"])
def get_filtered_dashboard_data(request, dashboard_id: int, filters: dict = {}):
    """Apply filters and get updated chart data for dashboard"""
    orguser: OrgUser = request.orguser

    from ddpui.services.dashboard_service import DashboardService

    try:
        chart_results = DashboardService.apply_filters(
            dashboard_id=dashboard_id, filters=filters, orguser=orguser
        )

        return {"success": True, "data": chart_results}
    except ValueError as e:
        raise HttpError(400, str(e))
    except Exception as e:
        logger.error(f"Error applying dashboard filters: {str(e)}")
        raise HttpError(500, "Failed to apply filters")
