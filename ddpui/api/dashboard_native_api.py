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
    grid_columns: Optional[int] = None
    layout_config: Optional[list[dict]] = None
    components: Optional[dict] = None
    filters: Optional[list[dict]] = None
    is_published: Optional[bool] = None


class FilterCreate(Schema):
    """Schema for creating a filter"""

    name: Optional[str] = None
    filter_type: str
    schema_name: str
    table_name: str
    column_name: str
    settings: dict = {}
    order: int = 0


class FilterUpdate(Schema):
    """Schema for updating a filter"""

    name: Optional[str] = None
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
    name: str
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
    layout_config: list[dict]
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
    """Convert dashboard model to response dict with migration support"""
    lock = getattr(dashboard, "lock", None)

    # Check if components need migration from old filter format to new format
    components = dashboard.components or {}
    needs_update = False

    for comp_id, component in components.items():
        if component.get("type") == "filter":
            # Check if this is old format (full config) vs new format (just filterId reference)
            config = component.get("config", {})
            if "filterId" not in config and "column_name" in config:
                # This is old format, needs migration
                # Find or create matching filter in DashboardFilter table
                matching_filter = dashboard.filters.filter(
                    column_name=config.get("column_name"),
                    table_name=config.get("table_name", ""),
                    schema_name=config.get("schema_name", ""),
                ).first()

                if not matching_filter:
                    # Create filter if it doesn't exist
                    matching_filter = DashboardFilter.objects.create(
                        dashboard=dashboard,
                        name=config.get("name", config.get("column_name", "")),
                        filter_type=config.get("filter_type", "value"),
                        schema_name=config.get("schema_name", ""),
                        table_name=config.get("table_name", ""),
                        column_name=config.get("column_name", ""),
                        settings=config.get("settings", {}),
                        order=0,
                    )

                # Update component to new format with just filterId reference
                components[comp_id] = {
                    "id": comp_id,
                    "type": "filter",
                    "config": {
                        "filterId": matching_filter.id,
                        "name": config.get("name", matching_filter.column_name),
                    },
                }
                needs_update = True

    if needs_update:
        # Save migrated components back to database
        dashboard.components = components
        dashboard.save(update_fields=["components"])
        logger.info(f"Migrated filter components for dashboard {dashboard.id}")

    response_data = dashboard.to_json()
    response_data["is_locked"] = bool(lock and not lock.is_expired())
    response_data["locked_by"] = (
        lock.locked_by.user.email if lock and not lock.is_expired() else None
    )

    # Add filters without position data in settings
    filters_data = []
    for f in dashboard.filters.all():
        filter_json = f.to_json()
        # Remove position and name from settings if they exist (for backward compatibility)
        if "settings" in filter_json:
            filter_json["settings"].pop("position", None)
            filter_json["settings"].pop("name", None)
        filters_data.append(filter_json)

    response_data["filters"] = filters_data

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
    # dashboards = (
    #     Dashboard.objects.filter(query)
    #     .prefetch_related("filters", Prefetch("lock", queryset=DashboardLock.objects.all()))
    #     .order_by("-updated_at")
    # )
    dashboards = Dashboard.objects.filter(query).order_by("-updated_at")

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

    if payload.grid_columns is not None:
        dashboard.grid_columns = payload.grid_columns

    if payload.layout_config is not None:
        dashboard.layout_config = payload.layout_config

    if payload.components is not None:
        dashboard.components = payload.components

    # Handle filters update with intelligent sync
    if payload.filters is not None:
        # Extract filter IDs that are currently in components
        filter_ids_in_components = set()
        if payload.components:
            for component in payload.components.values():
                if component.get("type") == "filter":
                    filter_id = component.get("config", {}).get("filterId")
                    if filter_id:
                        filter_ids_in_components.add(filter_id)

        # Get existing filter IDs
        existing_filter_ids = set(dashboard.filters.values_list("id", flat=True))

        # Delete filters that are no longer in components
        filters_to_delete = existing_filter_ids - filter_ids_in_components
        if filters_to_delete:
            dashboard.filters.filter(id__in=filters_to_delete).delete()
            logger.info(f"Deleted filters {filters_to_delete} from dashboard {dashboard_id}")

        # Update or create filters from payload
        for filter_data in payload.filters:
            filter_id = filter_data.get("id")

            # Clean settings - remove position and name if present
            settings = filter_data.get("settings", {}).copy()
            settings.pop("position", None)
            settings.pop("name", None)

            if filter_id and filter_id in existing_filter_ids:
                # Update existing filter
                dashboard.filters.filter(id=filter_id).update(
                    name=filter_data.get("name", ""),
                    filter_type=filter_data.get("filter_type", "value"),
                    schema_name=filter_data.get("schema_name", ""),
                    table_name=filter_data.get("table_name", ""),
                    column_name=filter_data.get("column_name", ""),
                    settings=settings,
                    order=filter_data.get("order", 0),
                )
            else:
                # Create new filter
                new_filter = DashboardFilter.objects.create(
                    dashboard=dashboard,
                    name=filter_data.get("name", filter_data.get("column_name", "")),
                    filter_type=filter_data.get("filter_type", "value"),
                    schema_name=filter_data.get("schema_name", ""),
                    table_name=filter_data.get("table_name", ""),
                    column_name=filter_data.get("column_name", ""),
                    settings=settings,
                    order=filter_data.get("order", 0),
                )
                # Update the filter_data with the new ID for response
                filter_data["id"] = new_filter.id

    if payload.is_published is not None:
        dashboard.is_published = payload.is_published
        if payload.is_published:
            dashboard.published_at = timezone.now()

    dashboard.last_modified_by = orguser
    dashboard.save()

    # Auto-refresh lock if dashboard is locked by current user
    if hasattr(dashboard, "lock") and dashboard.lock:
        if not dashboard.lock.is_expired() and dashboard.lock.locked_by == orguser:
            dashboard.lock.expires_at = timezone.now() + timedelta(minutes=2)
            dashboard.lock.save()
            logger.info(f"Auto-refreshed lock for dashboard {dashboard_id} during save")

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
                # Refresh lock with 2-minute duration
                lock.expires_at = timezone.now() + timedelta(minutes=2)
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

    # Create new lock with 2-minute duration
    lock = DashboardLock.objects.create(
        dashboard=dashboard,
        locked_by=orguser,
        lock_token=str(uuid.uuid4()),
        expires_at=timezone.now() + timedelta(minutes=2),
    )

    return LockResponse(
        lock_token=lock.lock_token, expires_at=lock.expires_at, locked_by=lock.locked_by.user.email
    )


@dashboard_native_router.put("/{dashboard_id}/lock/refresh/")
# @has_permission(["can_edit_dashboards"])
def refresh_dashboard_lock(request, dashboard_id: int):
    """Refresh dashboard lock to extend expiry"""
    orguser: OrgUser = request.orguser

    try:
        dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
    except Dashboard.DoesNotExist:
        raise HttpError(404, "Dashboard not found")

    try:
        lock = dashboard.lock
        if lock.is_expired():
            raise HttpError(410, "Lock has expired")
        if lock.locked_by != orguser:
            raise HttpError(403, "You can only refresh your own locks")

        # Refresh lock with 2-minute duration
        lock.expires_at = timezone.now() + timedelta(minutes=2)
        lock.save()

        logger.info(f"Refreshed lock for dashboard {dashboard_id}")

        return LockResponse(
            lock_token=lock.lock_token,
            expires_at=lock.expires_at,
            locked_by=lock.locked_by.user.email,
        )
    except DashboardLock.DoesNotExist:
        raise HttpError(404, "No active lock found")


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
        logger.info(f"Unlocked dashboard {dashboard_id}")
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
        name=payload.name or payload.column_name,
        filter_type=payload.filter_type,
        schema_name=payload.schema_name,
        table_name=payload.table_name,
        column_name=payload.column_name,
        settings=payload.settings,
        order=payload.order,
    )

    return filter.to_json()


@dashboard_native_router.get(
    "/{dashboard_id}/filters/{filter_id}/", response=DashboardFilterResponse
)
# @has_permission(["can_view_dashboards"])
def get_filter(request, dashboard_id: int, filter_id: int):
    """Get a specific dashboard filter"""
    orguser: OrgUser = request.orguser

    try:
        dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
        filter: DashboardFilter = dashboard.filters.get(id=filter_id)
    except Dashboard.DoesNotExist:
        raise HttpError(404, "Dashboard not found")
    except DashboardFilter.DoesNotExist:
        raise HttpError(404, "Filter not found")

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
        filter: DashboardFilter = dashboard.filters.get(id=filter_id)
    except Dashboard.DoesNotExist:
        raise HttpError(404, "Dashboard not found")
    except DashboardFilter.DoesNotExist:
        raise HttpError(404, "Filter not found")

    # Update fields
    if payload.name is not None:
        filter.name = payload.name

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
