"""Native Dashboard API endpoints"""

import copy
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
    DashboardFilterType,
)
from ddpui.models.org_user import OrgUser
from ddpui.auth import has_permission
from ddpui.utils.custom_logger import CustomLogger
from ddpui.services.dashboard_service import delete_dashboard_safely
from ddpui.auth import SUPER_ADMIN_ROLE

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
    target_screen_size: Optional[str] = None
    layout_config: Optional[list[dict]] = None
    components: Optional[dict] = None
    # filters removed - managed via separate filter endpoints
    filter_layout: Optional[str] = None
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
    target_screen_size: str
    filter_layout: str
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


class FilterOptionResponse(Schema):
    """Schema for individual filter option"""

    label: str
    value: str
    count: Optional[int] = None


class FilterOptionsResponse(Schema):
    """Response schema for filter options"""

    options: List[FilterOptionResponse]
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
@has_permission(["can_view_dashboards"])
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
@has_permission(["can_view_dashboards"])
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
@has_permission(["can_create_dashboards"])
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

    # --- Custom logic for org default and landing dashboard (permission-driven) ---
    has_org_default = Dashboard.objects.filter(org=orguser.org, is_org_default=True).exists()

    # If no org default dashboard exists, assign based on permission
    if not has_org_default:
        if "can_manage_org_default_dashboard" in getattr(request, "permissions", []):
            dashboard.is_org_default = True
            dashboard.save(update_fields=["is_org_default"])
        else:
            # If user does not have permission and has no landing_dashboard, set this as landing_dashboard
            if not orguser.landing_dashboard:
                orguser.landing_dashboard = dashboard
                orguser.save(update_fields=["landing_dashboard"])

    logger.info(f"Created dashboard {dashboard.id} for org {orguser.org.id}")

    return get_dashboard_response(dashboard)


@dashboard_native_router.put("/{dashboard_id}/", response=DashboardResponse)
@has_permission(["can_edit_dashboards"])
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

    if payload.target_screen_size is not None:
        dashboard.target_screen_size = payload.target_screen_size

    if payload.layout_config is not None:
        dashboard.layout_config = payload.layout_config

    if payload.components is not None:
        dashboard.components = payload.components

    if payload.filter_layout is not None:
        dashboard.filter_layout = payload.filter_layout

    # Note: Filters are now managed independently of components
    # Filter CRUD operations are handled through separate endpoints
    # No need to sync filters with components as they are no longer part of the canvas

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
@has_permission(["can_delete_dashboards"])
def delete_dashboard(request, dashboard_id: int):
    """Delete a dashboard"""
    orguser: OrgUser = request.orguser

    try:
        dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
    except Dashboard.DoesNotExist:
        raise HttpError(404, "Dashboard not found")

    # Prevent deletion if dashboard is org default
    if dashboard.is_org_default:
        raise HttpError(403, "Cannot delete the organization's default dashboard.")

    # Only allow deletion if the current user is the creator
    if dashboard.created_by != orguser:
        raise HttpError(403, "You can only delete dashboards you created.")

    # Prevent deletion if dashboard is landing page for any user
    if OrgUser.objects.filter(landing_dashboard=dashboard).exists():
        raise HttpError(
            403, "Cannot delete a dashboard that is set as landing page for one or more users."
        )

    # Check if dashboard is locked
    if hasattr(dashboard, "lock") and dashboard.lock and not dashboard.lock.is_expired():
        raise HttpError(423, "Cannot delete a locked dashboard")

    # Use safe deletion function
    success, error_message = delete_dashboard_safely(dashboard_id, orguser)
    if not success:
        raise HttpError(400, error_message)

    return {"success": True}


@dashboard_native_router.post("/{dashboard_id}/duplicate/", response=DashboardResponse)
@has_permission(["can_create_dashboards"])
def duplicate_dashboard(request, dashboard_id: int):
    """Duplicate a dashboard with all its configurations and filters"""
    orguser: OrgUser = request.orguser

    # Get the original dashboard
    try:
        original_dashboard = Dashboard.objects.prefetch_related("filters").get(
            id=dashboard_id, org=orguser.org
        )
    except Dashboard.DoesNotExist:
        raise HttpError(404, "Dashboard not found")

    # Create a copy of the dashboard
    with transaction.atomic():
        # First create new dashboard WITHOUT layout_config and components (we'll update them later)
        new_dashboard = Dashboard.objects.create(
            title=f"Copy of {original_dashboard.title}",
            description=original_dashboard.description,
            dashboard_type=original_dashboard.dashboard_type,
            grid_columns=original_dashboard.grid_columns,
            target_screen_size=original_dashboard.target_screen_size,
            layout_config=[],  # Will be updated after filter duplication
            components={},  # Will be updated after filter duplication
            created_by=orguser,
            org=orguser.org,
            last_modified_by=orguser,
        )

        # Copy all filters and create ID mapping
        filter_id_mapping = {}  # old_filter_id -> new_filter_id

        for original_filter in original_dashboard.filters.all():
            new_filter = DashboardFilter.objects.create(
                dashboard=new_dashboard,
                name=original_filter.name,
                filter_type=original_filter.filter_type,
                schema_name=original_filter.schema_name,
                table_name=original_filter.table_name,
                column_name=original_filter.column_name,
                settings=original_filter.settings,
                order=original_filter.order,
            )
            filter_id_mapping[str(original_filter.id)] = str(new_filter.id)

        # Now update layout_config and components with new filter IDs

        # Deep copy the original data to avoid modifying it
        new_layout_config = copy.deepcopy(original_dashboard.layout_config or [])
        new_components = copy.deepcopy(original_dashboard.components or {})

        # Update layout_config: change component IDs from "filter-{old_id}" to "filter-{new_id}"
        for layout_item in new_layout_config:
            item_id = layout_item.get("i", "")
            if item_id.startswith("filter-"):
                # Extract old filter ID and replace with new one
                old_filter_id = item_id.replace("filter-", "")
                if old_filter_id in filter_id_mapping:
                    new_filter_id = filter_id_mapping[old_filter_id]
                    layout_item["i"] = f"filter-{new_filter_id}"

        # Update components: change component keys and filterId references
        updated_components = {}
        for component_id, component_data in new_components.items():
            new_component_id = component_id
            new_component_data = copy.deepcopy(component_data)

            # If this is a filter component
            if component_id.startswith("filter-"):
                old_filter_id = component_id.replace("filter-", "")
                if old_filter_id in filter_id_mapping:
                    new_filter_id = filter_id_mapping[old_filter_id]
                    new_component_id = f"filter-{new_filter_id}"

                    # Update the filterId reference in the component config
                    if (
                        "config" in new_component_data
                        and "filterId" in new_component_data["config"]
                    ):
                        new_component_data["config"]["filterId"] = int(new_filter_id)

            updated_components[new_component_id] = new_component_data

        # Update the dashboard with the corrected layout_config and components
        new_dashboard.layout_config = new_layout_config
        new_dashboard.components = updated_components
        new_dashboard.save()

        logger.info(
            f"Duplicated dashboard {dashboard_id} as {new_dashboard.id} for org {orguser.org.id}"
        )

    return get_dashboard_response(new_dashboard)


# Dashboard Lock endpoints
@dashboard_native_router.post("/{dashboard_id}/lock/", response=LockResponse)
@has_permission(["can_edit_dashboards"])
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
@has_permission(["can_edit_dashboards"])
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
@has_permission(["can_edit_dashboards"])
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
@has_permission(["can_edit_dashboards"])
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
@has_permission(["can_view_dashboards"])
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
@has_permission(["can_edit_dashboards"])
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
@has_permission(["can_edit_dashboards"])
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
@has_permission(["can_view_dashboards"])
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


# ===== Dashboard Sharing Endpoints =====


class DashboardShareToggle(Schema):
    """Schema for toggling dashboard sharing"""

    is_public: bool


class DashboardShareResponse(Schema):
    """Schema for share response"""

    is_public: bool
    public_url: Optional[str] = None
    public_share_token: Optional[str] = None
    message: str


class DashboardShareStatus(Schema):
    """Schema for share status response"""

    is_public: bool
    public_url: Optional[str] = None
    public_access_count: int
    last_public_accessed: Optional[datetime] = None
    public_shared_at: Optional[datetime] = None


@dashboard_native_router.put("/{dashboard_id}/share/")
@has_permission(["can_share_dashboards"])
def toggle_dashboard_sharing(request, dashboard_id: int, payload: DashboardShareToggle):
    """Toggle public sharing for a dashboard"""
    orguser: OrgUser = request.orguser

    try:
        dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
    except Dashboard.DoesNotExist:
        raise HttpError(404, "Dashboard not found")

    # Check permissions - only dashboard creator or org admin can modify sharing
    if dashboard.created_by != orguser:
        # TODO: Add org admin check if needed
        raise HttpError(403, "Only dashboard creators can modify sharing settings")

    is_public = payload.is_public

    if is_public:
        # Generate token if making public
        if not dashboard.public_share_token:
            import secrets

            dashboard.public_share_token = secrets.token_urlsafe(48)
        dashboard.public_shared_at = timezone.now()
        dashboard.public_disabled_at = None
    else:
        # Disable public sharing but keep token for audit
        dashboard.public_disabled_at = timezone.now()

    dashboard.is_public = is_public
    dashboard.save()

    # Build response
    response_data = {
        "is_public": dashboard.is_public,
        "message": f'Dashboard {"made public" if is_public else "made private"}',
    }

    if dashboard.is_public and dashboard.public_share_token:
        # Generate the full public URL
        from django.conf import settings

        # Use FRONTEND_URL_V2 for webapp_v2, fallback to FRONTEND_URL, then localhost
        FRONTEND_URL_V2 = getattr(settings, "FRONTEND_URL_V2", None)
        frontend_url = FRONTEND_URL_V2 or getattr(settings, "FRONTEND_URL", "http://localhost:3001")
        response_data[
            "public_url"
        ] = f"{frontend_url}/share/dashboard/{dashboard.public_share_token}"
        response_data["public_share_token"] = dashboard.public_share_token

    # Audit logging
    action = "enabled_public_sharing" if is_public else "disabled_public_sharing"
    logger.info(
        f"Dashboard {dashboard_id} sharing {action} by user {orguser.user.email}, token: {dashboard.public_share_token}"
    )

    return DashboardShareResponse(**response_data)


@dashboard_native_router.get("/{dashboard_id}/share/")
@has_permission(["can_view_dashboards"])
def get_dashboard_sharing_status(request, dashboard_id: int):
    """Get dashboard sharing status"""
    orguser: OrgUser = request.orguser

    try:
        dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
    except Dashboard.DoesNotExist:
        raise HttpError(404, "Dashboard not found")

    # Check permissions - only dashboard creator or org admin can view sharing status
    if dashboard.created_by != orguser:
        # TODO: Add org admin check if needed
        raise HttpError(403, "Only dashboard creators can view sharing settings")

    response_data = {
        "is_public": dashboard.is_public,
        "public_access_count": dashboard.public_access_count,
        "last_public_accessed": dashboard.last_public_accessed,
        "public_shared_at": dashboard.public_shared_at,
    }

    if dashboard.is_public and dashboard.public_share_token:
        from django.conf import settings

        # Use FRONTEND_URL_V2 for webapp_v2, fallback to FRONTEND_URL, then localhost
        FRONTEND_URL_V2 = getattr(settings, "FRONTEND_URL_V2", None)
        frontend_url = FRONTEND_URL_V2 or getattr(settings, "FRONTEND_URL", "http://localhost:3001")
        response_data[
            "public_url"
        ] = f"{frontend_url}/share/dashboard/{dashboard.public_share_token}"

    return DashboardShareStatus(**response_data)


# =============================================================================
# Landing Page Management APIs
# =============================================================================


class LandingPageResponse(Schema):
    """Response schema for landing page operations"""

    success: bool
    message: str = ""


@dashboard_native_router.post(
    "/landing-page/set-personal/{dashboard_id}", response=LandingPageResponse
)
@has_permission(["can_view_dashboards", "can_create_dashboards"])
def set_personal_landing_dashboard(request, dashboard_id: int):
    """Set a dashboard as user's personal landing page"""
    orguser: OrgUser = request.orguser

    # Check if dashboard exists and belongs to user's org
    try:
        dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
    except Dashboard.DoesNotExist:
        raise HttpError(404, "Dashboard not found")

    # Set as personal landing page
    orguser.landing_dashboard = dashboard
    orguser.save()

    logger.info(
        f"User {orguser.user.email} set dashboard {dashboard.title} as personal landing page"
    )
    return LandingPageResponse(success=True, message="Dashboard set as personal landing page")


@dashboard_native_router.delete("/landing-page/remove-personal", response=LandingPageResponse)
@has_permission(["can_view_dashboards", "can_create_dashboards"])
def remove_personal_landing_dashboard(request):
    """Remove user's personal landing page preference"""
    orguser: OrgUser = request.orguser

    if orguser.landing_dashboard:
        previous_dashboard = orguser.landing_dashboard.title
        orguser.landing_dashboard = None
        orguser.save()

        logger.info(
            f"User {orguser.user.email} removed personal landing page: {previous_dashboard}"
        )
        return LandingPageResponse(success=True, message="Personal landing page preference removed")
    else:
        return LandingPageResponse(success=True, message="No personal landing page was set")


@dashboard_native_router.post(
    "/landing-page/set-org-default/{dashboard_id}", response=LandingPageResponse
)
@has_permission(["can_manage_org_default_dashboard"])
def set_org_default_dashboard(request, dashboard_id: int):
    """Set a dashboard as organization's default landing page (Admin only)"""
    orguser: OrgUser = request.orguser

    # Check if dashboard exists and belongs to user's org
    try:
        dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
    except Dashboard.DoesNotExist:
        raise HttpError(404, "Dashboard not found")

    with transaction.atomic():
        # Remove previous org default
        Dashboard.objects.filter(org=orguser.org, is_org_default=True).update(is_org_default=False)

        # Set new org default
        dashboard.is_org_default = True
        dashboard.save()

    logger.info(
        f"User {orguser.user.email} set dashboard {dashboard.title} as org default landing page"
    )
    return LandingPageResponse(
        success=True, message="Dashboard set as organization default landing page"
    )


@dashboard_native_router.delete("/landing-page/remove-org-default", response=LandingPageResponse)
@has_permission(["can_manage_org_default_dashboard"])
def remove_org_default_dashboard(request):
    """Remove organization's default landing page (Admin only)"""
    orguser: OrgUser = request.orguser

    org_default = Dashboard.objects.filter(org=orguser.org, is_org_default=True).first()
    if org_default:
        org_default.is_org_default = False
        org_default.save()

        logger.info(
            f"User {orguser.user.email} removed org default landing page: {org_default.title}"
        )
        return LandingPageResponse(
            success=True, message="Organization default landing page removed"
        )
    else:
        return LandingPageResponse(
            success=True, message="No organization default landing page was set"
        )


@dashboard_native_router.get("/landing-page/resolve", response=dict)
@has_permission(["can_view_dashboards"])
def resolve_user_landing_page(request):
    """Resolve which dashboard should be the user's landing page"""
    orguser: OrgUser = request.orguser

    # 1. Check personal preference first
    if orguser.landing_dashboard:
        return {
            "dashboard_id": orguser.landing_dashboard.id,
            "dashboard_title": orguser.landing_dashboard.title,
            "dashboard_type": orguser.landing_dashboard.dashboard_type,
            "source": "personal",
        }

    # 2. Check org default
    org_default = Dashboard.objects.filter(org=orguser.org, is_org_default=True).first()
    if org_default:
        return {
            "dashboard_id": org_default.id,
            "dashboard_title": org_default.title,
            "dashboard_type": org_default.dashboard_type,
            "source": "org_default",
        }

    # 3. No landing page set
    return {"dashboard_id": None, "dashboard_title": None, "dashboard_type": None, "source": "none"}
