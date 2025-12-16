"""Dashboard service for business logic

This module encapsulates all dashboard-related business logic,
separating it from the API layer for better testability and maintainability.
"""

from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import json
import uuid

from django.core.cache import cache
from django.db.models import Q
from django.utils import timezone
from sqlalchemy import text, distinct, column
from sqlalchemy.dialects import postgresql

from ddpui.models.dashboard import (
    Dashboard,
    DashboardFilter,
    DashboardLock,
    DashboardComponentType,
    DashboardFilterType,
)
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.visualization import Chart
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from ddpui.core.charts.charts_service import (
    get_warehouse_client,
    execute_chart_query,
    transform_data_for_chart,
)
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.redis_client import RedisClient
from ddpui.datainsights.query_builder import AggQueryBuilder
from ddpui.schemas.dashboard_schema import DashboardUpdate, FilterUpdate

logger = CustomLogger("ddpui.dashboard_service")


# =============================================================================
# Custom Exceptions
# =============================================================================


class DashboardServiceError(Exception):
    """Base exception for dashboard service errors"""

    def __init__(self, message: str, error_code: str = "DASHBOARD_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class DashboardNotFoundError(DashboardServiceError):
    """Raised when dashboard is not found"""

    def __init__(self, dashboard_id: int):
        super().__init__(f"Dashboard with id {dashboard_id} not found", "DASHBOARD_NOT_FOUND")
        self.dashboard_id = dashboard_id


class DashboardLockedError(DashboardServiceError):
    """Raised when dashboard is locked by another user"""

    def __init__(self, locked_by_email: str):
        super().__init__(f"Dashboard is locked by {locked_by_email}", "DASHBOARD_LOCKED")
        self.locked_by_email = locked_by_email


class DashboardPermissionError(DashboardServiceError):
    """Raised when user doesn't have permission"""

    def __init__(self, message: str = "Permission denied"):
        super().__init__(message, "PERMISSION_DENIED")


class FilterNotFoundError(DashboardServiceError):
    """Raised when filter is not found"""

    def __init__(self, filter_id: int):
        super().__init__(f"Filter with id {filter_id} not found", "FILTER_NOT_FOUND")
        self.filter_id = filter_id


class FilterValidationError(DashboardServiceError):
    """Raised when filter validation fails"""

    def __init__(self, message: str):
        super().__init__(message, "FILTER_VALIDATION_ERROR")


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class DashboardData:
    """Data class for dashboard creation"""

    title: str
    description: Optional[str] = None
    grid_columns: int = 12


@dataclass
class FilterData:
    """Data class for filter creation"""

    filter_type: str
    schema_name: str
    table_name: str
    column_name: str
    name: Optional[str] = None
    settings: Optional[dict] = None
    order: int = 0


@dataclass
class LockInfo:
    """Data class for lock information"""

    lock_token: str
    expires_at: datetime
    locked_by_email: str


class DashboardService:
    """Service class for dashboard-related operations"""

    # =========================================================================
    # CRUD Operations
    # =========================================================================

    @staticmethod
    def get_dashboard(dashboard_id: int, org: Org) -> Dashboard:
        """Get a dashboard by ID for an organization.

        Args:
            dashboard_id: The dashboard ID
            org: The organization

        Returns:
            Dashboard instance

        Raises:
            DashboardNotFoundError: If dashboard doesn't exist or doesn't belong to org
        """
        try:
            return Dashboard.objects.prefetch_related("filters").get(id=dashboard_id, org=org)
        except Dashboard.DoesNotExist:
            raise DashboardNotFoundError(dashboard_id)

    @staticmethod
    def get_dashboard_response(dashboard: Dashboard) -> Dict[str, Any]:
        """Convert dashboard model to response dict with migration support.

        This method handles:
        - Migration of old filter format to new format
        - Adding lock information
        - Formatting filters data

        Args:
            dashboard: The dashboard instance

        Returns:
            Dictionary containing dashboard response data
        """
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

    @staticmethod
    def list_dashboards(
        org: Org,
        dashboard_type: Optional[str] = None,
        search: Optional[str] = None,
        is_published: Optional[bool] = None,
    ) -> List[Dashboard]:
        """List dashboards for an organization with filtering.

        Args:
            org: The organization
            dashboard_type: Optional filter by dashboard type
            search: Optional search term for title/description
            is_published: Optional filter by published status

        Returns:
            List of Dashboard instances
        """
        query = Q(org=org)

        if dashboard_type:
            query &= Q(dashboard_type=dashboard_type)

        if search:
            query &= Q(title__icontains=search) | Q(description__icontains=search)

        if is_published is not None:
            query &= Q(is_published=is_published)

        return list(Dashboard.objects.filter(query).order_by("-updated_at"))

    @staticmethod
    def create_dashboard(data: DashboardData, orguser: OrgUser) -> Dashboard:
        """Create a new dashboard.

        Args:
            data: Dashboard creation data
            orguser: The user creating the dashboard

        Returns:
            Created Dashboard instance
        """
        dashboard = Dashboard.objects.create(
            title=data.title,
            description=data.description,
            grid_columns=data.grid_columns,
            created_by=orguser,
            org=orguser.org,
            last_modified_by=orguser,
        )

        logger.info(f"Created dashboard {dashboard.id} for org {orguser.org.id}")
        return dashboard

    @staticmethod
    def update_dashboard(
        dashboard_id: int,
        org: Org,
        orguser: OrgUser,
        data: DashboardUpdate,
    ) -> Dashboard:
        """Update an existing dashboard.

        Args:
            dashboard_id: The dashboard ID
            org: The organization
            orguser: The user making the update
            data: Dashboard update data

        Returns:
            Updated Dashboard instance

        Raises:
            DashboardNotFoundError: If dashboard doesn't exist
            DashboardLockedError: If dashboard is locked by another user
        """
        dashboard = DashboardService.get_dashboard(dashboard_id, org)

        # Check if dashboard is locked by another user
        if hasattr(dashboard, "lock") and dashboard.lock:
            if not dashboard.lock.is_expired() and dashboard.lock.locked_by != orguser:
                raise DashboardLockedError(dashboard.lock.locked_by.user.email)

        # Apply updates
        if data.title is not None:
            dashboard.title = data.title
        if data.description is not None:
            dashboard.description = data.description
        if data.grid_columns is not None:
            dashboard.grid_columns = data.grid_columns
        if data.target_screen_size is not None:
            dashboard.target_screen_size = data.target_screen_size
        if data.layout_config is not None:
            dashboard.layout_config = data.layout_config
        if data.components is not None:
            dashboard.components = data.components
        if data.filter_layout is not None:
            dashboard.filter_layout = data.filter_layout
        if data.is_published is not None:
            dashboard.is_published = data.is_published
            if data.is_published:
                dashboard.published_at = timezone.now()

        dashboard.last_modified_by = orguser
        dashboard.save()

        # Auto-refresh lock if dashboard is locked by current user
        if hasattr(dashboard, "lock") and dashboard.lock:
            if not dashboard.lock.is_expired() and dashboard.lock.locked_by == orguser:
                dashboard.lock.expires_at = timezone.now() + timedelta(minutes=2)
                dashboard.lock.save()
                logger.info(f"Auto-refreshed lock for dashboard {dashboard_id} during save")

        logger.info(f"Updated dashboard {dashboard.id}")
        return dashboard

    @staticmethod
    def delete_dashboard(
        dashboard_id: int, org: Org, orguser: OrgUser, force: bool = False
    ) -> bool:
        """Delete a dashboard with safety checks.

        Args:
            dashboard_id: The dashboard ID
            org: The organization
            orguser: The user deleting the dashboard
            force: If True, bypass some safety checks

        Returns:
            True if deletion was successful

        Raises:
            DashboardNotFoundError: If dashboard doesn't exist
            DashboardPermissionError: If user doesn't have permission
            DashboardLockedError: If dashboard is locked
        """
        dashboard = DashboardService.get_dashboard(dashboard_id, org)

        # Check if this is org default
        if dashboard.is_org_default and not force:
            raise DashboardPermissionError("Cannot delete the organization's default dashboard.")

        # Only allow deletion if the current user is the creator
        if dashboard.created_by != orguser:
            raise DashboardPermissionError("You can only delete dashboards you created.")

        # Check if dashboard is landing page for any user
        if OrgUser.objects.filter(landing_dashboard=dashboard).exists():
            raise DashboardPermissionError(
                "Cannot delete a dashboard that is set as landing page for one or more users."
            )

        # Check if dashboard is locked
        if hasattr(dashboard, "lock") and dashboard.lock and not dashboard.lock.is_expired():
            raise DashboardLockedError(dashboard.lock.locked_by.user.email)

        # Use safe deletion
        success, error_message = delete_dashboard_safely(dashboard_id, orguser)
        if not success:
            raise DashboardServiceError(error_message)

        return True

    # =========================================================================
    # Lock Operations
    # =========================================================================

    @staticmethod
    def lock_dashboard(dashboard_id: int, org: Org, orguser: OrgUser) -> LockInfo:
        """Lock a dashboard for editing.

        Args:
            dashboard_id: The dashboard ID
            org: The organization
            orguser: The user locking the dashboard

        Returns:
            LockInfo with lock details

        Raises:
            DashboardNotFoundError: If dashboard doesn't exist
            DashboardLockedError: If dashboard is locked by another user
        """
        dashboard = DashboardService.get_dashboard(dashboard_id, org)

        # Check if already locked
        try:
            lock = dashboard.lock
            if not lock.is_expired():
                if lock.locked_by == orguser:
                    # Refresh lock
                    lock.expires_at = timezone.now() + timedelta(minutes=2)
                    lock.save()
                    return LockInfo(
                        lock_token=lock.lock_token,
                        expires_at=lock.expires_at,
                        locked_by_email=lock.locked_by.user.email,
                    )
                else:
                    raise DashboardLockedError(lock.locked_by.user.email)
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
            expires_at=timezone.now() + timedelta(minutes=2),
        )

        return LockInfo(
            lock_token=lock.lock_token,
            expires_at=lock.expires_at,
            locked_by_email=lock.locked_by.user.email,
        )

    @staticmethod
    def unlock_dashboard(dashboard_id: int, org: Org, orguser: OrgUser) -> bool:
        """Unlock a dashboard.

        Args:
            dashboard_id: The dashboard ID
            org: The organization
            orguser: The user unlocking the dashboard

        Returns:
            True if unlock was successful

        Raises:
            DashboardNotFoundError: If dashboard doesn't exist
            DashboardPermissionError: If user doesn't own the lock
        """
        dashboard = DashboardService.get_dashboard(dashboard_id, org)

        try:
            lock = dashboard.lock
            if lock.locked_by != orguser:
                raise DashboardPermissionError("You can only unlock your own locks")
            lock.delete()
            logger.info(f"Unlocked dashboard {dashboard_id}")
        except DashboardLock.DoesNotExist:
            pass

        return True

    @staticmethod
    def refresh_lock(dashboard_id: int, org: Org, orguser: OrgUser) -> LockInfo:
        """Refresh a dashboard lock to extend its expiry.

        Args:
            dashboard_id: The dashboard ID
            org: The organization
            orguser: The user refreshing the lock

        Returns:
            LockInfo with updated lock details

        Raises:
            DashboardNotFoundError: If dashboard doesn't exist
            DashboardPermissionError: If user doesn't own the lock
            DashboardServiceError: If lock has expired or doesn't exist
        """
        dashboard = DashboardService.get_dashboard(dashboard_id, org)

        try:
            lock = dashboard.lock
            if lock.is_expired():
                raise DashboardServiceError("Lock has expired", "LOCK_EXPIRED")
            if lock.locked_by != orguser:
                raise DashboardPermissionError("You can only refresh your own locks")

            # Refresh lock
            lock.expires_at = timezone.now() + timedelta(minutes=2)
            lock.save()

            logger.info(f"Refreshed lock for dashboard {dashboard_id}")

            return LockInfo(
                lock_token=lock.lock_token,
                expires_at=lock.expires_at,
                locked_by_email=lock.locked_by.user.email,
            )
        except DashboardLock.DoesNotExist:
            raise DashboardServiceError("No active lock found", "NO_LOCK")

    # =========================================================================
    # Filter Operations
    # =========================================================================

    @staticmethod
    def create_filter(dashboard_id: int, org: Org, data: FilterData) -> DashboardFilter:
        """Create a filter for a dashboard.

        Args:
            dashboard_id: The dashboard ID
            org: The organization
            data: Filter creation data

        Returns:
            Created DashboardFilter instance

        Raises:
            DashboardNotFoundError: If dashboard doesn't exist
            FilterValidationError: If filter type is invalid
        """
        dashboard = DashboardService.get_dashboard(dashboard_id, org)

        # Validate filter type
        if data.filter_type not in [ft.value for ft in DashboardFilterType]:
            raise FilterValidationError(f"Invalid filter type: {data.filter_type}")

        filter_obj = DashboardFilter.objects.create(
            dashboard=dashboard,
            name=data.name or data.column_name,
            filter_type=data.filter_type,
            schema_name=data.schema_name,
            table_name=data.table_name,
            column_name=data.column_name,
            settings=data.settings or {},
            order=data.order,
        )

        logger.info(f"Created filter {filter_obj.id} for dashboard {dashboard_id}")
        return filter_obj

    @staticmethod
    def get_filter(dashboard_id: int, filter_id: int, org: Org) -> DashboardFilter:
        """Get a specific filter from a dashboard.

        Args:
            dashboard_id: The dashboard ID
            filter_id: The filter ID
            org: The organization

        Returns:
            DashboardFilter instance

        Raises:
            DashboardNotFoundError: If dashboard doesn't exist
            FilterNotFoundError: If filter doesn't exist
        """
        dashboard = DashboardService.get_dashboard(dashboard_id, org)

        try:
            return dashboard.filters.get(id=filter_id)
        except DashboardFilter.DoesNotExist:
            raise FilterNotFoundError(filter_id)

    @staticmethod
    def update_filter(
        dashboard_id: int,
        filter_id: int,
        org: Org,
        data: FilterUpdate,
    ) -> DashboardFilter:
        """Update a filter in a dashboard.

        Args:
            dashboard_id: The dashboard ID
            filter_id: The filter ID
            org: The organization
            data: Filter update data

        Returns:
            Updated DashboardFilter instance

        Raises:
            DashboardNotFoundError: If dashboard doesn't exist
            FilterNotFoundError: If filter doesn't exist
            FilterValidationError: If filter type is invalid
        """
        filter_obj = DashboardService.get_filter(dashboard_id, filter_id, org)

        if data.name is not None:
            filter_obj.name = data.name

        if data.filter_type is not None:
            if data.filter_type not in [ft.value for ft in DashboardFilterType]:
                raise FilterValidationError(f"Invalid filter type: {data.filter_type}")
            filter_obj.filter_type = data.filter_type

        if data.schema_name is not None:
            filter_obj.schema_name = data.schema_name

        if data.table_name is not None:
            filter_obj.table_name = data.table_name

        if data.column_name is not None:
            filter_obj.column_name = data.column_name

        if data.settings is not None:
            filter_obj.settings = data.settings

        if data.order is not None:
            filter_obj.order = data.order

        filter_obj.save()
        logger.info(f"Updated filter {filter_id} in dashboard {dashboard_id}")
        return filter_obj

    @staticmethod
    def delete_filter(dashboard_id: int, filter_id: int, org: Org) -> bool:
        """Delete a filter from a dashboard.

        Args:
            dashboard_id: The dashboard ID
            filter_id: The filter ID
            org: The organization

        Returns:
            True if deletion was successful

        Raises:
            DashboardNotFoundError: If dashboard doesn't exist
            FilterNotFoundError: If filter doesn't exist
        """
        filter_obj = DashboardService.get_filter(dashboard_id, filter_id, org)
        filter_obj.delete()
        logger.info(f"Deleted filter {filter_id} from dashboard {dashboard_id}")
        return True

    # =========================================================================
    # Business Logic Operations (existing methods)
    # =========================================================================

    @staticmethod
    def apply_filters(dashboard_id: int, filters: Dict[str, Any], orguser) -> Dict[str, Any]:
        """Apply filters to all chart components in dashboard

        Args:
            dashboard_id: Dashboard ID
            filters: Dictionary of filter values keyed by filter ID
            orguser: OrgUser instance

        Returns:
            Dictionary with chart data for each chart component
        """
        try:
            dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
        except Dashboard.DoesNotExist:
            raise ValueError("Dashboard not found")

        # Get org warehouse
        org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
        if not org_warehouse:
            raise ValueError("No warehouse configured for organization")

        # Get all chart components from dashboard
        components = dashboard.components
        chart_results = {}

        for component_id, component_config in components.items():
            if component_config.get("type") == DashboardComponentType.CHART.value:
                chart_id = component_config.get("config", {}).get("chartId")
                if chart_id:
                    try:
                        # Get chart configuration
                        chart = Chart.objects.get(id=chart_id, org=orguser.org)

                        # Apply filters to chart query
                        filtered_data = DashboardService._apply_filters_to_chart(
                            chart, dashboard.filters.all(), filters, org_warehouse
                        )

                        chart_results[component_id] = {"success": True, "data": filtered_data}
                    except Exception as e:
                        logger.error(f"Error applying filters to chart {chart_id}: {str(e)}")
                        chart_results[component_id] = {"success": False, "error": str(e)}

        return chart_results

    @staticmethod
    def _apply_filters_to_chart(
        chart: Chart,
        dashboard_filters: List[DashboardFilter],
        filter_values: Dict[str, Any],
        org_warehouse: OrgWarehouse,
    ) -> Dict[str, Any]:
        """Apply dashboard filters to a chart query

        Args:
            chart: Chart instance
            dashboard_filters: List of dashboard filter configurations
            filter_values: User-selected filter values
            org_warehouse: Organization warehouse

        Returns:
            Filtered chart data
        """
        # Get chart configuration
        extra_config = chart.extra_config or {}

        # Build WHERE conditions from filters
        where_conditions = []

        for filter in dashboard_filters:
            filter_id = str(filter.id)
            if filter_id in filter_values:
                value = filter_values[filter_id]

                # Check if this filter applies to the chart's table
                if (
                    filter.schema_name == chart.schema_name
                    and filter.table_name == chart.table_name
                ):
                    if filter.filter_type == DashboardFilterType.VALUE.value:
                        # Handle categorical filters
                        if isinstance(value, list):
                            # Multiple values
                            condition = (
                                f"{filter.column_name} IN ({','.join([repr(v) for v in value])})"
                            )
                        else:
                            # Single value
                            condition = f"{filter.column_name} = {repr(value)}"
                        where_conditions.append(condition)

                    elif filter.filter_type == DashboardFilterType.NUMERICAL.value:
                        # Handle numerical filters
                        settings = filter.settings or {}
                        if settings.get("isRange", True):
                            # Range filter
                            if isinstance(value, dict):
                                min_val = value.get("min")
                                max_val = value.get("max")
                                if min_val is not None and max_val is not None:
                                    condition = (
                                        f"{filter.column_name} BETWEEN {min_val} AND {max_val}"
                                    )
                                    where_conditions.append(condition)
                        else:
                            # Single value filter
                            if value is not None:
                                condition = f"{filter.column_name} = {value}"
                                where_conditions.append(condition)

        # Modify chart query with filters
        modified_config = extra_config.copy()

        # Add WHERE conditions to existing ones
        existing_where = modified_config.get("where_conditions", [])
        if isinstance(existing_where, str):
            existing_where = [existing_where] if existing_where else []

        all_conditions = existing_where + where_conditions
        if all_conditions:
            modified_config["where_conditions"] = all_conditions

        # Execute the query with filters
        payload = {
            "chart_type": chart.chart_type,
            "schema_name": chart.schema_name,
            "table_name": chart.table_name,
            "extra_config": modified_config,
        }

        # Get chart data
        # Note: This uses a simplified approach for filtered data
        # Full implementation would need to use the charts API directly
        return {"data": [], "message": "Filtered chart data - implementation pending"}

    @staticmethod
    def check_lock_status(dashboard_id: int) -> Dict[str, Any]:
        """Check if dashboard is locked for editing

        Args:
            dashboard_id: Dashboard ID

        Returns:
            Lock status information
        """
        try:
            dashboard = Dashboard.objects.get(id=dashboard_id)
            if hasattr(dashboard, "lock") and dashboard.lock:
                lock = dashboard.lock
                if not lock.is_expired():
                    return {
                        "is_locked": True,
                        "locked_by": lock.locked_by.user.email,
                        "locked_at": lock.locked_at.isoformat(),
                        "expires_at": lock.expires_at.isoformat(),
                    }
        except Dashboard.DoesNotExist:
            pass

        return {"is_locked": False}

    @staticmethod
    def generate_filter_options(
        schema: str, table: str, column: str, org_warehouse: OrgWarehouse, limit: int = 100
    ) -> List[Dict[str, str]]:
        """Generate filter options for categorical filters

        Args:
            schema: Schema name
            table: Table name
            column: Column name
            org_warehouse: Organization warehouse
            limit: Maximum number of options to return

        Returns:
            List of filter options with label and value
        """
        # Check cache first
        cache_key = f"filter_options:{org_warehouse.org.id}:{schema}:{table}:{column}"
        cached_options = cache.get(cache_key)

        if cached_options is not None:
            return cached_options

        try:
            # Get warehouse client
            warehouse_client = get_warehouse_client(org_warehouse)

            # Build query for distinct values
            if org_warehouse.wtype == "postgres":
                query = f"""
                    SELECT DISTINCT "{column}" as value
                    FROM "{schema}"."{table}"
                    WHERE "{column}" IS NOT NULL
                    ORDER BY value
                    LIMIT {limit}
                """
            elif org_warehouse.wtype == "bigquery":
                query = f"""
                    SELECT DISTINCT `{column}` as value
                    FROM `{org_warehouse.bq_location}.{schema}.{table}`
                    WHERE `{column}` IS NOT NULL
                    ORDER BY value
                    LIMIT {limit}
                """
            else:
                raise ValueError(f"Unsupported warehouse type: {org_warehouse.wtype}")

            # Execute query
            result = warehouse_client.execute(query)

            # Extract values and format as objects with label and value
            # This matches what the frontend expects
            options = [
                {"label": str(row.get("value", "")), "value": str(row.get("value", ""))}
                for row in result
                if row.get("value") is not None
            ]

            # Cache for 5 minutes
            cache.set(cache_key, options, 300)

            return options
        except Exception as e:
            logger.error(f"Error generating filter options: {str(e)}")
            return []

    @staticmethod
    def get_dashboard_charts(dashboard_id: int, orguser) -> List[Dict[str, Any]]:
        """Get all charts referenced in a dashboard

        Args:
            dashboard_id: Dashboard ID
            orguser: OrgUser instance

        Returns:
            List of chart information
        """
        try:
            dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
        except Dashboard.DoesNotExist:
            raise ValueError("Dashboard not found")

        components = dashboard.components
        chart_ids = []

        # Extract chart IDs from components
        for component_id, component_config in components.items():
            if component_config.get("type") == DashboardComponentType.CHART.value:
                chart_id = component_config.get("config", {}).get("chartId")
                if chart_id:
                    chart_ids.append(chart_id)

        # Fetch chart details
        charts = Chart.objects.filter(id__in=chart_ids, org=orguser.org)

        return [
            {
                "id": chart.id,
                "title": chart.title,
                "chart_type": chart.chart_type,
                "schema_name": chart.schema_name,
                "table_name": chart.table_name,
            }
            for chart in charts
        ]

    @staticmethod
    def validate_dashboard_config(dashboard: Dashboard) -> Dict[str, Any]:
        """Validate dashboard configuration

        Args:
            dashboard: Dashboard instance

        Returns:
            Validation result with any errors found
        """
        errors = []
        warnings = []

        # Validate layout config
        layout_config = dashboard.layout_config or {}
        components = dashboard.components or {}

        # Check all components have layout entries
        for component_id in components:
            if component_id not in layout_config:
                warnings.append(f"Component {component_id} has no layout configuration")

        # Check all layout entries have components
        for layout_id in layout_config:
            if layout_id not in components:
                errors.append(f"Layout entry {layout_id} has no corresponding component")

        # Validate component configurations
        for component_id, component_config in components.items():
            component_type = component_config.get("type")

            if not component_type:
                errors.append(f"Component {component_id} has no type specified")
                continue

            if component_type not in [ct.value for ct in DashboardComponentType]:
                errors.append(f"Component {component_id} has invalid type: {component_type}")

            # Validate chart components
            if component_type == DashboardComponentType.CHART.value:
                config = component_config.get("config", {})
                if not config.get("chartId"):
                    errors.append(f"Chart component {component_id} has no chartId")

        # Validate filters
        for filter in dashboard.filters.all():
            if filter.filter_type not in [ft.value for ft in DashboardFilterType]:
                errors.append(f"Filter {filter.id} has invalid type: {filter.filter_type}")

        return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}


def delete_dashboard_safely(dashboard_id: int, orguser: OrgUser) -> tuple[bool, str]:
    """
    Safely delete a dashboard with protection logic for landing pages.

    Args:
        dashboard_id: ID of dashboard to delete
        orguser: OrgUser performing the deletion

    Returns:
        Tuple of (success, error_message)
    """
    try:
        dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
    except Dashboard.DoesNotExist:
        return False, "Dashboard not found"

    # Check if this is the last dashboard in the org
    dashboard_count = Dashboard.objects.filter(org=orguser.org).count()
    if dashboard_count <= 1:
        return False, "Cannot delete the last dashboard in the organization"

    # If deleting org default, auto-assign new default
    if dashboard.is_org_default:
        new_default = Dashboard.objects.filter(org=orguser.org).exclude(id=dashboard_id).first()
        if new_default:
            new_default.is_org_default = True
            new_default.save()
            logger.info(f"Auto-assigned new org default dashboard: {new_default.title}")

    # Clear any user landing page preferences pointing to this dashboard
    OrgUser.objects.filter(landing_dashboard=dashboard).update(landing_dashboard=None)

    # Delete the dashboard
    dashboard_title = dashboard.title
    dashboard.delete()

    logger.info(f"Dashboard '{dashboard_title}' deleted by {orguser.user.email}")
    return True, ""
