"""Chart service for business logic

This module encapsulates all chart-related business logic,
separating it from the API layer for better testability and maintainability.
"""

from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

from django.db.models import Q

from ddpui.models.visualization import Chart
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.dashboard import Dashboard, DashboardComponentType
from ddpui.core.charts.chart_validator import ChartValidator
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.chart_service")


@dataclass
class ChartData:
    """Data class for chart creation/update payloads"""

    title: str
    chart_type: str
    schema_name: str
    table_name: str
    extra_config: dict
    description: Optional[str] = None


class ChartServiceError(Exception):
    """Base exception for chart service errors"""

    def __init__(self, message: str, error_code: str = "CHART_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class ChartNotFoundError(ChartServiceError):
    """Raised when chart is not found"""

    def __init__(self, chart_id: int):
        super().__init__(f"Chart with id {chart_id} not found", "CHART_NOT_FOUND")
        self.chart_id = chart_id


class ChartValidationError(ChartServiceError):
    """Raised when chart validation fails"""

    def __init__(self, message: str):
        super().__init__(message, "VALIDATION_ERROR")


class ChartPermissionError(ChartServiceError):
    """Raised when user doesn't have permission"""

    def __init__(self, message: str = "Permission denied"):
        super().__init__(message, "PERMISSION_DENIED")


class ChartService:
    """Service class for chart-related operations"""

    @staticmethod
    def get_chart(chart_id: int, org: Org) -> Chart:
        """Get a chart by ID for an organization.

        Args:
            chart_id: The chart ID
            org: The organization

        Returns:
            Chart instance

        Raises:
            ChartNotFoundError: If chart doesn't exist or doesn't belong to org
        """
        try:
            return Chart.objects.get(id=chart_id, org=org)
        except Chart.DoesNotExist:
            raise ChartNotFoundError(chart_id)

    @staticmethod
    def list_charts(
        org: Org,
        page: int = 1,
        page_size: int = 10,
        search: Optional[str] = None,
        chart_type: Optional[str] = None,
    ) -> Tuple[List[Chart], int]:
        """List charts for an organization with pagination and filtering.

        Args:
            org: The organization
            page: Page number (1-indexed)
            page_size: Number of items per page
            search: Optional search term (searches title, description, schema_name, table_name)
            chart_type: Optional filter by chart type

        Returns:
            Tuple of (charts list, total count)
        """
        query = Q(org=org)

        if search:
            query &= (
                Q(title__icontains=search)
                | Q(description__icontains=search)
                | Q(schema_name__icontains=search)
                | Q(table_name__icontains=search)
            )

        if chart_type and chart_type != "all":
            query &= Q(chart_type=chart_type)

        queryset = Chart.objects.filter(query).order_by("-updated_at")
        total = queryset.count()

        # Apply pagination
        offset = (page - 1) * page_size
        charts = list(queryset[offset : offset + page_size])

        return charts, total

    @staticmethod
    def create_chart(data: ChartData, orguser: OrgUser) -> Chart:
        """Create a new chart.

        Args:
            data: Chart creation data
            orguser: The user creating the chart

        Returns:
            Created Chart instance

        Raises:
            ChartValidationError: If chart configuration is invalid
        """
        # Validate chart configuration
        is_valid, error_message = ChartValidator.validate_chart_config(
            chart_type=data.chart_type,
            extra_config=data.extra_config,
            schema_name=data.schema_name,
            table_name=data.table_name,
        )

        if not is_valid:
            raise ChartValidationError(error_message)

        chart = Chart.objects.create(
            title=data.title,
            description=data.description,
            chart_type=data.chart_type,
            schema_name=data.schema_name,
            table_name=data.table_name,
            extra_config=data.extra_config,
            created_by=orguser,
            last_modified_by=orguser,
            org=orguser.org,
        )

        logger.info(f"Created chart {chart.id} for org {orguser.org.id}")
        return chart

    @staticmethod
    def update_chart(
        chart_id: int,
        org: Org,
        orguser: OrgUser,
        title: Optional[str] = None,
        description: Optional[str] = None,
        chart_type: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        extra_config: Optional[dict] = None,
    ) -> Chart:
        """Update an existing chart.

        Args:
            chart_id: The chart ID
            org: The organization
            orguser: The user making the update
            title: Optional new title
            description: Optional new description
            chart_type: Optional new chart type
            schema_name: Optional new schema name
            table_name: Optional new table name
            extra_config: Optional new extra config

        Returns:
            Updated Chart instance

        Raises:
            ChartNotFoundError: If chart doesn't exist
            ChartValidationError: If updated configuration is invalid
        """
        chart = ChartService.get_chart(chart_id, org)

        # Prepare updated values for validation
        updated_chart_type = chart_type if chart_type is not None else chart.chart_type
        updated_extra_config = extra_config if extra_config is not None else chart.extra_config
        updated_schema_name = schema_name if schema_name is not None else chart.schema_name
        updated_table_name = table_name if table_name is not None else chart.table_name

        # Validate the updated configuration
        is_valid, error_message = ChartValidator.validate_for_update(
            existing_chart_type=chart.chart_type,
            new_chart_type=chart_type,
            extra_config=updated_extra_config,
            schema_name=updated_schema_name,
            table_name=updated_table_name,
        )

        if not is_valid:
            raise ChartValidationError(error_message)

        # Apply updates
        if title is not None:
            chart.title = title
        if description is not None:
            chart.description = description
        if chart_type is not None:
            chart.chart_type = chart_type
        if schema_name is not None:
            chart.schema_name = schema_name
        if table_name is not None:
            chart.table_name = table_name
        if extra_config is not None:
            chart.extra_config = extra_config

        chart.last_modified_by = orguser
        chart.save()

        logger.info(f"Updated chart {chart.id}")
        return chart

    @staticmethod
    def delete_chart(chart_id: int, org: Org, orguser: OrgUser) -> bool:
        """Delete a chart.

        Args:
            chart_id: The chart ID
            org: The organization
            orguser: The user deleting the chart

        Returns:
            True if deletion was successful

        Raises:
            ChartNotFoundError: If chart doesn't exist
            ChartPermissionError: If user doesn't have permission to delete
        """
        chart = ChartService.get_chart(chart_id, org)

        # Only allow deletion if the current user is the creator
        if chart.created_by != orguser:
            raise ChartPermissionError("You can only delete charts you created.")

        chart_title = chart.title
        chart.delete()

        logger.info(f"Deleted chart '{chart_title}' (id={chart_id}) by {orguser.user.email}")
        return True

    @staticmethod
    def bulk_delete_charts(chart_ids: List[int], org: Org, orguser: OrgUser) -> Dict[str, Any]:
        """Delete multiple charts.

        Args:
            chart_ids: List of chart IDs to delete
            org: The organization
            orguser: The user deleting the charts

        Returns:
            Dict with deletion results including counts and missing IDs
        """
        if not chart_ids:
            return {
                "deleted_count": 0,
                "requested_count": 0,
                "missing_ids": [],
            }

        # Get charts that belong to this org
        charts = Chart.objects.filter(id__in=chart_ids, org=org)
        found_ids = list(charts.values_list("id", flat=True))

        # Check if all requested charts were found
        missing_ids = set(chart_ids) - set(found_ids)
        if missing_ids:
            logger.warning(f"Charts not found or not accessible: {missing_ids}")

        # Delete the charts
        deleted_count = charts.delete()[0]

        logger.info(f"Bulk deleted {deleted_count} charts by {orguser.user.email}")

        return {
            "deleted_count": deleted_count,
            "requested_count": len(chart_ids),
            "missing_ids": list(missing_ids),
        }

    @staticmethod
    def get_chart_dashboards(chart_id: int, org: Org) -> List[Dict[str, Any]]:
        """Get list of dashboards that use a specific chart.

        Args:
            chart_id: The chart ID
            org: The organization

        Returns:
            List of dashboard info dictionaries
        """
        # Verify chart exists
        ChartService.get_chart(chart_id, org)

        # Find dashboards that have this chart in their components
        dashboards_with_chart = []
        dashboards = Dashboard.objects.filter(org=org)

        for dashboard in dashboards:
            if dashboard.components:
                for component_id, component in dashboard.components.items():
                    if (
                        component.get("type") == DashboardComponentType.CHART.value
                        and component.get("config", {}).get("chartId") == chart_id
                    ):
                        dashboards_with_chart.append(
                            {
                                "id": dashboard.id,
                                "title": dashboard.title,
                                "dashboard_type": dashboard.dashboard_type,
                            }
                        )
                        break  # Found chart in this dashboard

        return dashboards_with_chart

    @staticmethod
    def get_org_warehouse(org: Org) -> Optional[OrgWarehouse]:
        """Get the warehouse for an organization.

        Args:
            org: The organization

        Returns:
            OrgWarehouse instance or None if not configured
        """
        return OrgWarehouse.objects.filter(org=org).first()
