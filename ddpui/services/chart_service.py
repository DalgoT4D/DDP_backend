"""Chart service for business logic

This module encapsulates all chart-related business logic,
separating it from the API layer for better testability and maintainability.
"""

from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass

from django.db import transaction
from django.db.models import Q

from ddpui.core.ownership import can_delete_resource
from ddpui.models.visualization import Chart
from ddpui.models.favorite import FavoriteResourceType
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.dashboard import Dashboard, DashboardComponentType
from ddpui.services.favorite_service import FavoriteService
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
            return Chart.objects.select_related("created_by__user").get(id=chart_id, org=org)
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

        queryset = (
            Chart.objects.filter(query).select_related("created_by__user").order_by("-updated_at")
        )
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

        Note:
            The caller is expected to pass a validated payload — the API
            layer's `ChartCreate` schema enforces per-chart_type rules
            (required dimension_column, metric aggregation enum, customizations
            constraints, etc.) before reaching this method.
        """
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

        # Re-load with relations pre-fetched so building the API response
        # (chart.created_by.user.email) doesn't trigger extra lazy queries
        chart = Chart.objects.select_related("created_by__user").get(id=chart.id)
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

        Note:
            The caller is expected to pass a validated payload — the API
            layer's `ChartUpdate` schema runs per-chart_type validation when
            both `chart_type` and `extra_config` are present.
        """
        chart = ChartService.get_chart(chart_id, org)

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
    def delete_chart(chart_id: int, org: Org, orguser: OrgUser) -> str:
        """Delete a chart.

        Args:
            chart_id: The chart ID
            org: The organization
            orguser: The user deleting the chart

        Returns:
            The chart's title, so callers (e.g. the API layer's audit log)
            don't need a separate fetch of their own.

        Raises:
            ChartNotFoundError: If chart doesn't exist
            ChartPermissionError: If user doesn't have permission to delete
        """
        chart = ChartService.get_chart(chart_id, org)

        # Only allow deletion if the user is the owner or an admin
        if not can_delete_resource(orguser, chart):
            raise ChartPermissionError("Only the owner or an admin can delete this chart.")

        chart_title = chart.title
        with transaction.atomic():
            chart.delete()
            FavoriteService.remove_favorites_for_resource(FavoriteResourceType.CHART, chart_id)

        logger.info(f"Deleted chart '{chart_title}' (id={chart_id}) by {orguser.user.email}")
        return chart_title

    @staticmethod
    def bulk_delete_charts(chart_ids: List[int], org: Org, orguser: OrgUser) -> Dict[str, Any]:
        """Delete multiple charts.

        Args:
            chart_ids: List of chart IDs to delete
            org: The organization
            orguser: The user deleting the charts

        Returns:
            Dict with deletion results including counts, missing IDs, and
            forbidden IDs (charts the user may not delete — owner-or-admin rule)
        """
        if not chart_ids:
            return {
                "deleted_count": 0,
                "requested_count": 0,
                "missing_ids": [],
                "forbidden_ids": [],
                "deleted_titles": [],
            }

        # Get charts that belong to this org
        charts = Chart.objects.filter(id__in=chart_ids, org=org)
        found_ids = list(charts.values_list("id", flat=True))

        # Check if all requested charts were found
        missing_ids = set(chart_ids) - set(found_ids)
        if missing_ids:
            logger.warning(f"Charts not found or not accessible: {missing_ids}")

        # Same owner-or-admin rule as single delete: skip charts the user may not delete
        deletable = [chart for chart in charts if can_delete_resource(orguser, chart)]
        forbidden_ids = sorted(set(found_ids) - {chart.id for chart in deletable})
        if forbidden_ids:
            logger.warning(
                f"Charts not deletable by {orguser.user.email} (not owner or admin): {forbidden_ids}"
            )

        deletable_ids = [chart.id for chart in deletable]
        deleted_titles = [chart.title for chart in deletable]
        with transaction.atomic():
            deleted_count = Chart.objects.filter(id__in=deletable_ids).delete()[0]
            FavoriteService.remove_favorites_for_resources(
                FavoriteResourceType.CHART, deletable_ids
            )

        logger.info(f"Bulk deleted {deleted_count} charts by {orguser.user.email}")

        return {
            "deleted_count": deleted_count,
            "requested_count": len(chart_ids),
            "missing_ids": list(missing_ids),
            "forbidden_ids": forbidden_ids,
            "deleted_titles": deleted_titles,
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
            found = False
            for tab in dashboard.tabs or []:
                for component in (tab.get("components") or {}).values():
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
                        found = True
                        break
                if found:
                    break

        return dashboards_with_chart

    @staticmethod
    def favorite_chart(chart_id: int, org: Org, orguser: OrgUser) -> None:
        """Mark a chart as favorited by this user.

        Args:
            chart_id: The chart ID
            org: The organization
            orguser: The user favoriting the chart

        Raises:
            ChartNotFoundError: If chart doesn't exist or doesn't belong to org
        """
        ChartService.get_chart(chart_id, org)  # raises ChartNotFoundError if not in org
        FavoriteService.add_favorite(FavoriteResourceType.CHART, chart_id, orguser)

    @staticmethod
    def unfavorite_chart(chart_id: int, org: Org, orguser: OrgUser) -> None:
        """Remove this user's favorite on a chart, if any.

        Args:
            chart_id: The chart ID
            org: The organization
            orguser: The user unfavoriting the chart

        Raises:
            ChartNotFoundError: If chart doesn't exist or doesn't belong to org
        """
        ChartService.get_chart(chart_id, org)  # raises ChartNotFoundError if not in org
        FavoriteService.remove_favorite(FavoriteResourceType.CHART, chart_id, orguser)

    @staticmethod
    def get_favorited_chart_ids(chart_ids: List[int], orguser: OrgUser) -> Set[int]:
        """Return the subset of chart_ids this user has favorited.

        Args:
            chart_ids: Chart IDs to check
            orguser: The user whose favorites to look up

        Returns:
            Set of chart IDs favorited by this user
        """
        return FavoriteService.get_favorited_ids(FavoriteResourceType.CHART, chart_ids, orguser)

    @staticmethod
    def is_chart_favorited(chart_id: int, orguser: OrgUser) -> bool:
        """Whether this user has favorited a single chart.

        Args:
            chart_id: The chart ID
            orguser: The user whose favorites to look up

        Returns:
            True if this user has favorited the chart
        """
        return FavoriteService.is_favorited(FavoriteResourceType.CHART, chart_id, orguser)
