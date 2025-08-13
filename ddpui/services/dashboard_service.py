"""Dashboard service for business logic"""

from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import json

from django.core.cache import cache
from sqlalchemy import text, distinct, column
from sqlalchemy.dialects import postgresql

from ddpui.models.dashboard import (
    Dashboard,
    DashboardFilter,
    DashboardComponentType,
    DashboardFilterType,
)
from ddpui.models.visualization import Chart
from ddpui.models.org import OrgWarehouse
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from ddpui.core.charts.charts_service import (
    get_warehouse_client,
    execute_chart_query,
    transform_data_for_chart,
)
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.redis_client import RedisClient
from ddpui.datainsights.query_builder import AggQueryBuilder

logger = CustomLogger("ddpui.dashboard_service")


class DashboardService:
    """Service class for dashboard-related operations"""

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
            "computation_type": chart.computation_type,
            "schema_name": chart.schema_name,
            "table_name": chart.table_name,
            "extra_config": modified_config,
        }

        # Get chart data
        query_result = execute_chart_query(payload, org_warehouse)
        chart_data = transform_data_for_chart(
            payload, query_result.data, query_result.columns, query_result.computation_type
        )

        return chart_data

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
    ) -> List[str]:
        """Generate filter options for categorical filters

        Args:
            schema: Schema name
            table: Table name
            column: Column name
            org_warehouse: Organization warehouse
            limit: Maximum number of options to return

        Returns:
            List of distinct values
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

            # Extract values - result is a list of dictionaries
            options = [str(row.get("value", "")) for row in result if row.get("value") is not None]

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
