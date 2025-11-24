"""
Dashboard Chat API endpoints for AI-powered dashboard analysis.
Provides context-aware chat functionality for dashboard insights.
"""

import json
import asyncio
import time
import uuid
from typing import Optional, List, Dict, Any, Union
from django.utils import timezone
from django.http import JsonResponse, StreamingHttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.utils.decorators import method_decorator
from django.views import View
from ninja import Router, Schema
from ninja.security import HttpBearer

from ddpui.auth import has_permission
from ddpui.utils.custom_logger import CustomLogger
from ddpui.core.ai.factory import get_default_ai_provider
from ddpui.core.ai.interfaces import AIMessage
from ddpui.core.ai.data_intelligence import DataIntelligenceService
from ddpui.core.ai.query_executor import DynamicQueryExecutor, QueryExecutionResult
from ddpui.core.ai.smart_chat_processor import SmartChatProcessor, MessageIntent
from ddpui.models.dashboard import Dashboard
from ddpui.models.org_settings import OrgSettings
from ddpui.models.ai_chat_logging import AIChatLog, AIChatMetering

# Remove unused import - orguser is accessed via request.orguser

logger = CustomLogger("ddpui.api.dashboard_chat")

# Ninja router for Dashboard Chat API
router = Router()


# Dashboard chat API routes are working - see OpenAPI spec at /api/docs


# Pydantic schemas
class DashboardChatMessage(Schema):
    role: str  # "user", "assistant", "system"
    content: str
    timestamp: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class DashboardChatRequest(Schema):
    messages: List[DashboardChatMessage]
    include_data: bool = False  # Whether to include actual data or just schema
    max_rows: int = 100  # Maximum rows to include if data is enabled
    selected_chart_id: Optional[str] = None
    stream: bool = False
    provider_type: Optional[str] = None


class DashboardContextRequest(Schema):
    include_data: bool = False
    max_rows: int = 100


class DashboardChatResponse(Schema):
    content: str
    usage: Optional[Dict[str, int]] = None
    context_included: bool
    data_included: bool
    metadata: Optional[Dict[str, Any]] = None


def log_ai_chat_conversation(
    org,
    user,
    user_prompt,
    ai_response,
    request_timestamp,
    response_timestamp,
    dashboard_id=None,
    chart_id=None,
    session_id=None,
):
    """
    Log complete AI chat conversation (request-response pair) when ai_logging_acknowledged is enabled.

    Args:
        org: Organization object
        user: User object
        user_prompt: The user's original question/prompt
        ai_response: The AI's complete response
        request_timestamp: When the user sent the request
        response_timestamp: When the AI response was completed
        dashboard_id: Dashboard ID if applicable
        chart_id: Chart ID if applicable
        session_id: Chat session identifier
    """
    try:
        # Check if logging is enabled for this organization
        org_settings = OrgSettings.objects.filter(org=org).first()
        if not org_settings or not org_settings.ai_logging_acknowledged:
            return  # Skip logging if not acknowledged

        # Create the log entry
        AIChatLog.objects.create(
            org=org,
            user=user,
            dashboard_id=dashboard_id,
            chart_id=chart_id,
            session_id=session_id or str(uuid.uuid4()),
            user_prompt=user_prompt[:10000],  # Limit content to 10k characters
            ai_response=ai_response[:10000],  # Limit content to 10k characters
            request_timestamp=request_timestamp,
            response_timestamp=response_timestamp,
        )
        logger.info(
            f"Logged conversation for org {org.slug} user {user.email} session {session_id}"
        )

    except Exception as e:
        logger.error(f"Error logging AI chat conversation: {e}")


def log_ai_chat_metering(
    org,
    user,
    model_used,
    prompt_tokens,
    completion_tokens,
    response_time_ms,
    include_data,
    max_rows=None,
    dashboard_id=None,
    chart_id=None,
    session_id=None,
):
    """
    Log AI chat usage metrics for billing and monitoring.
    This is always logged regardless of user logging preferences.

    Args:
        org: Organization object
        user: User object
        model_used: AI model identifier (e.g., 'openai-gpt-4')
        prompt_tokens: Number of tokens in prompt
        completion_tokens: Number of tokens in completion
        response_time_ms: Response time in milliseconds
        include_data: Whether data was included in the request
        max_rows: Maximum rows if data sharing enabled
        dashboard_id: Dashboard ID if applicable
        chart_id: Chart ID if applicable
        session_id: Chat session identifier
    """
    try:
        total_tokens = prompt_tokens + completion_tokens

        AIChatMetering.objects.create(
            org=org,
            user=user,
            dashboard_id=dashboard_id,
            chart_id=chart_id,
            session_id=session_id or str(uuid.uuid4()),
            model_used=model_used,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
            response_time_ms=response_time_ms,
            include_data=include_data,
            max_rows=max_rows if include_data else None,
        )
        logger.info(
            f"Logged AI metering for org {org.slug}: {total_tokens} tokens, {response_time_ms}ms"
        )

    except Exception as e:
        logger.error(f"Error logging AI chat metering: {e}")


class DashboardContextAnalyzer:
    """
    Analyzes dashboard context to provide AI with relevant information.
    Enhanced with DataIntelligenceService for richer data context.
    """

    def __init__(
        self, dashboard_id: int, orguser_obj, include_data: bool = False, max_rows: int = 100
    ):
        self.dashboard_id = dashboard_id
        self.orguser = orguser_obj
        self.include_data = include_data
        self.max_rows = max_rows
        self.logger = CustomLogger("dashboard_context")
        self.data_intelligence = DataIntelligenceService()

    def get_dashboard_context(self) -> Dict[str, Any]:
        """
        Get comprehensive dashboard context for AI analysis.

        Returns:
            Dictionary containing dashboard metadata, charts, and optionally data
        """
        try:
            # Get dashboard metadata
            dashboard = Dashboard.objects.filter(id=self.dashboard_id, org=self.orguser.org).first()

            if not dashboard:
                return {"error": "Dashboard not found or access denied"}

            context = {
                "dashboard": {
                    "id": dashboard.id,
                    "title": dashboard.title,
                    "description": dashboard.description or "",
                    "dashboard_type": dashboard.dashboard_type,
                    "created_at": dashboard.created_at.isoformat(),
                    "updated_at": dashboard.updated_at.isoformat(),
                    "filter_layout": dashboard.filter_layout or {},
                    "target_screen_size": dashboard.target_screen_size or "desktop",
                },
                "charts": [],
                "filters": [],
                "data_sources": [],
                "summary": {
                    "total_charts": 0,
                    "chart_types": {},
                    "data_included": self.include_data,
                    "max_rows": self.max_rows if self.include_data else 0,
                },
            }

            # Get dashboard configuration and charts
            if dashboard.dashboard_type == "native":
                context.update(self._get_native_dashboard_context(dashboard))
            else:
                context.update(self._get_superset_dashboard_context(dashboard))

            # Enhanced: Add rich data intelligence context
            context.update(self._get_data_intelligence_context(dashboard, context))

            return context

        except Exception as e:
            self.logger.error(f"Error getting dashboard context: {e}")
            return {"error": f"Failed to analyze dashboard context: {str(e)}"}

    def _get_native_dashboard_context(self, dashboard) -> Dict[str, Any]:
        """Get context for native dashboard type."""
        from ddpui.models.dashboard import DashboardFilter

        context_data = {"charts": [], "filters": [], "data_sources": []}

        try:
            # Get dashboard filters
            filters = DashboardFilter.objects.filter(dashboard=dashboard)
            for filter_obj in filters:
                filter_info = {
                    "id": filter_obj.id,
                    "name": filter_obj.name,
                    "filter_type": filter_obj.filter_type,
                    "schema_name": getattr(filter_obj, "schema_name", None),
                    "table_name": getattr(filter_obj, "table_name", None),
                    "column_name": getattr(filter_obj, "column_name", None),
                }
                context_data["filters"].append(filter_info)

            # Get charts from dashboard layout_config and components
            layout_config = getattr(dashboard, "layout_config", [])
            components = getattr(dashboard, "components", {})

            if isinstance(layout_config, str):
                layout_config = json.loads(layout_config)
            if isinstance(components, str):
                components = json.loads(components)

            chart_types = {}

            for layout_item in layout_config:
                component_id = layout_item.get("i")
                if not component_id or component_id not in components:
                    continue

                component = components[component_id]
                if component.get("type") != "chart":
                    continue

                try:
                    chart_config = component.get("config", {})
                    chart_id = chart_config.get("chartId")

                    if not chart_id:
                        continue

                    # Get chart metadata
                    chart_info = {
                        "id": str(chart_id),
                        "component_id": component_id,
                        "type": chart_config.get("chart_type", "unknown"),
                        "title": chart_config.get("title", f"Chart {chart_id}"),
                        "position": {
                            "x": layout_item.get("x", 0),
                            "y": layout_item.get("y", 0),
                            "w": layout_item.get("w", 12),
                            "h": layout_item.get("h", 8),
                        },
                    }

                    # Count chart types
                    chart_type = chart_info["type"]
                    chart_types[chart_type] = chart_types.get(chart_type, 0) + 1

                    # Get chart schema/data if requested
                    if self.include_data:
                        chart_info.update(self._get_chart_data_context(chart_id))
                    else:
                        chart_info.update(self._get_chart_schema_context(chart_id))

                    context_data["charts"].append(chart_info)

                except Exception as e:
                    self.logger.error(f"Error processing chart {component_id}: {e}")
                    continue

            context_data["summary"] = {
                "total_charts": len(context_data["charts"]),
                "chart_types": chart_types,
                "data_included": self.include_data,
                "max_rows": self.max_rows if self.include_data else 0,
            }

        except Exception as e:
            self.logger.error(f"Error getting native dashboard context: {e}")

        return context_data

    def _get_superset_dashboard_context(self, dashboard) -> Dict[str, Any]:
        """Get context for Superset dashboard type."""
        # For Superset dashboards, we have limited access to internal structure
        return {
            "charts": [],
            "filters": [],
            "data_sources": [],
            "superset_info": {
                "external_dashboard": True,
                "superset_id": getattr(dashboard, "superset_id", None),
            },
            "summary": {
                "total_charts": 0,
                "chart_types": {"superset": 1},
                "data_included": False,
                "max_rows": 0,
            },
        }

    def _get_chart_schema_context(self, chart_id: int) -> Dict[str, Any]:
        """Get chart schema information without actual data."""
        try:
            from ddpui.models.visualization import Chart
            from ddpui.models.org import OrgWarehouse
            from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory

            chart = Chart.objects.filter(id=chart_id, org=self.orguser.org).first()

            if not chart:
                return {"schema": {}, "sample_data": None}

            # Extract schema information from chart configuration
            extra_config = chart.extra_config or {}

            # Get actual table schema from database
            actual_columns = []
            try:
                # Get warehouse connection
                org_warehouse = OrgWarehouse.objects.filter(org=self.orguser.org).first()
                if org_warehouse:
                    warehouse = WarehouseFactory.get_warehouse_client(org_warehouse)
                    table_columns_list = warehouse.get_table_columns(
                        chart.schema_name, chart.table_name
                    )

                    # Extract column information with types
                    for col in table_columns_list:
                        if isinstance(col, dict):
                            actual_columns.append(
                                {
                                    "name": col.get("name", "unknown"),
                                    "data_type": col.get("data_type", "unknown"),
                                    "nullable": col.get("nullable", True),
                                    "translated_type": col.get("translated_type", "string"),
                                }
                            )

                    self.logger.info(
                        f"Chart {chart_id} schema: found {len(actual_columns)} columns from database"
                    )
                else:
                    self.logger.warning(f"Chart {chart_id} schema: no warehouse found")
            except Exception as schema_error:
                self.logger.error(f"Chart {chart_id} schema fetch error: {schema_error}")

            # Build comprehensive schema info
            schema_info = {
                "schema_name": chart.schema_name,
                "table_name": chart.table_name,
                "chart_type": chart.chart_type,
                "computation_type": chart.computation_type,
                # Use actual columns if available, fallback to config
                "table_columns": actual_columns if actual_columns else [],
                "chart_columns": extra_config.get("columns", []),
                "metrics": extra_config.get("metrics", []),
                "dimensions": extra_config.get("dimensions", []),
                "filters": extra_config.get("filters", []),
                "aggregate_functions": extra_config.get("aggregate_functions", []),
            }

            return {
                "schema": schema_info,
                "sample_data": None,
            }
        except Exception as e:
            self.logger.error(f"Error getting chart schema for {chart_id}: {e}")
            return {"schema": {}, "sample_data": None}

    def _get_chart_data_context(self, chart_id: int) -> Dict[str, Any]:
        """Get chart data and schema information using enhanced approach with actual chart execution."""
        self.logger.info(f"=== Starting enhanced data fetch for chart {chart_id} ===")

        try:
            # Get schema first
            schema_info = self._get_chart_schema_context(chart_id)

            # Enhanced approach: Try to get actual chart data first, fallback to raw data
            chart_data = self._get_actual_chart_data(chart_id)

            if chart_data and chart_data.get("rows") and len(chart_data["rows"]) > 0:
                self.logger.info(
                    f"Chart {chart_id} - Using actual chart data: {len(chart_data['rows'])} rows"
                )
                sample_data = chart_data
            else:
                self.logger.warning(
                    f"Chart {chart_id} - Chart data unavailable, falling back to raw table data"
                )
                sample_data = self._get_simple_table_data(chart_id)

            return {"schema": schema_info["schema"], "sample_data": sample_data}
        except Exception as e:
            self.logger.error(f"Error getting chart data for {chart_id}: {e}")
            return {"schema": {}, "sample_data": None}

    def _get_actual_chart_data(self, chart_id: int) -> Dict[str, Any]:
        """Get actual chart data using the chart's configuration and execution logic."""
        try:
            from ddpui.models.visualization import Chart
            from ddpui.models.org import OrgWarehouse
            from ddpui.core.charts import charts_service
            from ddpui.schemas.chart_schema import ChartDataPayload

            # Get chart object
            chart = Chart.objects.filter(id=chart_id, org=self.orguser.org).first()
            if not chart:
                self.logger.error(f"Chart {chart_id} not found in database")
                return {"rows": [], "total_rows": 0, "columns": [], "error": "Chart not found"}

            self.logger.info(f"Chart {chart_id} found: {chart.title} ({chart.chart_type})")

            # Get warehouse
            org_warehouse = OrgWarehouse.objects.filter(org=self.orguser.org).first()
            if not org_warehouse:
                self.logger.error(f"No warehouse found for org {self.orguser.org.slug}")
                return {
                    "rows": [],
                    "total_rows": 0,
                    "columns": [],
                    "error": "No warehouse configured",
                }

            # Build chart data payload from the chart configuration
            extra_config = chart.extra_config or {}

            # Create ChartDataPayload from chart configuration
            payload = ChartDataPayload(
                chart_type=chart.chart_type,
                schema_name=chart.schema_name,
                table_name=chart.table_name,
                computation_type=chart.computation_type,
                x_axis=extra_config.get("x_axis"),
                y_axis=extra_config.get("y_axis"),
                dimension_col=extra_config.get("dimension_col"),
                metrics=extra_config.get("metrics", []),
                dimensions=extra_config.get("dimensions", []),
                filters=extra_config.get("filters", []),
                sort=extra_config.get("sort", []),
                time_grain=extra_config.get("time_grain"),
                # Limit data for AI context
                extra_config={
                    "pagination": {
                        "enabled": True,
                        "page_size": min(self.max_rows, 50),  # Limit for AI context
                    }
                },
            )

            # Use the same logic as the charts API to generate chart data
            self.logger.info(
                f"Chart {chart_id} - Executing with payload: computation_type={payload.computation_type}"
            )

            # Get warehouse client
            warehouse = charts_service.get_warehouse_client(org_warehouse)

            # Build query using chart service
            query_builder = charts_service.build_chart_query(payload, org_warehouse)

            # Execute query
            results = charts_service.execute_query(warehouse, query_builder)

            if results:
                # Format results for AI context
                columns = list(results[0].keys()) if results else []

                self.logger.info(
                    f"Chart {chart_id} - Successfully executed chart query: {len(results)} rows, {len(columns)} columns"
                )

                return {
                    "rows": results,
                    "total_rows": len(results),
                    "columns": [{"name": col, "type": "chart_result"} for col in columns],
                    "source": "chart_execution",
                    "chart_type": chart.chart_type,
                    "computation_type": chart.computation_type,
                }
            else:
                self.logger.warning(f"Chart {chart_id} - Chart execution returned no results")
                return {
                    "rows": [],
                    "total_rows": 0,
                    "columns": [],
                    "source": "chart_execution_empty",
                }

        except Exception as e:
            self.logger.error(f"Chart {chart_id} - Error executing actual chart data: {e}")
            return {
                "rows": [],
                "total_rows": 0,
                "columns": [],
                "error": f"Chart execution failed: {str(e)}",
                "source": "chart_execution_failed",
            }

    def _get_simple_table_data(self, chart_id: int) -> Dict[str, Any]:
        """Get simple raw table data for any chart."""
        try:
            from ddpui.models.visualization import Chart
            from ddpui.models.org import OrgWarehouse
            from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
            from sqlalchemy import text

            # Get chart object
            chart = Chart.objects.filter(id=chart_id, org=self.orguser.org).first()
            if not chart:
                self.logger.error(f"Chart {chart_id} not found in database")
                return {"rows": [], "total_rows": 0, "columns": [], "error": "Chart not found"}

            self.logger.info(f"Chart {chart_id} found: {chart.title}")
            self.logger.info(f"Chart {chart_id} table: {chart.schema_name}.{chart.table_name}")

            # Get warehouse
            org_warehouse = OrgWarehouse.objects.filter(org=self.orguser.org).first()
            if not org_warehouse:
                self.logger.error(f"No warehouse found for org {self.orguser.org.slug}")
                return {
                    "rows": [],
                    "total_rows": 0,
                    "columns": [],
                    "error": "No warehouse configured",
                }

            # Get warehouse client
            warehouse = WarehouseFactory.get_warehouse_client(org_warehouse)

            # First, get the table columns to know what we're working with
            try:
                table_columns_list = warehouse.get_table_columns(
                    chart.schema_name, chart.table_name
                )
                if not table_columns_list:
                    self.logger.warning(
                        f"Chart {chart_id} no columns found for table {chart.schema_name}.{chart.table_name}"
                    )
                    return {
                        "rows": [],
                        "total_rows": 0,
                        "columns": [],
                        "error": "No columns found in table",
                    }

                # Extract column names from dict format
                column_names = [
                    col["name"]
                    for col in table_columns_list
                    if isinstance(col, dict) and "name" in col
                ]
                if not column_names:
                    self.logger.error(
                        f"Chart {chart_id} could not extract column names from: {table_columns_list[:5]}"
                    )
                    return {
                        "rows": [],
                        "total_rows": 0,
                        "columns": [],
                        "error": "Could not extract column names from table metadata",
                    }

                self.logger.info(
                    f"Chart {chart_id} found {len(column_names)} columns: {column_names[:10]}"
                )

                # Try multiple query approaches
                limit = min(self.max_rows, 20)  # Start with smaller limit
                select_columns = column_names[:5]  # Start with just 5 columns

                # Approach 1: Simple SELECT with quoted identifiers
                try:
                    columns_str = ", ".join([f'"{col}"' for col in select_columns])
                    query1 = f'SELECT {columns_str} FROM "{chart.schema_name}"."{chart.table_name}" LIMIT {limit}'

                    self.logger.info(f"Chart {chart_id} trying approach 1: {query1}")
                    results = warehouse.execute(text(query1))

                    if results and len(results) > 0:
                        rows = self._format_query_results(results, select_columns)
                        self.logger.info(f"Chart {chart_id} approach 1 success: {len(rows)} rows")
                        # DEBUG: Log actual data being fetched
                        self.logger.info(
                            f"Chart {chart_id} sample data preview: {rows[:2] if rows else 'No rows'}"
                        )
                        return {
                            "rows": rows,
                            "total_rows": len(rows),
                            "columns": [{"name": col, "type": "string"} for col in select_columns],
                            "source": "simple_query",
                        }
                except Exception as e1:
                    self.logger.warning(f"Chart {chart_id} approach 1 failed: {e1}")

                # Approach 2: SELECT without quotes
                try:
                    columns_str = ", ".join(select_columns)
                    query2 = f"SELECT {columns_str} FROM {chart.schema_name}.{chart.table_name} LIMIT {limit}"

                    self.logger.info(f"Chart {chart_id} trying approach 2: {query2}")
                    results = warehouse.execute(text(query2))

                    if results and len(results) > 0:
                        rows = self._format_query_results(results, select_columns)
                        self.logger.info(f"Chart {chart_id} approach 2 success: {len(rows)} rows")
                        return {
                            "rows": rows,
                            "total_rows": len(rows),
                            "columns": [{"name": col, "type": "string"} for col in select_columns],
                            "source": "unquoted_query",
                        }
                except Exception as e2:
                    self.logger.warning(f"Chart {chart_id} approach 2 failed: {e2}")

                # Approach 3: SELECT COUNT(*) to verify table exists and has data
                try:
                    count_query = f'SELECT COUNT(*) FROM "{chart.schema_name}"."{chart.table_name}"'
                    self.logger.info(f"Chart {chart_id} trying count query: {count_query}")
                    count_result = warehouse.execute(text(count_query))

                    if count_result and len(count_result) > 0:
                        total_rows = (
                            count_result[0]["count"]
                            if "count" in count_result[0]
                            else count_result[0].get("COUNT(*)", 0)
                        )
                        self.logger.info(f"Chart {chart_id} table has {total_rows} total rows")

                        if total_rows == 0:
                            return {
                                "rows": [],
                                "total_rows": 0,
                                "columns": [
                                    {"name": col, "type": "string"} for col in select_columns
                                ],
                                "source": "empty_table",
                                "info": f"Table exists but is empty (0 rows)",
                            }
                except Exception as e3:
                    self.logger.warning(f"Chart {chart_id} count query failed: {e3}")

                # Approach 4: Very simple SELECT *
                try:
                    simple_query = (
                        f'SELECT * FROM "{chart.schema_name}"."{chart.table_name}" LIMIT 5'
                    )
                    self.logger.info(f"Chart {chart_id} trying SELECT *: {simple_query}")
                    results = warehouse.execute(text(simple_query))

                    if results and len(results) > 0:
                        # Use column names from results keys
                        if isinstance(results[0], dict):
                            actual_columns = list(results[0].keys())[
                                :5
                            ]  # Use first 5 columns from results
                        else:
                            actual_columns = select_columns  # Fallback to original selection

                        rows = self._format_query_results(results, actual_columns)
                        self.logger.info(f"Chart {chart_id} SELECT * success: {len(rows)} rows")
                        return {
                            "rows": rows,
                            "total_rows": len(rows),
                            "columns": [{"name": col, "type": "string"} for col in actual_columns],
                            "source": "select_star",
                        }
                except Exception as e4:
                    self.logger.error(f"Chart {chart_id} SELECT * failed: {e4}")

                # All approaches failed - return column info with detailed error
                self.logger.error(f"Chart {chart_id} all query approaches failed")
                return {
                    "rows": [],
                    "total_rows": 0,
                    "columns": [{"name": col, "type": "string"} for col in column_names[:10]],
                    "error": f"Could not fetch data but found {len(column_names)} columns",
                    "debug_info": {
                        "table_columns_found": len(column_names),
                        "sample_columns": column_names[:10],
                        "schema_name": chart.schema_name,
                        "table_name": chart.table_name,
                    },
                }

            except Exception as outer_error:
                self.logger.error(f"Chart {chart_id} outer query block failed: {outer_error}")
                return {
                    "rows": [],
                    "total_rows": 0,
                    "columns": [],
                    "error": f"Failed to connect to table: {str(outer_error)}",
                }

        except Exception as e:
            self.logger.error(f"Chart {chart_id} simple data fetch failed: {e}")
            return {
                "rows": [],
                "total_rows": 0,
                "columns": [],
                "error": f"Data fetch error: {str(e)}",
            }

    def _format_query_results(self, results, columns):
        """Format raw query results into list of dicts."""
        try:
            rows = []
            for row in results:
                row_dict = {}

                # Handle different result formats
                if isinstance(row, dict):
                    # Results are already in dict format (most common case)
                    for col_name in columns:
                        if col_name in row:
                            value = row[col_name]
                            # Handle special values
                            if value is None:
                                value = "NULL"
                            elif isinstance(value, (int, float, str, bool)):
                                value = value
                            else:
                                value = str(value)
                            row_dict[col_name] = value
                        else:
                            row_dict[col_name] = "N/A"
                else:
                    # Results are in tuple/list format (fallback)
                    for i, col_name in enumerate(columns):
                        if i < len(row):
                            value = row[i]
                            # Handle special values
                            if value is None:
                                value = "NULL"
                            elif isinstance(value, (int, float, str, bool)):
                                value = value
                            else:
                                value = str(value)
                            row_dict[col_name] = value
                        else:
                            row_dict[col_name] = "N/A"

                rows.append(row_dict)
            return rows
        except Exception as e:
            self.logger.error(f"Error formatting query results: {e}")
            self.logger.error(f"Results sample: {results[:2] if results else 'None'}")
            self.logger.error(f"Columns: {columns}")
            return []

    def _get_data_intelligence_context(
        self, dashboard, existing_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Enhanced context using DataIntelligenceService for richer AI understanding.

        This provides comprehensive data catalog information that helps AI understand:
        - Available tables and their business context
        - Column types and sample data
        - Relationships between data sources
        - Query patterns and possibilities
        """
        enhanced_context = {
            "data_catalog": None,
            "ai_data_context": "",
            "enhanced_analysis": {
                "tables_analyzed": 0,
                "business_context_available": False,
                "sample_data_included": False,
                "query_suggestions_available": False,
            },
        }

        try:
            # Only enhance if data sharing is enabled
            org_settings = OrgSettings.objects.filter(org=self.orguser.org).first()
            if not org_settings or not org_settings.ai_data_sharing_enabled:
                self.logger.info(
                    f"Data intelligence enhancement skipped - data sharing disabled for org {self.orguser.org.slug}"
                )
                return enhanced_context

            self.logger.info(
                f"Building enhanced data intelligence context for dashboard {self.dashboard_id}"
            )

            # Get data catalog for the organization
            catalog = self.data_intelligence.get_org_data_catalog(self.orguser.org)

            if catalog.total_tables == 0:
                self.logger.warning(
                    f"No tables found in data catalog for org {self.orguser.org.slug}"
                )
                return enhanced_context

            # Build AI-friendly data context focused on this dashboard
            ai_data_context = self.data_intelligence.build_ai_data_context(
                org=self.orguser.org, dashboard_id=self.dashboard_id
            )

            # Update enhanced context
            enhanced_context.update(
                {
                    "data_catalog": {
                        "total_tables": catalog.total_tables,
                        "total_charts": catalog.total_charts,
                        "tables_with_data": len([t for t in catalog.tables.values() if t.columns]),
                        "generated_at": catalog.generated_at.isoformat(),
                    },
                    "ai_data_context": ai_data_context,
                    "enhanced_analysis": {
                        "tables_analyzed": catalog.total_tables,
                        "business_context_available": any(
                            table.business_description for table in catalog.tables.values()
                        ),
                        "sample_data_included": any(
                            any(col.sample_values for col in table.columns)
                            for table in catalog.tables.values()
                        ),
                        "query_suggestions_available": True,
                    },
                }
            )

            # Add table summaries to existing context data_sources
            if "data_sources" in existing_context:
                for table_key, table_info in catalog.tables.items():
                    enhanced_context.setdefault("data_sources", []).append(
                        {
                            "table_key": table_key,
                            "schema_name": table_info.schema_name,
                            "table_name": table_info.table_name,
                            "business_description": table_info.business_description,
                            "column_count": len(table_info.columns),
                            "charts_using_table": len(table_info.charts_using_table),
                            "dimension_columns": [
                                col.name for col in table_info.columns if col.is_dimension
                            ],
                            "metric_columns": [
                                col.name for col in table_info.columns if col.is_metric
                            ],
                        }
                    )

            self.logger.info(
                f"Enhanced context built: {catalog.total_tables} tables, "
                f"{len([t for t in catalog.tables.values() if t.columns])} with column data"
            )

        except Exception as e:
            self.logger.error(f"Error building enhanced data intelligence context: {e}")
            # Don't fail the entire context building - just log and continue
            enhanced_context["enhanced_analysis"]["error"] = str(e)

        return enhanced_context


@router.post("/{dashboard_id}/context")
@has_permission(["can_view_dashboards"])
def get_dashboard_context(request, dashboard_id: int, payload: DashboardContextRequest):
    """
    Get dashboard context for AI analysis.
    This provides the AI with information about the dashboard structure and optionally data.
    """
    try:
        orguser_obj = request.orguser
        if not orguser_obj:
            return JsonResponse({"error": "Unauthorized"}, status=401)

        analyzer = DashboardContextAnalyzer(
            dashboard_id=dashboard_id,
            orguser_obj=orguser_obj,
            include_data=payload.include_data,
            max_rows=payload.max_rows,
        )

        context = analyzer.get_dashboard_context()

        if "error" in context:
            return JsonResponse(context, status=400)

        return JsonResponse(
            {"dashboard_id": dashboard_id, "context": context, "timestamp": time.time()}
        )

    except Exception as e:
        logger.error(f"Error getting dashboard context: {e}")
        return JsonResponse({"error": "Internal server error"}, status=500)


@router.post("/{dashboard_id}/chat")
@has_permission(["can_view_dashboards"])
def dashboard_chat(request, dashboard_id: int, payload: DashboardChatRequest):
    """
    Chat with AI about a specific dashboard.
    Provides context-aware responses based on dashboard structure and data.
    """
    try:
        orguser_obj = request.orguser
        if not orguser_obj:
            return JsonResponse({"error": "Unauthorized"}, status=401)

        # Get AI provider
        provider = get_default_ai_provider()

        # Get dashboard context
        analyzer = DashboardContextAnalyzer(
            dashboard_id=dashboard_id,
            orguser_obj=orguser_obj,
            include_data=payload.include_data,
            max_rows=payload.max_rows,
        )

        context = analyzer.get_dashboard_context()
        if "error" in context:
            return JsonResponse(context, status=400)

        # Build AI messages with context
        ai_messages = []

        # Add system message with dashboard context
        system_prompt = _build_dashboard_system_prompt(context, payload.selected_chart_id)

        # DEBUG: Log what data is being sent to AI
        if payload.include_data:
            charts_with_data = [
                chart
                for chart in context.get("charts", [])
                if chart.get("sample_data", {}).get("rows")
            ]
            logger.info(
                f"Dashboard {dashboard_id} - Sending {len(charts_with_data)} charts with data to AI"
            )
            for chart in charts_with_data[:2]:  # Log first 2 charts
                sample_data = chart.get("sample_data", {})
                logger.info(
                    f"Chart {chart.get('title', 'Unknown')}: {len(sample_data.get('rows', []))} rows, sample: {sample_data.get('rows', [])[:2] if sample_data.get('rows') else 'No rows'}"
                )
        else:
            # DEBUG: Log schema information being sent
            charts_with_schema = [
                chart for chart in context.get("charts", []) if chart.get("schema")
            ]
            logger.info(
                f"Dashboard {dashboard_id} - Sending {len(charts_with_schema)} charts with schema to AI"
            )
            for chart in charts_with_schema[:2]:  # Log first 2 charts
                schema = chart.get("schema", {})
                table_columns = schema.get("table_columns", [])
                logger.info(
                    f"Chart {chart.get('title', 'Unknown')} schema: {len(table_columns)} columns: {[col.get('name') for col in table_columns[:5]]}"
                )

        ai_messages.append(AIMessage(role="system", content=system_prompt))

        # Add conversation history
        for msg in payload.messages:
            ai_messages.append(AIMessage(role=msg.role, content=msg.content, metadata=msg.metadata))

        # Generate session ID for this request
        session_id = str(uuid.uuid4())
        request_time = timezone.now()

        # Get the latest user message for logging
        user_message = None
        for msg in reversed(payload.messages):
            if msg.role == "user":
                user_message = msg.content
                break

        # Handle streaming vs non-streaming
        if payload.stream:
            return StreamingHttpResponse(
                _stream_dashboard_chat(
                    provider,
                    ai_messages,
                    context,
                    orguser_obj,
                    dashboard_id,
                    payload,
                    session_id,
                    user_message,
                    request_time,
                ),
                content_type="text/event-stream",
            )
        else:
            # Generate AI response with timing
            start_time = time.time()
            response = provider.chat_completion(
                messages=ai_messages, temperature=0.7, max_tokens=2000
            )
            end_time = time.time()
            response_time_ms = int((end_time - start_time) * 1000)
            response_time = timezone.now()

            # Log complete conversation if both user message and response exist
            if user_message:
                log_ai_chat_conversation(
                    org=orguser_obj.org,
                    user=orguser_obj.user,
                    user_prompt=user_message,
                    ai_response=response.content,
                    request_timestamp=request_time,
                    response_timestamp=response_time,
                    dashboard_id=dashboard_id,
                    chart_id=payload.selected_chart_id,
                    session_id=session_id,
                )

            # Always log metering data
            usage = response.usage or {}
            model_name = getattr(response, "provider", "unknown")
            if hasattr(response, "model"):
                model_name = response.model

            log_ai_chat_metering(
                org=orguser_obj.org,
                user=orguser_obj.user,
                model_used=model_name,
                prompt_tokens=usage.get("prompt_tokens", 0),
                completion_tokens=usage.get("completion_tokens", 0),
                response_time_ms=response_time_ms,
                include_data=payload.include_data,
                max_rows=payload.max_rows if payload.include_data else None,
                dashboard_id=dashboard_id,
                chart_id=payload.selected_chart_id,
                session_id=session_id,
            )

            return JsonResponse(
                {
                    "content": response.content,
                    "usage": response.usage,
                    "context_included": True,
                    "data_included": payload.include_data,
                    "dashboard_id": dashboard_id,
                    "metadata": {
                        "charts_analyzed": len(context.get("charts", [])),
                        "filters_available": len(context.get("filters", [])),
                        "selected_chart": payload.selected_chart_id,
                        "provider": response.provider,
                        "session_id": session_id,
                    },
                }
            )

    except Exception as e:
        logger.error(f"Error in dashboard chat: {e}")
        return JsonResponse({"error": "Internal server error"}, status=500)


async def _stream_dashboard_chat(
    provider,
    ai_messages,
    context,
    orguser_obj,
    dashboard_id,
    payload,
    session_id,
    user_message,
    request_time,
):
    """Generate streaming chat response with dashboard context."""
    try:
        start_time = time.time()
        full_response = ""
        final_usage = None

        async for chunk in provider.stream_chat_completion(ai_messages):
            data = {
                "content": chunk.content,
                "is_complete": chunk.is_complete,
                "context_included": True,
                "charts_analyzed": len(context.get("charts", [])),
                "timestamp": asyncio.get_event_loop().time(),
            }

            # Accumulate the full response for logging
            if chunk.content:
                full_response += chunk.content

            if chunk.usage:
                data["usage"] = chunk.usage
                final_usage = chunk.usage

            yield f"data: {json.dumps(data)}\n\n"

            if chunk.is_complete:
                end_time = time.time()
                response_time_ms = int((end_time - start_time) * 1000)
                response_time = timezone.now()

                # Log complete conversation if both user message and response exist
                if user_message:
                    log_ai_chat_conversation(
                        org=orguser_obj.org,
                        user=orguser_obj.user,
                        user_prompt=user_message,
                        ai_response=full_response,
                        request_timestamp=request_time,
                        response_timestamp=response_time,
                        dashboard_id=dashboard_id,
                        chart_id=payload.selected_chart_id,
                        session_id=session_id,
                    )

                # Always log metering data
                usage = final_usage or {}
                model_name = getattr(chunk, "provider", "unknown")
                if hasattr(chunk, "model"):
                    model_name = chunk.model

                log_ai_chat_metering(
                    org=orguser_obj.org,
                    user=orguser_obj.user,
                    model_used=model_name,
                    prompt_tokens=usage.get("prompt_tokens", 0),
                    completion_tokens=usage.get("completion_tokens", 0),
                    response_time_ms=response_time_ms,
                    include_data=payload.include_data,
                    max_rows=payload.max_rows if payload.include_data else None,
                    dashboard_id=dashboard_id,
                    chart_id=payload.selected_chart_id,
                    session_id=session_id,
                )

                yield "data: [DONE]\n\n"
                break

    except Exception as e:
        error_data = {"error": {"message": str(e), "type": type(e).__name__}}
        yield f"data: {json.dumps(error_data)}\n\n"


def _build_dashboard_system_prompt(
    context: Dict[str, Any], selected_chart_id: Optional[str] = None
) -> str:
    """
    Build system prompt with dashboard context for AI.
    """
    dashboard = context.get("dashboard", {})
    charts = context.get("charts", [])
    filters = context.get("filters", [])
    summary = context.get("summary", {})

    prompt_parts = [
        "You are an expert business data analyst who excels at explaining complex data insights to non-technical audiences.",
        "Your role is to translate technical data information into clear, actionable business insights that anyone can understand.",
        f"Current Dashboard: '{dashboard.get('title', 'Untitled Dashboard')}'",
        f"Description: {dashboard.get('description', 'No description available')}",
        "",
        "YOUR COMMUNICATION STYLE:",
        "- Business Translator: Convert technical data details into clear business insights",
        "- Insight Generator: Focus on what the data means for business decisions",
        "- User-Friendly Explainer: Use simple language while remaining authoritative",
        "- Context Provider: Help users understand the purpose and value of their data",
        "",
        "RESPONSE STYLE GUIDELINES:",
        "- BE DEFINITIVE AND AUTHORITATIVE: State facts, not possibilities",
        "- AVOID uncertain language: Never use 'might', 'probably', 'could be', 'perhaps', 'maybe', 'seems like', 'appears to'",
        "- USE CONFIDENT STATEMENTS: 'This data shows', 'The results indicate', 'Based on the data', 'This chart displays'",
        "- PROVIDE SPECIFIC INSIGHTS: Reference exact numbers, names, and values from the data",
        "- BE DIRECT: Answer questions clearly without hedging or speculation",
        "",
        "EXPLAINING CHARTS & DASHBOARDS:",
        "- Start with the BIG PICTURE: What business question does this chart answer?",
        "- Explain the PURPOSE: Why would someone look at this chart?",
        "- Describe what users will SEE: What patterns, trends, or insights are visible?",
        "- Connect to BUSINESS VALUE: How does this help with decisions?",
        "- Use SIMPLE LANGUAGE: Avoid technical jargon like 'VARCHAR', 'aggregation', 'schema'",
        "- Focus on MEANING not STRUCTURE: What the data tells us, not how it's organized",
        "",
        "EXAMPLE AUTHORITATIVE PHRASES:",
        "✅ USE: 'The data shows 15 customers with total revenue of $45,000'",
        "❌ AVOID: 'There appear to be around 15 customers with roughly $45,000 revenue'",
        "✅ USE: 'Revenue increased 23% from January to February'",
        "❌ AVOID: 'Revenue seems to have grown, possibly by around 23%'",
        "✅ USE: 'This table contains customer transaction data with 8 columns'",
        "❌ AVOID: 'This table might contain customer data with what looks like 8 columns'",
        "",
        "FORMATTING REQUIREMENTS:",
        "- Write in clear, professional business language without excessive formatting",
        "- NO markdown headers (###, ##) or excessive emojis in responses",
        "- Format numbers clearly (e.g., 1,234 instead of 1234)",
        "- Use simple bullet points for lists when needed",
        "- Write in paragraphs that flow naturally, like a business analyst explaining to colleagues",
        "- Focus on insights and explanations, not technical data structure",
    ]

    if dashboard.get("dashboard_type"):
        prompt_parts.append(f"Dashboard Type: {dashboard['dashboard_type']}")

    # Add chart information for business analysis
    if charts:
        prompt_parts.append(f"\nDASHBOARD CONTENT: {len(charts)} charts available for analysis")
        prompt_parts.append("")

        for i, chart in enumerate(charts[:10], 1):  # Limit to first 10 charts
            chart_type = chart.get("type", "unknown")
            chart_title = chart.get("title", "Untitled Chart")

            # Build business-focused chart description
            chart_info = f"Chart {i}: {chart_title} ({chart_type} chart)"

            # Add data source information simply
            schema = chart.get("schema", {})
            if schema:
                if schema.get("schema_name") and schema.get("table_name"):
                    chart_info += f" - Data from {schema['schema_name']}.{schema['table_name']}"

                # Summarize data availability without technical details
                table_columns = schema.get("table_columns", [])
                if table_columns:
                    # Focus on business insight potential, not technical schema
                    column_count = len(table_columns)
                    chart_info += f" with {column_count} data fields"

                    # Identify key business columns by name patterns
                    business_cols = []
                    for col in table_columns:
                        col_name = col.get("name", "").lower()
                        if any(
                            term in col_name
                            for term in [
                                "revenue",
                                "sales",
                                "customer",
                                "price",
                                "cost",
                                "profit",
                                "amount",
                                "quantity",
                                "date",
                                "time",
                                "name",
                                "id",
                            ]
                        ):
                            business_cols.append(col.get("name", "unknown"))

                    if business_cols:
                        chart_info += f" including {', '.join(business_cols[:4])}"
                        if len(business_cols) > 4:
                            chart_info += f" and {len(business_cols)-4} more"

            # Add data status with enhanced chart vs raw data distinction
            sample_data = chart.get("sample_data")
            if sample_data:
                if sample_data.get("rows"):
                    rows = sample_data["rows"]
                    data_source = sample_data.get("source", "unknown")

                    if data_source == "chart_execution":
                        # This is actual processed chart data
                        chart_info += f" - Contains {len(rows)} processed chart results"
                        chart_computation = sample_data.get("computation_type", "unknown")
                        if chart_computation == "aggregated":
                            chart_info += " (aggregated)"
                        elif chart_computation == "raw":
                            chart_info += " (filtered)"
                    else:
                        # This is raw table data fallback
                        chart_info += f" - Contains {len(rows)} raw table samples"

                    # Include key insights from data without raw dump
                    if len(rows) > 0:
                        # Extract business insights from first few rows
                        sample_row = rows[0] if isinstance(rows[0], dict) else {}
                        if sample_row:
                            # Identify interesting values
                            key_fields = []
                            for key, value in list(sample_row.items())[:3]:
                                if value is not None and str(value).strip():
                                    key_fields.append(f"{key}")
                            if key_fields:
                                chart_info += f" showing {', '.join(key_fields)}"
                elif sample_data.get("error"):
                    chart_info += f" - Data unavailable due to configuration issue"

            prompt_parts.append(chart_info)

        # Add summary line if there are more charts
        if len(charts) > 10:
            prompt_parts.append(f"Plus {len(charts) - 10} additional charts")

        prompt_parts.append("")

    # Add filter information
    if filters:
        prompt_parts.append(
            f"\nAVAILABLE FILTERS: {len(filters)} filters can be used to segment the data"
        )
        for filter_obj in filters[:5]:
            filter_name = filter_obj.get("name", "Unnamed Filter")
            filter_type = filter_obj.get("filter_type", "unknown")
            prompt_parts.append(f"- {filter_name} ({filter_type})")
        if len(filters) > 5:
            prompt_parts.append(f"Plus {len(filters) - 5} additional filters")
        prompt_parts.append("")

    # Add data availability info with detailed status
    prompt_parts.append("\nDATA ACCESS STATUS:")
    if summary.get("data_included"):
        prompt_parts.append(
            f"Data sharing is enabled - Up to {summary.get('max_rows', 0):,} rows per chart are available"
        )

        # Count charts with actual data
        charts_with_data = sum(1 for chart in charts if chart.get("sample_data", {}).get("rows"))
        charts_with_errors = sum(1 for chart in charts if chart.get("sample_data", {}).get("error"))
        charts_with_fallback = sum(
            1 for chart in charts if chart.get("sample_data", {}).get("fallback")
        )

        if charts_with_data > 0:
            prompt_parts.append(
                f"{charts_with_data} charts have sample data available for analysis"
            )
            if charts_with_fallback > 0:
                prompt_parts.append(f"{charts_with_fallback} charts are using raw table data")
        if charts_with_errors > 0:
            prompt_parts.append(f"{charts_with_errors} charts had data fetch errors")
    else:
        prompt_parts.append("Schema-only mode - Only table structure and column names available")
        prompt_parts.append("User can enable data sharing in chat settings for detailed analysis")

    # Enhanced: Add data intelligence context if available
    enhanced_analysis = context.get("enhanced_analysis", {})
    ai_data_context = context.get("ai_data_context", "")

    if enhanced_analysis.get("tables_analyzed", 0) > 0:
        prompt_parts.append("\nENHANCED DATA INTELLIGENCE AVAILABLE:")
        prompt_parts.append(
            f"Comprehensive analysis of {enhanced_analysis['tables_analyzed']} data tables completed."
        )

        if enhanced_analysis.get("business_context_available"):
            prompt_parts.append(
                "Business context and column descriptions are available for intelligent analysis."
            )

        if enhanced_analysis.get("sample_data_included"):
            prompt_parts.append("Sample data values are included to help understand data patterns.")

        if enhanced_analysis.get("query_suggestions_available"):
            prompt_parts.append(
                "Query guidelines and examples are available for complex analysis needs."
            )

        # Include the rich AI data context
        if ai_data_context and len(ai_data_context) > 100:  # Only include if substantial
            prompt_parts.append("\nCOMPREHENSIVE DATA CATALOG:")
            prompt_parts.append(ai_data_context)

        prompt_parts.append("\nWith this enhanced data intelligence, you can provide:")
        prompt_parts.extend(
            [
                "- Detailed explanations of available data sources and their business purpose",
                "- Specific insights about column types, sample values, and data patterns",
                "- Intelligent recommendations for data analysis and exploration",
                "- Business-focused interpretations of technical data structures",
                "- Contextual understanding of how data relates to business metrics",
            ]
        )

    prompt_parts.append("")

    # Add specific chart context if selected
    if selected_chart_id:
        selected_chart = next((c for c in charts if c.get("id") == selected_chart_id), None)
        if selected_chart:
            prompt_parts.append(
                f"\nUser is focusing on chart: '{selected_chart.get('title', 'Untitled Chart')}'"
            )
            if selected_chart.get("schema"):
                schema = selected_chart["schema"]
                if schema.get("columns"):
                    prompt_parts.append(f"Chart columns: {', '.join(schema['columns'])}")
                if schema.get("metrics"):
                    prompt_parts.append(f"Chart metrics: {', '.join(schema['metrics'])}")

    # Add specific instructions based on data availability
    if summary.get("data_included"):
        charts_with_data = sum(1 for chart in charts if chart.get("sample_data", {}).get("rows"))
        charts_with_errors = sum(1 for chart in charts if chart.get("sample_data", {}).get("error"))
        charts_with_chart_data = sum(
            1 for chart in charts if chart.get("sample_data", {}).get("source") == "chart_execution"
        )
        charts_with_raw_fallback = sum(
            1
            for chart in charts
            if chart.get("sample_data", {}).get("source") not in ["chart_execution", None]
            and chart.get("sample_data", {}).get("rows")
        )

        if charts_with_data > 0:
            prompt_parts.extend(
                [
                    "\nYOUR ANALYSIS CAPABILITIES WITH DATA:",
                    "- Analyze actual data values and trends from the sample data provided above",
                    "- Identify patterns, outliers, and insights in the real data",
                    "- Provide specific statistics and data-driven observations",
                    "- Suggest actionable insights based on chart data and actual values",
                    "- Explain what the data reveals about business metrics",
                    "- Recommend filters or views based on data patterns you observe",
                    "",
                ]
            )

            # Provide enhanced context about data types
            if charts_with_chart_data > 0:
                prompt_parts.append(
                    f"✅ EXCELLENT: {charts_with_chart_data} charts provide processed chart results - these show exactly what users see on dashboard"
                )

            if charts_with_raw_fallback > 0:
                prompt_parts.append(
                    f"⚠️  FALLBACK: {charts_with_raw_fallback} charts show raw table data due to chart execution issues"
                )

            if charts_with_errors > 0:
                prompt_parts.append(
                    f"❌ ERRORS: {charts_with_errors} charts had data fetch errors - provide schema-based analysis for those"
                )

            prompt_parts.extend(
                [
                    "\nCRITICAL: You have access to actual sample data values shown above. When responding:",
                    "",
                    "ANALYSIS REQUIREMENTS:",
                    "✅ FOR CHART-PROCESSED DATA (marked as 'processed chart results'):",
                    "- These represent exactly what users see in their dashboard charts",
                    "- State facts directly: 'This chart shows Karnataka has 61M total population'",
                    "- Use exact values from aggregated results: 'Total revenue: $1.2M across 50 customers'",
                    "- Answer questions immediately without suggesting queries",
                    "",
                    "⚠️  FOR RAW TABLE DATA (marked as 'raw table samples'):",
                    "- This is unprocessed data that may need aggregation",
                    "- Analyze patterns but note if aggregation is needed",
                    "- State facts: 'The sample shows district-level population data that sums to X for the state'",
                    "",
                    "🎯 ALWAYS ANSWER DIRECTLY when you have the data:",
                    "- Calculate from available data: Sum populations, count records, find totals",
                    "- Be specific: 'Customer John Doe has $1,500 revenue' not 'customers appear to have revenue'",
                    "- State definitive results: 'Karnataka population: 61,130,704' not 'you can query to find Karnataka population'",
                    "",
                ]
            )
        else:
            prompt_parts.extend(
                [
                    "\nDATA ISSUES DETECTED:",
                    "",
                    "You can still provide valuable analysis with:",
                    "- Understanding dashboard structure and chart configurations",
                    "- Explaining what each chart is designed to show based on schema",
                    "- Suggesting insights based on chart configurations and column types",
                    "- Explaining how to interpret visualizations",
                    "- Recommending filters or views that might be helpful",
                    "- Identifying potential configuration issues that prevented data loading",
                    "",
                    "Note: Data sharing is enabled but there were errors fetching the actual data. Focus on schema and configuration analysis and mention that charts may need configuration fixes.",
                    "",
                ]
            )
    else:
        prompt_parts.extend(
            [
                "\nSCHEMA-BASED ANALYSIS CAPABILITIES:",
                "",
                "You can provide valuable insights using the table structures and column information:",
                "- Analyze the actual table structures and column types shown above",
                "- Explain what each chart represents based on the real column names and data types",
                "- Identify specific insights possible with these column combinations",
                "- Recommend optimal chart types based on the available data types",
                "- Suggest useful filters and groupings based on the column structure",
                "- Categorize columns as dimensions vs metrics based on their data types",
                "",
                "SCHEMA ANALYSIS REQUIREMENTS:",
                "You have access to real table schemas with actual column names and data types.",
                "State facts: 'This table contains 8 columns including customer_name and revenue'",
                "Be specific: 'The date column enables time-series analysis' not 'date columns might allow temporal analysis'",
                "Make recommendations: 'Use customer_name for grouping and revenue for aggregation' not 'these columns could potentially be used'",
                "",
                "Note: User can enable data sharing in chat settings to analyze actual values and calculate precise metrics.",
                "",
            ]
        )

    return "\n".join(prompt_parts)


@router.post("/{dashboard_id}/chat-settings")
@has_permission(["can_view_dashboards"])
def update_chat_settings(request, dashboard_id: int):
    """
    Update chat settings for a dashboard session.
    """
    try:
        data = json.loads(request.body)

        # Settings could be stored in user preferences or session
        settings = {
            "include_data": data.get("include_data", False),
            "max_rows": data.get("max_rows", 100),
            "provider_type": data.get("provider_type"),
            "auto_context": data.get("auto_context", True),
        }

        # For now, just validate and return the settings
        # In a full implementation, you might store these in user preferences

        return JsonResponse(
            {"dashboard_id": dashboard_id, "settings": settings, "updated_at": time.time()}
        )

    except Exception as e:
        logger.error(f"Error updating chat settings: {e}")
        return JsonResponse({"error": "Internal server error"}, status=500)


@router.post("/{dashboard_id}/debug")
@has_permission(["can_view_dashboards"])
def debug_dashboard_context(request, dashboard_id: int, payload: DashboardContextRequest):
    """
    Debug endpoint to see detailed context information for troubleshooting.
    Returns the full context with all debug information.
    """
    try:
        orguser_obj = request.orguser
        if not orguser_obj:
            return JsonResponse({"error": "Unauthorized"}, status=401)

        analyzer = DashboardContextAnalyzer(
            dashboard_id=dashboard_id,
            orguser_obj=orguser_obj,
            include_data=payload.include_data,
            max_rows=payload.max_rows,
        )

        # Get the context with detailed logging
        context = analyzer.get_dashboard_context()

        # Add extra debug info
        debug_info = {
            "dashboard_id": dashboard_id,
            "user_org": orguser_obj.org.slug,
            "include_data": payload.include_data,
            "max_rows": payload.max_rows,
            "context": context,
            "timestamp": time.time(),
        }

        # Add warehouse info for debugging
        from ddpui.models.org import OrgWarehouse

        org_warehouse = OrgWarehouse.objects.filter(org=orguser_obj.org).first()
        if org_warehouse:
            debug_info["warehouse"] = {
                "name": org_warehouse.name,
                "type": org_warehouse.wtype,
                "credentials_available": bool(org_warehouse.credentials),
            }
        else:
            debug_info["warehouse"] = None

        return JsonResponse(debug_info)

    except Exception as e:
        logger.error(f"Error in debug dashboard context: {e}")
        return JsonResponse(
            {"error": f"Debug error: {str(e)}", "dashboard_id": dashboard_id}, status=500
        )


@router.get("/{dashboard_id}/test-chart/{chart_id}")
@has_permission(["can_view_dashboards"])
def test_single_chart(request, dashboard_id: int, chart_id: int):
    """
    Test endpoint to debug a single chart's data fetching.
    """
    try:
        orguser_obj = request.orguser
        if not orguser_obj:
            return JsonResponse({"error": "Unauthorized"}, status=401)

        analyzer = DashboardContextAnalyzer(
            dashboard_id=dashboard_id,
            orguser_obj=orguser_obj,
            include_data=True,  # Always include data for testing
            max_rows=10,  # Small sample for testing
        )

        # Test just this chart
        result = analyzer._get_chart_data_context(chart_id)

        return JsonResponse(
            {
                "chart_id": chart_id,
                "dashboard_id": dashboard_id,
                "result": result,
                "timestamp": time.time(),
            }
        )

    except Exception as e:
        logger.error(f"Error testing chart {chart_id}: {e}")
        return JsonResponse({"error": f"Test error: {str(e)}", "chart_id": chart_id}, status=500)


@router.get("/{dashboard_id}/test-data-intelligence")
@has_permission(["can_view_dashboards"])
def test_data_intelligence(request, dashboard_id: int):
    """
    Test endpoint for Layer 1: DataIntelligenceService
    Returns data catalog and enhanced context for debugging
    """
    try:
        orguser_obj = request.orguser
        if not orguser_obj:
            return JsonResponse({"error": "Unauthorized"}, status=401)

        # Initialize data intelligence service
        data_intelligence = DataIntelligenceService()

        # Test data catalog building
        catalog = data_intelligence.get_org_data_catalog(orguser_obj.org)

        # Test AI context building
        ai_context = data_intelligence.build_ai_data_context(
            org=orguser_obj.org, dashboard_id=dashboard_id
        )

        return JsonResponse(
            {
                "success": True,
                "dashboard_id": dashboard_id,
                "org": orguser_obj.org.slug,
                "catalog_summary": {
                    "total_tables": catalog.total_tables,
                    "total_charts": catalog.total_charts,
                    "table_keys": list(catalog.tables.keys()),
                    "chart_mappings_count": len(catalog.chart_table_mappings),
                },
                "ai_context_preview": ai_context[:1000] + "..."
                if len(ai_context) > 1000
                else ai_context,
                "ai_context_length": len(ai_context),
                "timestamp": timezone.now().isoformat(),
            }
        )

    except Exception as e:
        logger.error(f"Error testing data intelligence: {e}")
        return JsonResponse(
            {"error": f"Test failed: {str(e)}", "dashboard_id": dashboard_id}, status=500
        )


@router.get("/{dashboard_id}/debug-chart-config/{chart_id}")
@has_permission(["can_view_dashboards"])
def debug_chart_config(request, dashboard_id: int, chart_id: int):
    """
    Debug endpoint to see chart configuration details.
    """
    try:
        orguser_obj = request.orguser
        if not orguser_obj:
            return JsonResponse({"error": "Unauthorized"}, status=401)

        from ddpui.models.visualization import Chart

        chart = Chart.objects.filter(id=chart_id, org=orguser_obj.org).first()
        if not chart:
            return JsonResponse({"error": "Chart not found"}, status=404)

        extra_config = chart.extra_config or {}

        debug_info = {
            "chart_id": chart_id,
            "dashboard_id": dashboard_id,
            "chart_details": {
                "title": chart.title,
                "chart_type": chart.chart_type,
                "computation_type": chart.computation_type,
                "schema_name": chart.schema_name,
                "table_name": chart.table_name,
            },
            "extra_config": extra_config,
            "config_analysis": {
                "has_metrics": bool(extra_config.get("metrics")),
                "has_aggregate_functions": bool(extra_config.get("aggregate_functions")),
                "has_dimension_col": bool(extra_config.get("dimension_col")),
                "has_columns": bool(extra_config.get("columns")),
                "metrics_count": len(extra_config.get("metrics", [])),
                "columns_count": len(extra_config.get("columns", [])),
            },
            "recommended_action": "unknown",
        }

        # Determine recommended action
        if chart.computation_type == "aggregated":
            if not extra_config.get("metrics") and not extra_config.get("aggregate_functions"):
                debug_info["recommended_action"] = "force_raw_mode_no_metrics"
            elif chart.chart_type.lower() in [
                "table",
                "pie",
                "doughnut",
                "map",
                "choropleth",
            ] and not extra_config.get("dimension_col"):
                if not extra_config.get("columns"):
                    debug_info["recommended_action"] = "force_raw_mode_no_dimension_data"
                else:
                    debug_info["recommended_action"] = "infer_dimension_col"
            else:
                debug_info["recommended_action"] = "use_as_configured"
        else:
            debug_info["recommended_action"] = "use_raw_mode"

        return JsonResponse(debug_info)

    except Exception as e:
        logger.error(f"Error debugging chart config {chart_id}: {e}")
        return JsonResponse({"error": f"Debug error: {str(e)}", "chart_id": chart_id}, status=500)


# Layer 4: Enhanced Dashboard Chat API Integration


class EnhancedDashboardChatRequest(Schema):
    messages: List[DashboardChatMessage]
    include_data: bool = False
    max_rows: int = 100
    enable_smart_queries: bool = True
    selected_chart_id: Optional[str] = None
    user_preferences: Optional[Dict[str, Any]] = None


@router.post("/{dashboard_id}/enhanced-chat")
@has_permission(["can_view_dashboards"])
def enhanced_dashboard_chat(request, dashboard_id: int, payload: EnhancedDashboardChatRequest):
    """
    Layer 4: Enhanced Dashboard Chat with Intelligent Data Query Capabilities

    Automatically detects data queries in chat messages and executes them through
    the natural language query system while maintaining conversational flow.
    """
    try:
        orguser_obj = request.orguser
        if not orguser_obj:
            return JsonResponse({"error": "Unauthorized"}, status=401)

        # Check organization settings for AI features
        org_settings = OrgSettings.objects.filter(org=orguser_obj.org).first()
        if not org_settings or not org_settings.ai_consent:
            return JsonResponse(
                {
                    "error": "AI features must be enabled in organization settings",
                    "requires_consent": True,
                },
                status=400,
            )

        # Get the latest user message
        user_messages = [msg for msg in payload.messages if msg.role == "user"]
        if not user_messages:
            return JsonResponse({"error": "No user message found"}, status=400)

        latest_message = user_messages[-1].content
        session_id = str(uuid.uuid4())
        request_time = timezone.now()

        # Get dashboard context for enhanced AI capabilities
        analyzer = DashboardContextAnalyzer(
            dashboard_id=dashboard_id,
            orguser_obj=orguser_obj,
            include_data=payload.include_data,
            max_rows=payload.max_rows,
        )
        dashboard_context = analyzer.get_dashboard_context()

        # Initialize smart chat processor
        smart_processor = SmartChatProcessor()

        # Process the message with enhanced capabilities
        enhanced_response = smart_processor.process_enhanced_chat_message(
            message=latest_message,
            org=orguser_obj.org,
            dashboard_id=dashboard_id,
            dashboard_context=dashboard_context,
            user_context=payload.user_preferences or {},
            enable_data_query=payload.enable_smart_queries and org_settings.ai_data_sharing_enabled,
        )

        # Build response based on whether data query was executed
        if enhanced_response.query_executed and enhanced_response.data_results:
            # Data query was successfully executed
            response_content = {
                "content": enhanced_response.content,
                "enhanced_features": {
                    "intent_detected": enhanced_response.intent_detected.value,
                    "data_query_executed": True,
                    "query_results": enhanced_response.data_results,
                    "confidence_score": enhanced_response.confidence_score,
                    "recommendations": enhanced_response.recommendations,
                },
                "context_included": True,
                "data_included": True,
                "dashboard_id": dashboard_id,
                "metadata": {
                    "smart_processing": True,
                    "charts_analyzed": len(dashboard_context.get("charts", [])),
                    "filters_available": len(dashboard_context.get("filters", [])),
                    "selected_chart": payload.selected_chart_id,
                    "session_id": session_id,
                    "processing_time_ms": enhanced_response.data_results.get("analytics", {})
                    .get("performance_metrics", {})
                    .get("total_time_ms", 0),
                },
            }

        else:
            # Standard chat response or data query failed
            # Fall back to standard AI chat with enhanced context
            system_prompt = _build_dashboard_system_prompt(
                dashboard_context, payload.selected_chart_id
            )

            # Add smart processing context
            if enhanced_response.intent_detected != MessageIntent.GENERAL_CONVERSATION:
                system_prompt += (
                    f"\n\nUser Intent Detected: {enhanced_response.intent_detected.value}"
                )
                if enhanced_response.fallback_used:
                    system_prompt += (
                        "\nNote: A data query was attempted but failed. Provide helpful guidance."
                    )

            ai_messages = [AIMessage(role="system", content=system_prompt)]

            # Add conversation history
            for msg in payload.messages:
                ai_messages.append(AIMessage(role=msg.role, content=msg.content))

            # Get AI provider and generate response
            provider = get_default_ai_provider()
            start_time = time.time()
            response = provider.chat_completion(
                messages=ai_messages, temperature=0.7, max_tokens=2000
            )
            end_time = time.time()
            response_time_ms = int((end_time - start_time) * 1000)

            # Use enhanced response content if available, otherwise use AI response
            final_content = (
                enhanced_response.content if enhanced_response.content else response.content
            )

            response_content = {
                "content": final_content,
                "enhanced_features": {
                    "intent_detected": enhanced_response.intent_detected.value,
                    "data_query_executed": False,
                    "fallback_used": enhanced_response.fallback_used,
                    "recommendations": enhanced_response.recommendations,
                },
                "usage": response.usage,
                "context_included": True,
                "data_included": payload.include_data,
                "dashboard_id": dashboard_id,
                "metadata": {
                    "smart_processing": True,
                    "charts_analyzed": len(dashboard_context.get("charts", [])),
                    "filters_available": len(dashboard_context.get("filters", [])),
                    "selected_chart": payload.selected_chart_id,
                    "provider": response.provider,
                    "session_id": session_id,
                    "ai_response_time_ms": response_time_ms,
                },
            }

        # Log the interaction
        log_ai_chat_conversation(
            org=orguser_obj.org,
            user=orguser_obj.user,
            user_prompt=latest_message,
            ai_response=response_content["content"],
            request_timestamp=request_time,
            response_timestamp=timezone.now(),
            dashboard_id=dashboard_id,
            chart_id=payload.selected_chart_id,
            session_id=session_id,
        )

        # Log metering data
        usage = response_content.get("usage", {}) or {}
        log_ai_chat_metering(
            org=orguser_obj.org,
            user=orguser_obj.user,
            model_used=response_content.get("metadata", {}).get("provider", "enhanced_processor"),
            prompt_tokens=usage.get("prompt_tokens", 0),
            completion_tokens=usage.get("completion_tokens", 0),
            response_time_ms=response_content.get("metadata", {}).get("processing_time_ms", 0),
            include_data=payload.include_data,
            max_rows=payload.max_rows if payload.include_data else None,
            dashboard_id=dashboard_id,
            chart_id=payload.selected_chart_id,
            session_id=session_id,
        )

        return JsonResponse(response_content)

    except Exception as e:
        logger.error(f"Error in enhanced dashboard chat: {e}")
        return JsonResponse({"error": "Internal server error"}, status=500)


# Layer 2 Test Endpoints for Natural Language Query Processing


class NaturalLanguageQueryRequest(Schema):
    question: str
    test_mode: bool = True


@router.post("/{dashboard_id}/test-layer2-query-generation")
@has_permission(["can_view_dashboards"])
def test_layer2_query_generation(request, dashboard_id: int, payload: NaturalLanguageQueryRequest):
    """
    Test endpoint for Layer 2: Natural Language Query Generation
    Tests AI-powered conversion of natural language to SQL without execution
    """
    try:
        orguser_obj = request.orguser
        if not orguser_obj:
            return JsonResponse({"error": "Unauthorized"}, status=401)

        from ddpui.core.ai.query_generator import NaturalLanguageQueryService

        # Initialize query generator
        query_service = NaturalLanguageQueryService()

        # Generate query plan from the natural language question
        query_plan = query_service.generate_query_from_question(
            question=payload.question, org=orguser_obj.org, dashboard_id=dashboard_id
        )

        # Format response for testing
        return JsonResponse(
            {
                "success": True,
                "dashboard_id": dashboard_id,
                "org": orguser_obj.org.slug,
                "test_request": {"question": payload.question, "test_mode": payload.test_mode},
                "query_plan": {
                    "original_question": query_plan.original_question,
                    "generated_sql": query_plan.generated_sql,
                    "explanation": query_plan.explanation,
                    "confidence_score": query_plan.confidence_score,
                    "expected_result_type": query_plan.expected_result_type,
                    "requires_execution": query_plan.requires_execution,
                    "fallback_to_existing_data": query_plan.fallback_to_existing_data,
                    "ai_reasoning": query_plan.ai_reasoning,
                },
                "validation_result": {
                    "is_valid": query_plan.validation_result.is_valid
                    if query_plan.validation_result
                    else None,
                    "error_message": query_plan.validation_result.error_message
                    if query_plan.validation_result
                    else None,
                    "warnings": query_plan.validation_result.warnings
                    if query_plan.validation_result
                    else [],
                    "complexity_score": query_plan.validation_result.complexity_score
                    if query_plan.validation_result
                    else None,
                    "tables_accessed": query_plan.validation_result.tables_accessed
                    if query_plan.validation_result
                    else [],
                    "columns_accessed": query_plan.validation_result.columns_accessed
                    if query_plan.validation_result
                    else [],
                },
                "timestamp": timezone.now().isoformat(),
            }
        )

    except Exception as e:
        logger.error(f"Error testing Layer 2 query generation: {e}")
        return JsonResponse(
            {
                "error": f"Test failed: {str(e)}",
                "dashboard_id": dashboard_id,
                "question": payload.question,
            },
            status=500,
        )


@router.post("/{dashboard_id}/test-layer2-full-execution")
@has_permission(["can_view_dashboards"])
def test_layer2_full_execution(request, dashboard_id: int, payload: NaturalLanguageQueryRequest):
    """
    Test endpoint for Layer 2: Complete Natural Language Query Execution
    Tests the full flow from question to executed results (with safety limits)
    """
    try:
        orguser_obj = request.orguser
        if not orguser_obj:
            return JsonResponse({"error": "Unauthorized"}, status=401)

        from ddpui.core.ai.query_executor import DynamicQueryExecutor

        # Check if data sharing is enabled
        org_settings = OrgSettings.objects.filter(org=orguser_obj.org).first()
        if not org_settings or not org_settings.ai_data_sharing_enabled:
            return JsonResponse(
                {
                    "error": "Data sharing must be enabled in organization settings for query execution",
                    "dashboard_id": dashboard_id,
                    "requires_data_sharing": True,
                },
                status=400,
            )

        # Initialize executor
        executor = DynamicQueryExecutor()

        # Execute the natural language query
        execution_result = executor.execute_natural_language_query(
            question=payload.question,
            org=orguser_obj.org,
            dashboard_id=dashboard_id,
            user_context={
                "test_mode": payload.test_mode,
                "user_id": orguser_obj.user.id,
                "dashboard_id": dashboard_id,
            },
        )

        # Format response for testing
        response_data = {
            "success": execution_result.success,
            "dashboard_id": dashboard_id,
            "org": orguser_obj.org.slug,
            "test_request": {"question": payload.question, "test_mode": payload.test_mode},
            "execution_result": {
                "success": execution_result.success,
                "row_count": execution_result.row_count,
                "execution_time_ms": execution_result.execution_time_ms,
                "columns": execution_result.columns,
                "data_preview": execution_result.data[:5]
                if execution_result.data
                else [],  # First 5 rows for preview
                "total_data_rows": len(execution_result.data),
                "error_message": execution_result.error_message,
                "warnings": execution_result.warnings,
                "was_query_modified": execution_result.was_query_modified,
                "executed_at": execution_result.executed_at.isoformat(),
            },
            "query_details": {
                "original_query": execution_result.original_query,
                "executed_query": execution_result.executed_query,
                "confidence_score": execution_result.query_plan.confidence_score
                if execution_result.query_plan
                else None,
                "ai_explanation": execution_result.query_plan.explanation
                if execution_result.query_plan
                else None,
            },
            "resource_usage": execution_result.resource_usage,
            "timestamp": timezone.now().isoformat(),
        }

        # Add full data if requested and result set is small
        if payload.test_mode and len(execution_result.data) <= 20:
            response_data["execution_result"]["full_data"] = execution_result.data

        return JsonResponse(response_data)

    except Exception as e:
        logger.error(f"Error testing Layer 2 full execution: {e}")
        return JsonResponse(
            {
                "error": f"Execution test failed: {str(e)}",
                "dashboard_id": dashboard_id,
                "question": payload.question,
            },
            status=500,
        )


@router.get("/{dashboard_id}/layer2-execution-stats")
@has_permission(["can_view_dashboards"])
def get_layer2_execution_stats(request, dashboard_id: int):
    """
    Get execution statistics for Layer 2 query processing for this organization
    """
    try:
        orguser_obj = request.orguser
        if not orguser_obj:
            return JsonResponse({"error": "Unauthorized"}, status=401)

        from ddpui.core.ai.query_executor import DynamicQueryExecutor

        # Initialize executor to get stats
        executor = DynamicQueryExecutor()

        # Get stats for this org
        stats = executor.get_execution_stats(org_id=orguser_obj.org.id)

        return JsonResponse(
            {
                "success": True,
                "dashboard_id": dashboard_id,
                "org": orguser_obj.org.slug,
                "stats": stats,
                "safety_limits": {
                    "max_execution_time_seconds": executor.safety_limits.max_execution_time_seconds,
                    "max_result_rows": executor.safety_limits.max_result_rows,
                    "max_queries_per_minute": executor.safety_limits.max_queries_per_minute,
                    "max_queries_per_hour": executor.safety_limits.max_queries_per_hour,
                },
                "timestamp": timezone.now().isoformat(),
            }
        )

    except Exception as e:
        logger.error(f"Error getting Layer 2 execution stats: {e}")
        return JsonResponse(
            {"error": f"Stats retrieval failed: {str(e)}", "dashboard_id": dashboard_id}, status=500
        )


# Example questions for testing Layer 2
EXAMPLE_LAYER2_QUESTIONS = [
    "How many students attended school in 2021 in Maharashtra?",
    "What is the total revenue by state for last year?",
    "Show me the top 5 schools by performance score",
    "Count of customers by region in 2023",
    "Average sales amount per month",
    "Which products have the highest profit margins?",
    "List all customers with revenue greater than 10000",
    "Show enrollment trends over the past 3 years",
    "What is the distribution of students by grade level?",
    "Compare performance scores between different states",
]


@router.get("/{dashboard_id}/layer2-example-questions")
@has_permission(["can_view_dashboards"])
def get_layer2_example_questions(request, dashboard_id: int):
    """
    Get example questions that can be used to test Layer 2 functionality
    """
    try:
        orguser_obj = request.orguser
        if not orguser_obj:
            return JsonResponse({"error": "Unauthorized"}, status=401)

        # Check data sharing status
        org_settings = OrgSettings.objects.filter(org=orguser_obj.org).first()
        data_sharing_enabled = org_settings and org_settings.ai_data_sharing_enabled

        return JsonResponse(
            {
                "success": True,
                "dashboard_id": dashboard_id,
                "org": orguser_obj.org.slug,
                "data_sharing_enabled": data_sharing_enabled,
                "example_questions": EXAMPLE_LAYER2_QUESTIONS,
                "usage_instructions": {
                    "query_generation_test": f"POST /{dashboard_id}/test-layer2-query-generation",
                    "full_execution_test": f"POST /{dashboard_id}/test-layer2-full-execution",
                    "stats_endpoint": f"GET /{dashboard_id}/layer2-execution-stats",
                    "note": "Full execution requires data sharing to be enabled in org settings",
                },
                "timestamp": timezone.now().isoformat(),
            }
        )

    except Exception as e:
        logger.error(f"Error getting example questions: {e}")
        return JsonResponse(
            {"error": f"Failed to get examples: {str(e)}", "dashboard_id": dashboard_id}, status=500
        )
